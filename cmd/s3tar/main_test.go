package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"os"
	"testing"
)

var (
	client                     *s3.Client
	testBucket                 = os.Getenv("S3TAR_TEST_BUCKET")
	testRegion                 = os.Getenv("S3TAR_TEST_REGION")
	simpleSmallDataTarTestFile = "s3://" + testBucket + "/simple-small-data-test.tar"
	simpleLargeDataTarTestFile = "s3://" + testBucket + "/simple-large-data-test.tar"
	sourceSmallDataDir         = "s3://" + testBucket + "/test-data/small/"
	sourceLargeDataDir         = "s3://" + testBucket + "/test-data/large/"
	extractDir                 = "s3://" + testBucket + "/extract/"
	manifestTarTestFile        = "s3://" + testBucket + "/manifest-tests.tar"
	manifestTestCsvFile        = "manifest.csv"
	largeTestFileList          []TestFile
	smallTestFileList          []TestFile
)

func printHelp() {
	fmt.Printf("\n*** testing env-vars not set. skipping end-to-end tests.***\n\n")
}

func Test_create(t *testing.T) {

	if testBucket == "" {
		printHelp()
		t.SkipNow()
	}
	firstArgs := os.Args[0]
	type args struct {
		args []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "create-simple-small",
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-cf", simpleSmallDataTarTestFile,
					sourceSmallDataDir,
				},
			},
			wantErr: false,
		},
		{
			name: "create-simple-large",
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-cf", simpleLargeDataTarTestFile,
					sourceLargeDataDir,
				},
			},
			wantErr: false,
		},
		{
			name: "create-manifest",
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-cf", manifestTarTestFile,
					"-m", fmt.Sprintf("s3://%s/%s", testBucket, manifestTestCsvFile),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := run(tt.args.args); (err != nil) != tt.wantErr {
				t.Errorf("run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_run_extract(t *testing.T) {

	// TODO update test to verify extracted data
	if testBucket == "" {
		printHelp()
		t.SkipNow()
	}
	firstArgs := os.Args[0]
	type args struct {
		args []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "extract",
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-xf", manifestTarTestFile,
					"-C", extractDir,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := run(tt.args.args); (err != nil) != tt.wantErr {
				t.Errorf("run() error = %v, wantErr %v", err, tt.wantErr)
			}

			for _, f := range append(largeTestFileList, smallTestFileList...) {
				extractLocation := fmt.Sprintf("extract/%s", f.Key)
				headOutput, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
					Key:    &extractLocation,
					Bucket: &f.Bucket,
				})
				if err != nil {
					t.Errorf(err.Error())
				}
				t.Logf("local: %s - %s %s", f.ETag, *headOutput.ETag, extractLocation)
				if f.ETag != *headOutput.ETag {
					t.Errorf("etags do not match")
				}
			}
		})
	}
}

func createManifest(client *s3.Client, fileList []TestFile) {

	b := bytes.Buffer{}
	w := csv.NewWriter(&b)
	for _, i := range fileList {
		record := []string{i.Bucket, i.Key, fmt.Sprintf("%d", i.Size)}
		if err := w.Write(record); err != nil {
			panic(err)
		}
	}
	w.Flush()
	putObjectMPU(context.TODO(), client, testBucket, manifestTestCsvFile, b.Bytes())
}

type TestFile struct {
	ETag   string
	Bucket string
	Key    string
	Size   int
}

func setup() {
	fmt.Printf("creating test-data\n")
	if testRegion == "" {
		log.Fatalln("S3TAR_TEST_REGION not set")
	}
	data := make([]byte, 1024*1024*5)
	// setup large files (5MB)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("test-data/large/%04d", i)
		output := putObjectMPU(context.TODO(), client, testBucket, name, data)
		size := 1024 * 1024 * 5
		fmt.Printf("created %s %s\n", name, *output.ETag)
		largeTestFileList = append(largeTestFileList, TestFile{
			Bucket: testBucket,
			Key:    name,
			ETag:   *output.ETag,
			Size:   size,
		})
		data = bytes.Repeat([]byte{byte(i + 1)}, size)
	}
	for i := 3; i < 8; i++ {
		name := fmt.Sprintf("test-data/small/%04d", i)
		size := 1024 * 1024
		output := putObjectMPU(context.TODO(), client, testBucket, name, data[0:size])
		fmt.Printf("created %s %s\n", name, *output.ETag)
		smallTestFileList = append(smallTestFileList, TestFile{
			ETag:   *output.ETag,
			Bucket: testBucket,
			Key:    name,
			Size:   size,
		})
		data = bytes.Repeat([]byte{byte(i + 1)}, size)
	}

	createManifest(client, append(largeTestFileList, smallTestFileList...))

}
func teardown() {
	fmt.Printf("tear down\n")
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &testBucket,
	})
	if err != nil {
		panic(err)
	}
	var objList []types.ObjectIdentifier
	for _, o := range output.Contents {
		fmt.Printf("deleting s3://%s/%s\n", testBucket, *o.Key)
		objList = append(objList, types.ObjectIdentifier{Key: o.Key})
	}
	_, err = client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: &testBucket,
		Delete: &types.Delete{
			Objects: objList,
		},
	})
	if err != nil {
		panic(err)
	}
}

func TestMain(m *testing.M) {
	client = s3Client(context.TODO(), config.WithRegion(testRegion))
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}

// because Extract uses MPU to be able to do CopyPart
// we use a multipart object instead of PutObject so
// we can compare the extract's etag
func putObjectMPU(ctx context.Context, svc *s3.Client, bucket, key string, data []byte) *s3.CompleteMultipartUploadOutput {
	createMPU := &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	}
	var partNum int32 = 1
	createOutput, err := svc.CreateMultipartUpload(ctx, createMPU)
	if err != nil {
		panic(err)
	}
	uploadPartInput := &s3.UploadPartInput{
		UploadId:   createOutput.UploadId,
		Bucket:     &bucket,
		Key:        &key,
		PartNumber: partNum,
		Body:       bytes.NewReader(data),
	}
	uploadPart, err := svc.UploadPart(ctx, uploadPartInput)
	if err != nil {
		panic(err)
	}

	completeMPU := &s3.CompleteMultipartUploadInput{
		UploadId: createOutput.UploadId,
		Bucket:   &bucket,
		Key:      &key,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       uploadPart.ETag,
					PartNumber: uploadPartInput.PartNumber,
				},
			},
		},
	}

	output, err := svc.CompleteMultipartUpload(ctx, completeMPU)
	if err != nil {
		panic(err)
	}
	return output
}
