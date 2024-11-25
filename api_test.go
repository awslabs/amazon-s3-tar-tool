// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var (
	client     *s3.Client
	testBucket = os.Getenv("S3TAR_TEST_BUCKET")
	testRegion = os.Getenv("S3TAR_TEST_REGION")

	// smallFiles
	simpleSmallDataTarTestFile  = "s3://" + testBucket + "/simple-small-data-test.tar"
	sourceSmallDataDir          = "s3://" + testBucket + "/test-data/small/"
	dstBucketSmall, dstKeySmall = ExtractBucketAndPath(simpleSmallDataTarTestFile)
	srcBucketSmall, srcKeySmall = ExtractBucketAndPath(sourceSmallDataDir)

	// LargeFiles
	simpleLargeDataTarTestFile  = "s3://" + testBucket + "/simple-large-data-test.tar"
	sourceLargeDataDir          = "s3://" + testBucket + "/test-data/large/"
	dstBucketLarge, dstKeyLarge = ExtractBucketAndPath(simpleLargeDataTarTestFile)
	srcBucketLarge, srcKeyLarge = ExtractBucketAndPath(sourceLargeDataDir)
	//

	manifestTarTestFile                     = "s3://" + testBucket + "/manifest-tests.tar"
	srcBucketManifestTar, srcKeyManifestTar = ExtractBucketAndPath(manifestTarTestFile)
	manifestTestCsvFile                     = "manifest.csv"
	largeTestFileList                       []TestFile
	smallTestFileList                       []TestFile

	expectedTOC TOC
)

func TestMain(m *testing.M) {
	ctx := SetupLogger(context.Background())
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(testRegion))
	if err != nil {
		panic(err)
	}
	client = s3.NewFromConfig(cfg)
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}

type TestFile struct {
	ETag   string
	Bucket string
	Key    string
	Size   int
}

func TestArchive_Create(t *testing.T) {

	ctx := SetupLogger(context.Background())
	ctx = SetLogLevel(ctx, 0)
	type fields struct {
		client *s3.Client
	}
	type args struct {
		ctx     context.Context
		options *S3TarS3Options
		optFns  []func(options *S3TarS3Options)
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "create-small-files",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx     context.Context
				options *S3TarS3Options
				optFns  []func(options *S3TarS3Options)
			}{
				ctx: ctx,
				options: &S3TarS3Options{
					SrcBucket: srcBucketSmall,
					SrcPrefix: srcKeySmall,
					DstBucket: dstBucketSmall,
					DstKey:    dstKeySmall,
					DstPrefix: filepath.Dir(dstKeySmall),
				},
				optFns: nil,
			},
		},
		{
			name:   "create-large-files",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx     context.Context
				options *S3TarS3Options
				optFns  []func(options *S3TarS3Options)
			}{
				ctx: ctx,
				options: &S3TarS3Options{
					SrcBucket: srcBucketLarge,
					SrcPrefix: srcKeyLarge,
					DstBucket: dstBucketLarge,
					DstKey:    dstKeyLarge,
					DstPrefix: filepath.Dir(dstKeyLarge),
				},
				optFns: nil,
			},
		},
		{
			name:   "create-from-manifest",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx     context.Context
				options *S3TarS3Options
				optFns  []func(options *S3TarS3Options)
			}{
				ctx: ctx,
				options: &S3TarS3Options{
					SrcManifest: fmt.Sprintf("s3://%s/%s", testBucket, manifestTestCsvFile),
					DstBucket:   srcBucketManifestTar,
					DstKey:      srcKeyManifestTar,
					DstPrefix:   filepath.Dir(srcKeyManifestTar),
				},
				optFns: nil,
			},
		},
		// add a case to test storage-class
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &ArchiveClient{
				client: tt.fields.client,
			}
			if err := a.Create(tt.args.ctx, tt.args.options, tt.args.optFns...); (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestArchiveClient_EndOfFile(t *testing.T) {
	ctx := SetupLogger(context.Background())
	ctx = SetLogLevel(ctx, 0)

	expectedResult := make([]byte, 512*2)

	type fields struct {
		client *s3.Client
	}
	type args struct {
		tarFile string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		expected []byte
		wantErr  bool
	}{
		{
			name:   "eof-check-small-file",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				tarFile string
			}{
				tarFile: simpleSmallDataTarTestFile,
			},
			expected: expectedResult,
		},
		{
			name:   "eof-check-large-file",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				tarFile string
			}{
				tarFile: simpleLargeDataTarTestFile,
			},
			expected: expectedResult,
		},
		{
			name:   "eof-check-manifest-file",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				tarFile string
			}{
				tarFile: manifestTarTestFile,
			},
			expected: expectedResult,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key := ExtractBucketAndPath(tt.args.tarFile)
			headOutput, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
				Key:    &key,
				Bucket: &bucket,
			})
			if err != nil {
				t.Errorf(err.Error())
			}
			end := *headOutput.ContentLength
			start := end - (512 * 2)
			r, err := getObjectRange(context.TODO(), client, bucket, key, start, end)
			if err != nil {
				t.Errorf(err.Error())
			}
			data, err := io.ReadAll(r)
			if err != nil {
				t.Errorf(err.Error())
			}
			if !bytes.Equal(data, tt.expected) {
				// print where it doesn't match, useful for debugging
				for i := 0; i < len(data); i++ {
					if data[i] != 0x00 {
						t.Errorf("data at idx:%d does not match 0: %x", i, data[i])
					}
				}
				t.Fail()
			}
		})
	}
}

func TestArchiveClient_ExtractManifest(t *testing.T) {
	ctx := SetupLogger(context.Background())
	ctx = SetLogLevel(ctx, 0)

	extractDir := "s3://" + testBucket + "/extract/"
	dstBucket, dstKey := ExtractBucketAndPath(extractDir)
	dstPrefix := filepath.Dir(dstKey)

	extractDirSmall := "s3://" + testBucket + "/extract-small/"
	dstExtractSmallBucket, dstExtractSmallKey := ExtractBucketAndPath(extractDirSmall)
	dstExtractSmallPrefix := filepath.Dir(dstExtractSmallKey)

	extractDirLarge := "s3://" + testBucket + "/extract-large/"
	dstExtractLargeBucket, dstExtractLargeKey := ExtractBucketAndPath(extractDirLarge)
	dstExtractLargePrefix := filepath.Dir(dstExtractLargeKey)

	type fields struct {
		client *s3.Client
	}
	type args struct {
		ctx     context.Context
		options *S3TarS3Options
		optFns  []func(options *S3TarS3Options)
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		fileList []TestFile
		wantErr  bool
	}{
		{
			name:   "extract-manifest",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx     context.Context
				options *S3TarS3Options
				optFns  []func(options *S3TarS3Options)
			}{
				ctx: ctx,
				options: &S3TarS3Options{
					SrcBucket: srcBucketManifestTar,
					SrcKey:    srcKeyManifestTar,
					DstBucket: dstBucket,
					DstPrefix: dstPrefix,
				},
				optFns: nil,
			},
			fileList: append(largeTestFileList, smallTestFileList...),
		},
		{
			name:   "extract-small",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx     context.Context
				options *S3TarS3Options
				optFns  []func(options *S3TarS3Options)
			}{
				ctx: ctx,
				options: &S3TarS3Options{
					SrcBucket: dstBucketSmall,
					SrcKey:    dstKeySmall,
					DstBucket: dstExtractSmallBucket,
					DstPrefix: dstExtractSmallPrefix,
				},
				optFns: nil,
			},
			fileList: smallTestFileList,
		},
		{
			name:   "extract-large",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx     context.Context
				options *S3TarS3Options
				optFns  []func(options *S3TarS3Options)
			}{
				ctx: ctx,
				options: &S3TarS3Options{
					SrcBucket: dstBucketLarge,
					SrcKey:    dstKeyLarge,
					DstBucket: dstExtractLargeBucket,
					DstPrefix: dstExtractLargePrefix,
				},
				optFns: nil,
			},
			fileList: largeTestFileList,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &ArchiveClient{
				client: tt.fields.client,
			}
			if err := a.Extract(tt.args.ctx, tt.args.options, tt.args.optFns...); (err != nil) != tt.wantErr {
				t.Errorf("Extract() error = %v, wantErr %v", err, tt.wantErr)
			}
			for _, f := range tt.fileList {
				extractLocation := fmt.Sprintf("%s/%s", tt.args.options.DstPrefix, f.Key)
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

func TestArchiveClient_List(t *testing.T) {
	ctx := SetupLogger(context.Background())
	ctx = SetLogLevel(ctx, 0)
	type fields struct {
		client *s3.Client
	}
	type args struct {
		ctx          context.Context
		archiveS3Url string
		options      *S3TarS3Options
		optFns       []func(options *S3TarS3Options)
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    TOC
		wantErr bool
	}{
		{
			name:   "list",
			fields: struct{ client *s3.Client }{client: client},
			args: struct {
				ctx          context.Context
				archiveS3Url string
				options      *S3TarS3Options
				optFns       []func(options *S3TarS3Options)
			}{
				ctx:          ctx,
				archiveS3Url: manifestTarTestFile,
				options:      &S3TarS3Options{},
				optFns:       nil,
			},
			want: expectedTOC,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &ArchiveClient{
				client: tt.fields.client,
			}
			got, err := a.List(tt.args.ctx, tt.args.archiveS3Url, tt.args.options, tt.args.optFns...)
			if (err != nil) != tt.wantErr {
				t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("List() got = %v, want %v", got, tt.want)
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

	combined := append(largeTestFileList, smallTestFileList...)
	createManifest(client, combined)

	// would be great to have a way to generate this dynamically
	indices := []int64{3584, 5248000, 10492416, 15736832, 16786944, 17837056, 18887168, 19937280}

	for n, obj := range combined {
		expectedTOC = append(expectedTOC, &FileMetadata{
			Filename: obj.Key,
			Start:    indices[n],
			Size:     int64(obj.Size),
		})
	}

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
		PartNumber: aws.Int32(partNum),
		Body:       io.ReadSeeker(bytes.NewReader(data)),
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
