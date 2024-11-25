package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	s3tar "github.com/awslabs/amazon-s3-tar-tool"
	"os"
	"testing"
)

var (
	client                     *s3.Client
	testBucket                 = os.Getenv("S3TAR_TEST_BUCKET")
	testRegion                 = os.Getenv("S3TAR_TEST_REGION")
	srcPath                    = "s3://src-bucket/src-prefix"
	dstPath                    = "s3://dst-bucket/dst-key.tar"
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

type mockArchiveManifest struct {
	mockArchive
	client *s3.Client
}

func newMockArchiveManifest(client *s3.Client) s3tar.Archiver {
	return &mockArchiveManifest{client: client}
}

type mockArchive struct {
	client *s3.Client
}

func newMockArchive(client *s3.Client) s3tar.Archiver {
	return &mockArchive{client}
}
func (a *mockArchive) Extract(ctx context.Context, opts *s3tar.S3TarS3Options, optFns ...func(options *s3tar.S3TarS3Options)) error {
	return nil
}

func (a *mockArchive) List(ctx context.Context, archveS3Url string, opts *s3tar.S3TarS3Options, optFns ...func(options *s3tar.S3TarS3Options)) (s3tar.TOC, error) {
	return s3tar.TOC{}, nil
}

func (a *mockArchive) Create(ctx context.Context, options *s3tar.S3TarS3Options, optFns ...func(options *s3tar.S3TarS3Options)) error {
	if options.SrcBucket != "src-bucket" {
		return fmt.Errorf("invalid src-bucket")
	}
	if options.SrcPrefix != "src-prefix" {
		return fmt.Errorf("invalid src-key. got: %s", options.SrcPrefix)
	}
	if options.SrcKey != "" {
		return fmt.Errorf("src-key has a value. %s", options.SrcKey)
	}
	if options.DstBucket != "dst-bucket" {
		return fmt.Errorf("invalid dst-bucket")
	}
	if options.DstKey != "dst-key.tar" {
		return fmt.Errorf("invalid dst-key")
	}
	if options.SrcManifest != "" {
		return fmt.Errorf("manifest not expected")
	}

	return nil
}
func (a *mockArchive) CreateFromList(ctx context.Context, objectList []*s3tar.S3Obj, options *s3tar.S3TarS3Options, optFns ...func(options *s3tar.S3TarS3Options)) error {
	return nil
}
func (a *mockArchiveManifest) Create(ctx context.Context, options *s3tar.S3TarS3Options, optFns ...func(options *s3tar.S3TarS3Options)) error {
	if options.SrcManifest == "" {
		return fmt.Errorf("manifest expected")
	}
	if options.SrcBucket != "" {
		return fmt.Errorf("invalid src-bucket when providing manifest")
	}
	if options.SrcPrefix != "" {
		return fmt.Errorf("invalid src-key. got: %s", options.SrcPrefix)
	}
	if options.SrcKey != "" {
		return fmt.Errorf("src-key has a value. %s", options.SrcKey)
	}
	if options.DstBucket != "dst-bucket" {
		return fmt.Errorf("invalid dst-bucket")
	}
	if options.DstKey != "dst-key.tar" {
		return fmt.Errorf("invalid dst-key")
	}

	return nil
}

func mockListAllObjects(ctx context.Context, client *s3.Client, Bucket, Prefix string, filterFns ...func(types.Object) bool) ([]*s3tar.S3Obj, int64, error) {
	return []*s3tar.S3Obj{}, 0, nil
}

func mockLoadCSV(ctx context.Context, svc *s3.Client, fpath string, skipHeader, urlDecode bool) ([]*s3tar.S3Obj, int64, error) {
	return []*s3tar.S3Obj{}, 0, nil
}

func Test_cli(t *testing.T) {

	firstArgs := os.Args[0]
	type args struct {
		args []string
	}
	tests := []struct {
		name               string
		archiveInitializer func(*s3.Client) s3tar.Archiver
		listObjFun         func(context.Context, *s3.Client, string, string, ...func(types.Object) bool) ([]*s3tar.S3Obj, int64, error)
		listObjManifest    func(context.Context, *s3.Client, string, bool, bool) ([]*s3tar.S3Obj, int64, error)
		args               args
		wantErr            bool
	}{
		{
			name:               "create-simple-small",
			archiveInitializer: newMockArchive,
			listObjFun:         mockListAllObjects,
			listObjManifest:    mockLoadCSV,
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-cf", dstPath,
					srcPath,
				},
			},
			wantErr: false,
		},
		{
			name:               "create-with-manifest",
			archiveInitializer: newMockArchiveManifest,
			listObjFun:         mockListAllObjects,
			listObjManifest:    mockLoadCSV,
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-cf", dstPath,
					"-m", manifestTestCsvFile,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newArchiveClient = tt.archiveInitializer
			listAllObjects = tt.listObjFun
			loadCSV = tt.listObjManifest
			defer func() {
				newArchiveClient = s3tar.NewArchiveClient
				listAllObjects = s3tar.ListAllObjects
				loadCSV = s3tar.LoadCSV
			}()
			if err := run(tt.args.args); (err != nil) != tt.wantErr {
				t.Errorf("run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}

type TestFile struct {
	ETag   string
	Bucket string
	Key    string
	Size   int
}
