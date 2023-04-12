package main

import (
	"fmt"
	"os"
	"testing"
)

var (
	testBucket          = os.Getenv("S3TAR_TEST_BUCKET")
	testRegion          = os.Getenv("S3TAR_TEST_REGION")
	simpleTarTestFile   = "s3://" + testBucket + "/simple-test.tar"
	sourceDataDir       = "s3://" + testBucket + "/data/"
	extractDir          = "s3://" + testBucket + "/extract/"
	manifestTarTestFile = "s3://" + testBucket + "/manifest-tests.tar"
	manifestTestCsvFile = "s3://" + testBucket + "/manifest.csv"
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
			name: "create-simple",
			args: args{
				[]string{firstArgs,
					"--region", testRegion,
					"-cf", simpleTarTestFile,
					sourceDataDir,
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
					"-m", manifestTestCsvFile,
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
					"-xf", simpleTarTestFile,
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
		})
	}
}
