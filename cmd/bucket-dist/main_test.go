// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"testing"
)

var (
	csvData = `bucket-name,a/file1.txt,0
bucket-name,a/file2.txt,1
bucket-name,a/file3.txt,512
bucket-name,a/file4.txt,1024
bucket-name,a/file5.txt,2048
bucket-name,file6.txt,4096
bucket-name,file7.txt,8192
bucket-name,file8.txt,16384
bucket-name,file9.txt,32768
bucket-name,file10.txt,65536
bucket-name,file11.txt,131072
bucket-name,file12.txt,262144
bucket-name,file13.txt,524288
bucket-name,file14.txt,1048576
bucket-name,file15.txt,2097152`
)

func TestCalculateObjectSizeDistribution(t *testing.T) {
	var compressedCSV bytes.Buffer
	writer := gzip.NewWriter(&compressedCSV)
	_, err := writer.Write([]byte(csvData))
	if err != nil {
		t.Fatalf("Failed to write CSV data to gzip: %v", err)
	}
	writer.Close()

	// Mock manifest file
	mockManifest := &ManifestFile{
		SourceBucket:      "mock-source-bucket",
		DestinationBucket: "arn:aws:s3:::mock-destination-bucket",
		Version:           "2016-11-30",
		CreationTimestamp: "1719882000000",
		FileFormat:        "CSV",
		FileSchema:        "Bucket,Key,Size",
		Files: []CSVFile{
			{Key: "mock-key", Size: len(compressedCSV.Bytes()), MD5Checksum: "mock-checksum"},
		},
		SizeColumn: 2,
		KeyColumn:  1,
	}

	// Mock download function
	mockDownload := func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(compressedCSV.Bytes())), nil
	}

	// Run the function with mocked inputs
	ctx := context.Background()

	type fields struct {
		client *s3.Client
	}
	type args struct {
		ctx        context.Context
		manifest   *ManifestFile
		prefix     string
		downloadFn func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	}

	tests := []struct {
		name          string
		enabled       bool
		args          args
		expectedBins  map[string]int
		expectedTotal uint64
	}{
		{
			name:    "all-files",
			enabled: false,
			args: struct {
				ctx        context.Context
				manifest   *ManifestFile
				prefix     string
				downloadFn func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
			}{
				ctx:        ctx,
				manifest:   mockManifest,
				prefix:     "",
				downloadFn: mockDownload,
			},
			expectedBins: map[string]int{
				"0 bytes":           1,
				"1 byte to 1 KB":    2,
				"1 KB to 2 KB":      1,
				"2 KB to 4 KB":      1,
				"4 KB to 8 KB":      1,
				"8 KB to 16 KB":     1,
				"16 KB to 32 KB":    1,
				"32 KB to 64 KB":    1,
				"64 KB to 128 KB":   1,
				"128 KB to 256 KB":  1,
				"256 KB to 512 KB":  1,
				"512 KB to 1 MB":    1,
				"1 MB to 2 MB":      1,
				"Greater than 2 MB": 1,
			},
			expectedTotal: 15,
		},
		{
			name:    "prefix-files",
			enabled: true,
			args: struct {
				ctx        context.Context
				manifest   *ManifestFile
				prefix     string
				downloadFn func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
			}{
				ctx:        ctx,
				manifest:   mockManifest,
				prefix:     "a/",
				downloadFn: mockDownload,
			},
			expectedBins: map[string]int{
				"0 bytes":           1,
				"1 byte to 1 KB":    2,
				"1 KB to 2 KB":      1,
				"2 KB to 4 KB":      1,
				"4 KB to 8 KB":      0,
				"8 KB to 16 KB":     0,
				"16 KB to 32 KB":    0,
				"32 KB to 64 KB":    0,
				"64 KB to 128 KB":   0,
				"128 KB to 256 KB":  0,
				"256 KB to 512 KB":  0,
				"512 KB to 1 MB":    0,
				"1 MB to 2 MB":      0,
				"Greater than 2 MB": 0,
			},
			expectedTotal: 5,
		},
	}

	for _, tt := range tests {
		if !tt.enabled {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			bins, totalObjects := calculateObjectSizeDistribution(tt.args.ctx, tt.args.manifest, tt.args.prefix, tt.args.downloadFn)

			if totalObjects != tt.expectedTotal {
				t.Errorf("%s: Expected total objects to be %d, got %d", tt.name, tt.expectedTotal, totalObjects)
			}

			for bin, count := range tt.expectedBins {
				if bins[bin] != count {
					t.Errorf("%s: Expected %d objects in bin %s, got %d  ", tt.name, count, bin, bins[bin])
				}
			}
		})
	}
}
