// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import "testing"

func TestExtractBucketAndPath(t *testing.T) {
	type args struct {
		s3url string
	}
	tests := []struct {
		name       string
		args       args
		wantBucket string
		wantPath   string
	}{
		{
			name:       "valid path",
			args:       args{s3url: "s3://bucket/prefix"},
			wantBucket: "bucket",
			wantPath:   "prefix",
		},
		{
			name:       "valid path - end in slash",
			args:       args{s3url: "s3://bucket/prefix/"},
			wantBucket: "bucket",
			wantPath:   "prefix/",
		},
		{
			name:       "valid path, no prefix",
			args:       args{s3url: "s3://bucket"},
			wantBucket: "bucket",
			wantPath:   "",
		},
		{
			name:       "invalid path",
			args:       args{s3url: "/home/yanko"},
			wantBucket: "",
			wantPath:   "",
		},
		{
			name:       "no path",
			args:       args{s3url: ""},
			wantBucket: "",
			wantPath:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotPath := ExtractBucketAndPath(tt.args.s3url)
			if gotBucket != tt.wantBucket {
				t.Errorf("ExtractBucketAndPath() gotBucket = %v, want %v", gotBucket, tt.wantBucket)
			}
			if gotPath != tt.wantPath {
				t.Errorf("ExtractBucketAndPath() gotPath = %v, want %v", gotPath, tt.wantPath)
			}
		})
	}
}
