// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func LoadCSV(ctx context.Context, fpath string, skipHeader bool) ([]*S3Obj, error) {

	var r io.ReadCloser
	var err error
	if strings.Contains(fpath, "s3://") {
		r, err = loadS3CSV(ctx, fpath)
	} else {
		r, err = loadLocalFile(fpath)
	}
	defer r.Close()
	if err != nil {
		return nil, err
	}

	return parseCSV(r, skipHeader)

}

func loadS3CSV(ctx context.Context, s3path string) (io.ReadCloser, error) {

	client := GetS3Client(ctx)
	bucket, key := ExtractBucketAndPath(s3path)

	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func loadLocalFile(fpath string) (io.ReadCloser, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	return f, err
}

func parseCSV(f io.Reader, skipHeader bool) ([]*S3Obj, error) {

	data := []*S3Obj{}

	r := csv.NewReader(f)
	for lineNumber := 0; ; lineNumber++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if lineNumber == 0 && skipHeader {
			continue
		}
		if len(record) != 3 {
			log.Printf("not enough values in csv line. skipping line %d", lineNumber+1)
			continue
		}

		size, err := strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			log.Printf("unable to parse size. setting to zero")
			size = 0
		}

		obj := NewS3ObjOptions(
			WithBucketAndKey(record[0], record[1]),
			WithSize(size))
		data = append(data, obj)
	}

	return data, nil

}
