// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"context"
	"encoding/csv"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"strconv"
)

func LoadCSV(ctx context.Context, svc *s3.Client, fpath string, skipHeader bool) ([]*S3Obj, int64, error) {
	r, err := loadFile(ctx, svc, fpath)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()
	return parseCSV(r, skipHeader)
}

func parseCSV(f io.Reader, skipHeader bool) ([]*S3Obj, int64, error) {

	var data []*S3Obj
	var accum int64

	r := csv.NewReader(f)
	for lineNumber := 0; ; lineNumber++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, err
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
		accum += estimateObjectSize(size)
	}

	return data, accum, nil

}
