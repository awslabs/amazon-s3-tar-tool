// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func buildHeader(o, prev *S3Obj, addZeros bool) S3Obj {

	name := *o.Key
	var buff bytes.Buffer
	tw := tar.NewWriter(&buff)
	hdr := &tar.Header{
		Name:       name,
		Mode:       0600,
		Size:       *o.Size,
		ModTime:    *o.LastModified,
		ChangeTime: *o.LastModified,
		AccessTime: time.Now(),
		Format:     tarFormat,
	}
	if addZeros {
		buff.Write(pad)
	}

	if prev != nil && prev.Size != nil && *prev.Size > 0 {
		padSize := findPadding(*prev.Size)
		buff.Write(pad[:padSize])
	}
	if err := tw.WriteHeader(hdr); err != nil {
		log.Println("here...")
		log.Fatal(err)
	}
	if err := tw.Flush(); err != nil {
		// we ignore this error, the tar library will complain that we
		// didn't write the whole file. This part is already on Amazon S3
	}
	data := buff.Bytes()
	atomic.AddInt64(&accum, int64(len(data)+int(*o.Size)))
	ETag := fmt.Sprintf("%x", md5.Sum(data))
	return S3Obj{
		Object: types.Object{
			Key:  aws.String("header"),
			ETag: &ETag,
			Size: aws.Int64(int64(len(data))),
		},
		Data: data,
	}
}

func buildHeaders(objectList []*S3Obj, frontPad bool) []*S3Obj {
	headers := []*S3Obj{}
	for i := 0; i < len(objectList); i++ {
		o := objectList[i]
		name := *o.Key
		filename := filepath.Base(name)
		prev := &S3Obj{Object: types.Object{}}
		addZero := true
		if i > 0 {
			prev = objectList[i-1]
			addZero = false
		}
		if !frontPad {
			addZero = false
		}
		newObject := buildHeader(o, prev, addZero)
		newObject.PartNum = i
		newObject.Key = aws.String(filename + ".hdr")
		headers = append(headers, &newObject)
	}
	return headers
}

func processHeaders(ctx context.Context, objectList []*S3Obj, frontPad bool) []*S3Obj {
	headers := buildHeaders(objectList, frontPad)
	sort.Sort(byPartNum(headers))

	///////////////////////
	// Create last header
	// remove 5MB
	atomic.AddInt64(&accum, -int64(beginningPad))
	lastblockSize := findPadding(accum)
	if lastblockSize == 0 {
		lastblockSize = blockSize
	}
	lastblockSize += blockSize * 2
	lastBytes := make([]byte, lastblockSize)
	lastHeader := NewS3Obj()
	lastHeader.AddData(lastBytes)
	lastHeader.NoHeaderRequired = true
	lastHeader.Key = aws.String("last.hdr")
	lastHeader.PartNum = len(headers) + 1
	headers = append(headers, lastHeader)
	return headers
}
