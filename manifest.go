// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func buildManifest(ctx context.Context, objectList []*S3Obj) (*S3Obj, *S3Obj) {

	headers := processHeaders(ctx, objectList, false)
	manifest := _buildManifest(ctx, headers, objectList)

	// Build a header with the original data
	manifestObj := NewS3Obj()
	manifestObj.Key = aws.String("manifest.csv")
	manifestObj.AddData(manifest.Bytes())
	manifestHeader := buildHeader(manifestObj, nil, false)
	manifestHeader.Bucket = objectList[0].Bucket
	manifestObj.Bucket = objectList[0].Bucket

	return manifestObj, &manifestHeader
}

func _buildManifest(ctx context.Context, headers []*S3Obj, objectList []*S3Obj) *bytes.Buffer {

	var currLocation int64 = 0
	data := createCSVManifest(currLocation, headers, objectList)
	estimate := int64(data.Len())

	for {
		data = createCSVManifest(int64(estimate), headers, objectList)
		l := int64(data.Len())
		lp := l + findPadding(l)
		if lp >= estimate {
			break
		} else {
			estimate = lp
		}
	}

	return data
}

func createCSVManifest(offset int64, headers []*S3Obj, objectList []*S3Obj) *bytes.Buffer {
	headerOffset := paxTarHeaderSize
	if tarFormat == tar.FormatGNU {
		headerOffset = gnuTarHeaderSize
	}
	var currLocation int64 = offset + headerOffset
	currLocation = currLocation + findPadding(currLocation)
	buf := bytes.Buffer{}
	manifest := [][]string{}

	for i := 0; i < len(objectList); i++ {
		currLocation += headers[i].Size
		// log.Printf("%d -> %d -> %s", currLocation, objectList[i].Size, *objectList[i].Key)
		line := []string{}
		line = append(line,
			*objectList[i].Key,
			fmt.Sprintf("%d", currLocation),
			fmt.Sprintf("%d", objectList[i].Size),
			*objectList[i].ETag)
		manifest = append(manifest, line)
		currLocation += objectList[i].Size
	}
	cw := csv.NewWriter(&buf)
	if err := cw.WriteAll(manifest); err != nil {
		log.Fatal(err.Error())
	}
	cw.Flush()

	return &buf
}

func buildFirstPart(csvData []byte) *S3Obj {
	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)
	hdr := &tar.Header{
		Name:       "manifest.csv",
		Mode:       0600,
		Size:       int64(len(csvData)),
		ModTime:    time.Now(),
		ChangeTime: time.Now(),
		AccessTime: time.Now(),
		Format:     tarFormat,
	}
	buf.Write(pad)
	if err := tw.WriteHeader(hdr); err != nil {
		log.Fatal(err)
	}
	if err := tw.Flush(); err != nil {
		// we ignore this error, the tar library will complain that we
		// didn't write the whole file. This part is already on Amazon S3
	}
	buf.Write(csvData)

	padding := findPadding(int64(len(csvData)))
	if padding == 0 {
		padding = blockSize
	}
	lastBytes := make([]byte, padding)
	buf.Write(lastBytes)

	endPadding := NewS3Obj()
	endPadding.AddData(buf.Bytes())
	return endPadding
}
