// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func buildToc(ctx context.Context, objectList []*S3Obj) (*S3Obj, *S3Obj) {

	headers := processHeaders(ctx, objectList, false)
	toc := _buildToc(ctx, headers, objectList)

	// Build a header with the original data
	tocObj := NewS3Obj()
	tocObj.Key = aws.String("toc.csv")
	tocObj.AddData(toc.Bytes())
	tocHeader := buildHeader(tocObj, nil, false)
	tocHeader.Bucket = objectList[0].Bucket
	tocObj.Bucket = objectList[0].Bucket

	return tocObj, &tocHeader
}

func _buildToc(ctx context.Context, headers []*S3Obj, objectList []*S3Obj) *bytes.Buffer {

	var currLocation int64 = 0
	data := createCSVTOC(currLocation, headers, objectList)
	estimate := int64(data.Len())

	for {
		data = createCSVTOC(int64(estimate), headers, objectList)
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

func createCSVTOC(offset int64, headers []*S3Obj, objectList []*S3Obj) *bytes.Buffer {
	headerOffset := paxTarHeaderSize
	if tarFormat == tar.FormatGNU {
		headerOffset = gnuTarHeaderSize
	}
	var currLocation int64 = offset + headerOffset
	currLocation = currLocation + findPadding(currLocation)
	buf := bytes.Buffer{}
	toc := [][]string{}

	for i := 0; i < len(objectList); i++ {
		currLocation += headers[i].Size
		line := []string{}
		line = append(line,
			*objectList[i].Key,
			fmt.Sprintf("%d", currLocation),
			fmt.Sprintf("%d", objectList[i].Size),
			*objectList[i].ETag)
		toc = append(toc, line)
		currLocation += objectList[i].Size
	}
	cw := csv.NewWriter(&buf)
	if err := cw.WriteAll(toc); err != nil {
		log.Fatal(err.Error())
	}
	cw.Flush()

	return &buf
}

func buildFirstPart(csvData []byte) *S3Obj {
	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)
	hdr := &tar.Header{
		Name:       "toc.csv",
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

// GenerateToc creates a TOC csv of an existing TAR file (not created by s3tar)
// tar file MUST NOT have compression.
// tar file must be on the local file system to.
// TODO: It should be possible to generate a TOC from an existing TAR already by only reading the headers and skipping the data.
func GenerateToc(tarFile, outputToc string, opts *S3TarS3Options) error {

	r, err := os.Open(tarFile)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	w, err := os.Create(outputToc)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	cw := csv.NewWriter(w)
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		offset, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		offsetStr := fmt.Sprintf("%d", offset)
		size := fmt.Sprintf("%d", h.Size)
		record := []string{h.Name, offsetStr, size, ""}
		if err = cw.Write(record); err != nil {
			return err
		}

	}
	cw.Flush()
	return nil
}
