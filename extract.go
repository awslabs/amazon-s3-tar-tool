// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	gnuTarHeaderSize = blockSize * 1
	paxTarHeaderSize = blockSize * 3
)

// Extract will unpack the tar file from source to target without downloading the archive locally.
// The archive has to be created with the manifest option.
func Extract(ctx context.Context, svc *s3.Client, prefix string, opts *S3TarS3Options) error {

	if err := checkIfObjectExists(ctx, svc, opts.SrcBucket, opts.SrcKey); err != nil {
		return err
	}

	toc, err := extractCSVToc(ctx, svc, opts.SrcBucket, opts.SrcKey)
	if err != nil {
		return err
	}

	extract := func() error {
		g, _ := errgroup.WithContext(ctx)
		g.SetLimit(50)

		for _, f := range toc {
			f := f
			if strings.HasPrefix(f.Filename, prefix) {
				g.Go(func() error {
					dstKey := filepath.Join(opts.DstPrefix, f.Filename)
					err = extractRange(ctx, svc, opts.SrcBucket, opts.SrcKey, opts.DstBucket, dstKey, f.Start, f.Size, opts)
					if err != nil {
						Fatalf(ctx, err.Error())
					}
					return nil
				})
			}
		}

		return g.Wait()
	}

	return extract()
}

var ErrUnableToAccess = errors.New("unable to access")

func checkIfObjectExists(ctx context.Context, svc *s3.Client, bucket, key string) error {
	_, err := svc.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		Errorf(ctx, "%s", err.Error())
		Errorf(ctx, "does s3://%s/%s exist?", bucket, key)
		return ErrUnableToAccess
	}
	return nil
}

// List will print out the contents in a tar, we do this by just printing from the TOC.
func List(ctx context.Context, svc *s3.Client, bucket, key string) (TOC, error) {
	if err := checkIfObjectExists(ctx, svc, bucket, key); err != nil {
		return nil, err
	}
	toc, err := extractCSVToc(ctx, svc, bucket, key)
	if err != nil {
		return TOC{}, err
	}
	return toc, nil
}

func extractRange(ctx context.Context, svc *s3.Client, bucket, key, dstBucket, dstKey string, start, size int64, opts *S3TarS3Options) error {

	output, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(dstBucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		return err
	}
	uploadId := *output.UploadId
	//Infof(ctx, "s3://%s/%s", bucket, dstKey)

	var parts []types.CompletedPart
	if size > 0 {
		copySourceRange := fmt.Sprintf("bytes=%d-%d", start, start+size-1)
		parts, err = extractCopyRange(ctx, svc, bucket, key, dstBucket, dstKey, uploadId, copySourceRange)
		if err != nil {
			return err
		}
	} else {
		parts, err = extractEmptyRange(ctx, svc, dstBucket, dstKey, uploadId)
		if err != nil {
			return err
		}
	}

	completeOutput, err := svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &dstBucket,
		Key:      &dstKey,
		UploadId: &uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return err
	}
	Infof(ctx, "x s3://%s/%s", *completeOutput.Bucket, *completeOutput.Key)
	return nil
}

func extractEmptyRange(ctx context.Context, svc *s3.Client, dstBucket string, dstKey string, uploadId string) ([]types.CompletedPart, error) {
	input := s3.UploadPartInput{
		Bucket:     &dstBucket,
		Key:        &dstKey,
		PartNumber: 1,
		UploadId:   &uploadId,
		Body:       new(bytes.Buffer),
	}

	res, err := svc.UploadPart(ctx, &input)
	if err != nil {
		return nil, err
	}
	parts := []types.CompletedPart{
		types.CompletedPart{
			ETag:       res.ETag,
			PartNumber: 1},
	}
	return parts, nil
}

func extractCopyRange(ctx context.Context, svc *s3.Client, bucket string, key string, dstBucket string, dstKey string, uploadId string, copySourceRange string) ([]types.CompletedPart, error) {
	input := s3.UploadPartCopyInput{
		Bucket:          &dstBucket,
		Key:             &dstKey,
		PartNumber:      1,
		UploadId:        &uploadId,
		CopySource:      aws.String(bucket + "/" + key),
		CopySourceRange: aws.String(copySourceRange),
	}

	res, err := svc.UploadPartCopy(ctx, &input)

	if err != nil {
		return nil, err
	}
	parts := []types.CompletedPart{
		types.CompletedPart{
			ETag:       res.CopyPartResult.ETag,
			PartNumber: 1},
	}
	return parts, nil
}

type TOC []*FileMetadata
type FileMetadata struct {
	Filename string
	Start    int64
	Size     int64
	Etag     string
}

func extractTarHeader(ctx context.Context, svc *s3.Client, bucket, key string) (*tar.Header, int64, error) {

	headerSize := gnuTarHeaderSize
	ctr := 0

retry:

	if ctr >= 2 {
		return nil, 0, fmt.Errorf("unable to parse CSV TOC from TAR")
	}
	ctr += 1

	output, err := getObjectRange(ctx, svc, bucket, key, 0, headerSize-1)
	if err != nil {
		return nil, 0, err
	}
	tr := tar.NewReader(output)
	hdr, err := tr.Next()
	if err != nil {
		headerSize = paxTarHeaderSize
		goto retry
	}
	return hdr, headerSize, err
}

func extractCSVToc(ctx context.Context, svc *s3.Client, bucket, key string) (TOC, error) {
	var m TOC

	hdr, offset, err := extractTarHeader(ctx, svc, bucket, key)
	if err != nil {
		return m, err
	}
	// extract the csv now that we know the length of the CSV
	output, err := getObjectRange(ctx, svc, bucket, key, offset, offset+hdr.Size-1)
	if err != nil {
		return m, err
	}

	r := csv.NewReader(output)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if len(record) != 4 {
			Fatalf(ctx, "unable to parse csv TOC. Was this archive created with s3tar?")
		}
		start, err := StringToInt64(record[1])
		if err != nil {
			Fatalf(ctx, "Unable to parse int")
		}
		size, err := StringToInt64(record[2])
		if err != nil {
			Fatalf(ctx, "Unable to parse int")
		}
		m = append(m, &FileMetadata{
			Filename: record[0],
			Start:    start,
			Size:     size,
			Etag:     record[3],
		})
	}
	return m, nil
}

func getObjectRange(ctx context.Context, svc *s3.Client, bucket, key string, start, end int64) (io.ReadCloser, error) {
	byteRange := fmt.Sprintf("bytes=%d-%d", start, end)
	params := &s3.GetObjectInput{
		Range:  aws.String(byteRange),
		Key:    &key,
		Bucket: &bucket,
	}
	output, err := svc.GetObject(ctx, params)
	if err != nil {
		return output.Body, err
	}
	return output.Body, nil

}
