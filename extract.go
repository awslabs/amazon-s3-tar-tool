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
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
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

	toc, err := extractCSVToc(ctx, svc, opts.SrcBucket, opts.SrcKey, opts.ExternalToc)
	if err != nil {
		return err
	}

	extract := func() error {
		g, _ := errgroup.WithContext(ctx)
		g.SetLimit(opts.Threads)

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
func List(ctx context.Context, svc *s3.Client, bucket, key string, opts *S3TarS3Options) (TOC, error) {
	if err := checkIfObjectExists(ctx, svc, bucket, key); err != nil {
		return nil, err
	}
	toc, err := extractCSVToc(ctx, svc, bucket, key, opts.ExternalToc)
	if err != nil {
		return TOC{}, err
	}
	return toc, nil
}

func extractRange(ctx context.Context, svc *s3.Client, bucket, key, dstBucket, dstKey string, start, size int64, opts *S3TarS3Options) error {
	var Metadata map[string]string
	if opts.PreservePOSIXMetadata {
		hdr, headerSize, err := extractTarHeaderEnding(ctx, svc, bucket, key, start)
		if err != nil {
			Warnf(ctx, "unable to extract tar header for %s, cannot set permissions", dstKey)
			hdr = nil
		}
		if hdr != nil {
			var mtime string = strconv.FormatInt(hdr.ModTime.UnixMilli(), 10)
			var hasATime = hdr.Format == tar.FormatGNU || hdr.Format == tar.FormatPAX
			var atime string
			if hasATime {
				atime = strconv.FormatInt(hdr.AccessTime.UnixMilli(), 10)
			} else {
				atime = mtime
			}
			Metadata = map[string]string{
				"file-permissions": fmt.Sprintf("%#o", hdr.Mode),
				"file-owner":       strconv.Itoa(hdr.Uid),
				"file-group":       strconv.Itoa(hdr.Gid),
				"file-atime":       atime,
				"file-mtime":       mtime,
			}
			Debugf(ctx, "got posix metadata permissions: %s uid: %s gid: %s name: %s from header size %d, ending %d, format %s",
				Metadata["file-permissions"], Metadata["file-owner"], Metadata["file-group"], hdr.Name,
				headerSize, start, hdr.Format,
			)
		}

	}

	output, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(dstBucket),
		Key:      aws.String(dstKey),
		ACL:      types.ObjectCannedACLBucketOwnerFullControl,
		Metadata: Metadata,
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
		PartNumber: aws.Int32(1),
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
			PartNumber: aws.Int32(1),
		},
	}
	return parts, nil
}

func extractCopyRange(ctx context.Context, svc *s3.Client, bucket string, key string, dstBucket string, dstKey string, uploadId string, copySourceRange string) ([]types.CompletedPart, error) {
	input := s3.UploadPartCopyInput{
		Bucket:          &dstBucket,
		Key:             &dstKey,
		PartNumber:      aws.Int32(1),
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
			PartNumber: aws.Int32(1),
		},
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
func extractTarHeaderEnding(ctx context.Context, svc *s3.Client, bucket, key string, end int64) (*tar.Header, int64, error) {

	headerSize := gnuTarHeaderSize
	ctr := 0

retry:

	if ctr >= 2 {
		Fatalf(ctx, "unable to parse header ending %d from TAR", end)
	}
	ctr += 1

	output, err := getObjectRange(ctx, svc, bucket, key, end-headerSize, end-1)
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

func extractCSVToc(ctx context.Context, svc *s3.Client, bucket, key, externalToc string) (TOC, error) {
	var m TOC

	var output io.ReadCloser
	// for regular s3tar files that have a toc in them, else files with external TOCs
	if externalToc == "" {
		hdr, offset, err := extractTarHeader(ctx, svc, bucket, key)
		if err != nil {
			return m, err
		}
		// extract the csv now that we know the length of the CSV
		output, err = getObjectRange(ctx, svc, bucket, key, offset, offset+hdr.Size-1)
		if err != nil {
			return m, err
		}
	} else {
		fmt.Printf("using external-toc: %s\n", externalToc)
		var err error
		output, err = loadFile(ctx, svc, externalToc)
		if err != nil {
			return m, err
		}
	}
	defer output.Close()
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
