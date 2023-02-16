package s3tar

import (
	"archive/tar"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/remeh/sizedwaitgroup"
)

func Extract(ctx context.Context, svc *s3.Client, opts *S3TarS3Options) error {

	manifest, err := extractCSVManifest(ctx, svc, opts.SrcBucket, opts.SrcPrefix)
	if err != nil {
		return err
	}

	wg := sizedwaitgroup.New(50)

	for _, metadata := range manifest {
		wg.Add()
		go func(metadata *FileMetadata) {
			dstKey := filepath.Join(opts.DstPrefix, metadata.Filename)
			err = extractRange(ctx, svc, opts.SrcBucket, opts.SrcPrefix, dstKey, metadata.Start, metadata.Size, opts)
			if err != nil {
				Fatalf(ctx, err.Error())
			}
			wg.Done()
		}(metadata)
	}
	wg.Wait()

	return nil
}

func extractRange(ctx context.Context, svc *s3.Client, bucket, key, dstKey string, start, size int64, opts *S3TarS3Options) error {

	output, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		return err
	}
	uploadId := *output.UploadId
	copySourceRange := fmt.Sprintf("bytes=%d-%d", start, start+size-1)

	Infof(ctx, "s3://%s/%s", bucket, dstKey)

	input := s3.UploadPartCopyInput{
		Bucket:          &bucket,
		Key:             &dstKey,
		PartNumber:      1,
		UploadId:        &uploadId,
		CopySource:      aws.String(bucket + "/" + key),
		CopySourceRange: aws.String(copySourceRange),
	}

	res, err := svc.UploadPartCopy(ctx, &input)
	if err != nil {
		return err
	}

	parts := []types.CompletedPart{
		types.CompletedPart{
			ETag:       res.CopyPartResult.ETag,
			PartNumber: 1},
	}

	completeOutput, err := svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &dstKey,
		UploadId: &uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return err
	}
	Infof(ctx, "extracted: s3://%s/%s", *completeOutput.Bucket, *completeOutput.Key)
	return nil
}

type Manifest []*FileMetadata
type FileMetadata struct {
	Filename string
	Start    int64
	Size     int64
}

func extractCSVManifest(ctx context.Context, svc *s3.Client, bucket, key string) (Manifest, error) {
	var m Manifest
	// extract the first 512 bytes which contain the tar header
	output, err := getObjectRange(ctx, svc, bucket, key, 0, 512-1)
	if err != nil {
		return m, err
	}
	tr := tar.NewReader(output)
	hdr, err := tr.Next()
	if err != nil {
		return m, err
	}
	// extract the csv now that we know the length of the CSV
	output, err = getObjectRange(ctx, svc, bucket, key, 512, 512+hdr.Size-1)
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
			// log.Fatal(err.Error())
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
