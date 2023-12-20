package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
	"io"
	"time"
)

func buildInMemoryConcat(ctx context.Context, client *s3.Client, objectList []*S3Obj, estimatedSize int64, opts *S3TarS3Options) (*S3Obj, error) {

	largestObjectSize := findLargestObject(objectList)

	if largestObjectSize > partSizeMax {
		return nil, fmt.Errorf("largest object is over the 5GiB limit\n")
	}

	if estimatedSize < fileSizeMin {
		data, err := tarGroup(ctx, client, objectList)
		if err != nil {
			return nil, err
		}
		return uploadObject(ctx, client, opts.DstBucket, opts.DstKey, data, opts.storageClass)
	} else {

		sizeLimit := findMinimumPartSize(estimatedSize, largestObjectSize)

		Infof(ctx, "mpu partsize: %s, estimated ram usage: %s\n", formatBytes(sizeLimit), formatBytes(sizeLimit*int64(threads)*3))

		// TODO: fix TOC to be pre-appended
		// tocObj, _, err := buildToc(ctx, objectList)
		// if err != nil {
		// 	panic(err)
		// }
		// objectList = append([]*S3Obj{tocObj}, objectList...)

		groups := splitSliceBySizeLimit(sizeLimit, objectList)
		if len(groups) > maxPartNumLimit {
			return nil, fmt.Errorf("number of parts exceeded the number of mpu parts allowed\n")
		}

		// create MPU
		mpu, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:            &opts.DstBucket,
			Key:               &opts.DstKey,
			StorageClass:      opts.storageClass,
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		})
		if err != nil {
			Errorf(ctx, "unable to create multipart")
			return nil, err
		}

		parts := make([]types.CompletedPart, len(groups))
		partsSizeList := make([]int64, len(groups))

		processGroups := func() error {
			g, _ := errgroup.WithContext(context.Background())
			g.SetLimit(threads)

			for i, group := range groups {
				i, group := i, group

				g.Go(func() error {

					Infof(ctx, "Part %d of %d has %d objects\n", i+1, len(groups), len(group))
					data, err := tarGroup(ctx, client, group)
					if err != nil {
						return err
					}
					partNum := int32(i + 1)

					if i != len(groups)-1 { // only on the last iteration we leave the 2 block padding tar EOF.
						data = data[0 : len(data)-1024]
					}

					rc, err := uploadPart(ctx, client, *mpu.UploadId, opts.DstBucket, opts.DstKey, data, &partNum)
					if err != nil {
						return err
					}
					parts[i] = types.CompletedPart{
						ETag:           rc.ETag,
						PartNumber:     &partNum,
						ChecksumSHA256: rc.ChecksumSHA256,
					}
					partsSizeList[i] = int64(len(data))
					return nil
				})

			}

			Infof(ctx, "waiting for threads to finish")
			return g.Wait()
		}
		err = processGroups()
		if err != nil {
			return nil, err
		}

		Infof(ctx, "completing mpu-object")
		mpuOutput, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			UploadId: mpu.UploadId,
			Bucket:   &opts.DstBucket,
			Key:      &opts.DstKey,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: parts,
			},
		})
		if err != nil {
			Errorf(ctx, "unable to complete mpu")
			return nil, err
		}

		totalSize := sumSlice[int64](partsSizeList)

		now := time.Now()
		complete := &S3Obj{
			Bucket: *mpuOutput.Bucket,
			Object: types.Object{
				Key:          mpuOutput.Key,
				ETag:         mpuOutput.ETag,
				Size:         aws.Int64(totalSize),
				LastModified: &now,
			},
		}

		// once the TOC is working we need to subtract 1 to the number of files we report
		fmt.Printf("total files: %d\n", len(objectList))
		return complete, nil
	}

}

func sumSlice[T int | int32 | int64 | float64](i []T) (o T) {
	for _, v := range i {
		o += v
	}
	return
}

func findLargestObject(objectList []*S3Obj) int64 {
	var largestObject int64 = 0
	for _, o := range objectList {
		if *o.Size > largestObject {
			largestObject = *o.Size
		}
	}
	return largestObject
}

func uploadObject(ctx context.Context, client *s3.Client, bucket, key string, data []byte, storageClass types.StorageClass) (*S3Obj, error) {

	rc, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            &bucket,
		Key:               &key,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		StorageClass:      storageClass,
		Body:              bytes.NewReader(data),
	})
	if err != nil {
		return nil, err
	}

	now := time.Now()
	var complete *S3Obj
	complete = &S3Obj{
		Bucket: bucket,
		Object: types.Object{
			Key:          &key,
			ETag:         rc.ETag,
			Size:         aws.Int64(int64(len(data))),
			LastModified: &now,
		},
	}

	return complete, nil
}
func uploadPart(ctx context.Context, client *s3.Client, uploadId, bucket, key string, data []byte, partNum *int32) (*s3.UploadPartOutput, error) {

	rc, err := client.UploadPart(ctx, &s3.UploadPartInput{
		UploadId:          &uploadId,
		Bucket:            &bucket,
		Key:               &key,
		PartNumber:        partNum,
		Body:              bytes.NewReader(data),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})

	return rc, err

}

func tarGroup(ctx context.Context, client *s3.Client, objectList []*S3Obj) ([]byte, error) {

	buf := bytes.Buffer{}
	tw := tar.NewWriter(&buf)

	for _, o := range objectList {
		var r io.ReadCloser
		var err error
		if len(o.Data) > 0 {
			r = io.NopCloser(bytes.NewReader(o.Data))
		} else {
			r, err = downloadS3Data(ctx, client, o)
			if err != nil {
				return nil, err
			}
		}
		defer r.Close()
		h := tar.Header{
			Name:       *o.Key,
			Size:       *o.Size,
			Mode:       0600,
			ModTime:    *o.LastModified,
			ChangeTime: *o.LastModified,
			AccessTime: *o.LastModified,
			Format:     tarFormat,
		}

		if err := tw.WriteHeader(&h); err != nil {
			return nil, err
		}
		if _, err := io.Copy(tw, r); err != nil {
			return nil, err
		}

	}

	if err := tw.Flush(); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil

}

func splitSliceBySizeLimit(groupSizeLimit int64, objectList []*S3Obj) [][]*S3Obj {
	var groups [][]*S3Obj
	var currentGroup []*S3Obj
	var currentSize int64 = 0
	for i := 0; i < len(objectList); i++ {

		//estimatedNextSize := currentSize + *objectList[i].Size - (blockSize * 2) // we subtract the EOF just in case this is the last block
		//if len(currentGroup) > 0 && estimatedNextSize > groupSizeLimit && currentSize > fileSizeMin {
		//	groups = append(groups, currentGroup)
		//	currentGroup = nil
		//	currentSize = 0
		//}

		currentGroup = append(currentGroup, objectList[i])
		currentSize += *objectList[i].Size

		if currentSize > groupSizeLimit && currentSize > fileSizeMin {
			groups = append(groups, currentGroup)
			currentGroup = nil
			currentSize = 0
		}
	}

	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

func downloadS3Data(ctx context.Context, client *s3.Client, object *S3Obj) (io.ReadCloser, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &object.Bucket, Key: object.Key})
	if err != nil {
		fmt.Printf("error downloading: s3://%s/%s\n", object.Bucket, *object.Key)
		return nil, err
	}
	return resp.Body, nil
}
