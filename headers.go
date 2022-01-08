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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func buildHeader(o, prev types.Object, addZeros bool) S3Obj {

	name := *o.Key
	var buff bytes.Buffer
	tw := tar.NewWriter(&buff)
	hdr := &tar.Header{
		Name:       name,
		Mode:       0600,
		Size:       o.Size,
		ModTime:    *o.LastModified,
		ChangeTime: *o.LastModified,
		AccessTime: time.Now(),
		Format:     tar.FormatGNU,
	}
	if addZeros {
		buff.Write(pad)
	}

	if prev.Size > 0 {
		padSize := findPadding(prev.Size)
		buff.Write(pad[:padSize])
	}
	if err := tw.WriteHeader(hdr); err != nil {
		log.Fatal(err)
	}
	tw.Flush()
	data := buff.Bytes()
	atomic.AddInt64(&accum, int64(len(data)+int(o.Size)))
	ETag := fmt.Sprintf("%x", md5.Sum(data))
	return S3Obj{
		Object: types.Object{
			Key:  aws.String("pad_file"),
			ETag: &ETag,
			Size: int64(len(data)),
		},
		Data: data,
	}
}

func buildHeaders(objectList []types.Object, frontPad bool, opts *SSTarS3Options) []*S3Obj {
	headers := []*S3Obj{}
	for i := 0; i < len(objectList); i++ {
		o := objectList[i]
		name := *o.Key
		filename := filepath.Base(name)
		prev := types.Object{}
		addZero := true
		if i > 0 {
			prev = objectList[i-1]
			addZero = false
		}
		headerKey := filepath.Join(opts.DstPrefix, "headers", filename+".hdr")
		newObject := buildHeader(o, prev, addZero)
		newObject.PartNum = i
		newObject.Key = aws.String(headerKey)
		headers = append(headers, &newObject)
	}
	return headers
}

func processHeaders(ctx context.Context, objectList []types.Object, frontPad bool, opts *SSTarS3Options) []*S3Obj {
	client := GetS3Client(ctx)
	headers := buildHeaders(objectList, frontPad, opts)
	sort.Sort(byPartNum(headers))

	///////////////////////
	// Create last header
	// remove 5MB
	atomic.AddInt64(&accum, -int64(beginningPad))
	lastblockSize := findPadding(accum)
	if lastblockSize == 0 {
		lastblockSize = blockSize
	}
	lastblockSize += (blockSize * 2)
	lastHeaderKey := filepath.Join(opts.DstPrefix, "headers", "last.hdr")
	lastBytes := make([]byte, lastblockSize)
	res, err := putObject(ctx, client, opts.DstBucket, lastHeaderKey, lastBytes)
	if err != nil {
		panic(err)
	}
	headers = append(headers, &S3Obj{
		Bucket: opts.DstBucket,
		Object: types.Object{
			Key:  &lastHeaderKey,
			ETag: res.ETag,
			Size: lastblockSize,
		},
		PartNum: len(headers) + 1,
	})
	return headers
}

func mergeHeaderObjects(ctx context.Context, headers []*S3Obj, objectList []types.Object, opts *SSTarS3Options) []*S3Obj {
	messages := buildObjectHeader(ctx, headers, objectList, opts)
	result := make(chan *S3Obj)
	var wg sync.WaitGroup
	const numDigesters = 20
	wg.Add(numDigesters)
	for i := 0; i < numDigesters; i++ {
		go func() {
			digesterPairs(ctx, messages, result, opts)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(result)
	}()
	parts := []*S3Obj{}
	for r := range result {
		parts = append(parts, r)
	}
	sort.Sort(byPartNum(parts))
	return parts
}

func buildObjectHeader(ctx context.Context, headers []*S3Obj, objectList []types.Object, opts *SSTarS3Options) <-chan PartsMessage {
	// pairs
	pairsChan := make(chan PartsMessage)
	go func() {
		defer close(pairsChan)
		for i := 1; i < len(objectList); i++ {
			concatParts := []*S3Obj{
				{Object: objectList[i], Bucket: opts.SrcBucket},
				headers[i+1],
			}

		loop:
			select {
			case pairsChan <- PartsMessage{concatParts, i}:
			case <-ctx.Done():
				break loop
			}
		}
	}()
	return pairsChan
}
