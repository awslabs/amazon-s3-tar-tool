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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/remeh/sizedwaitgroup"
)

var (
	accum int64 = 0
)

type contextKey string

const (
	contextKeyS3Client = contextKey("s3-client")
)

func ServerSideTar(incoming context.Context, svc *s3.Client, opts *SSTarS3Options) {

	ctx := context.WithValue(incoming, contextKeyS3Client, svc)
	start := time.Now()
	headers, objectList := processHeaders(ctx, opts)

	// Join the first parts
	joinedParts := []S3Obj{}
	filename := filepath.Base(*objectList[0].Key)
	parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
	part1Filename := strings.Join([]string{"pad5mb", "tar_hdr1", filename, "hdr2", "part"}, ".")
	tempPart := filepath.Join(parentPartsKey, part1Filename)
	objectsToJoin := []S3Obj{
		headers[0],
		{
			Bucket: opts.SrcBucket,
			Key:    *objectList[0].Key,
			Etag:   *objectList[0].ETag,
			Size:   objectList[0].Size,
		},
		headers[1],
	}
	// for _, x := range objectsToJoin {
	// 	log.Printf("%+v", x.Key, x.PartNum)
	// }
	firstPart, err := concatObjects(ctx, svc, 0, objectsToJoin, opts.SrcBucket, tempPart)
	if err != nil {
		log.Printf(err.Error())
	}
	firstPart.PartNum = 0
	joinedParts = append(joinedParts, firstPart)
	////////////////////////

	parts := mergeHeaderObjects(ctx, headers, objectList, opts)
	joinedParts = append(joinedParts, parts...)
	log.Printf("Concat Final Object")
	finalObject, err := concatObjects(ctx, svc, beginningPad, joinedParts, opts.DstBucket, opts.DstKey)
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Printf("Finished\n%+v", finalObject)
	elapsed := time.Since(start)
	log.Printf("Time elapsed: %s", elapsed)
	log.Printf("Deleting intermediate objects")
	err = deleteObjectList(ctx, opts, joinedParts)
	if err != nil {
		log.Printf("Error deleting parts")
	}
	err = deleteObjectList(ctx, opts, headers)
	if err != nil {
		log.Printf("Error deleting headers")
	}

}

func _deleteObjectList(ctx context.Context, opts *SSTarS3Options, objectList []S3Obj) error {
	client := GetS3Client(ctx)
	objects := make([]types.ObjectIdentifier, len(objectList))
	for i := 0; i < len(objectList); i++ {
		objects[i] = types.ObjectIdentifier{
			Key: &objectList[i].Key,
		}
	}
	params := &s3.DeleteObjectsInput{
		Bucket: &objectList[0].Bucket,
		Delete: &types.Delete{
			Quiet:   true,
			Objects: objects,
		},
	}
	response, err := client.DeleteObjects(ctx, params)
	if err != nil {
		return err
	}
	if len(response.Errors) > 0 {
		fmt.Errorf("Error deleting objects")
	}
	return nil

}
func deleteObjectList(ctx context.Context, opts *SSTarS3Options, objectList []S3Obj) error {
	batch := 1000
	for i := 0; i < len(objectList); i += batch {
		start := i
		end := i + batch
		if end >= len(objectList) {
			end = len(objectList)
		}
		part := objectList[start:end]
		err := _deleteObjectList(ctx, opts, part)
		if err != nil {
			return err
		}
	}
	return nil
}

func mergeHeaderObjects(ctx context.Context, headers []S3Obj, objectList []types.Object, opts *SSTarS3Options) []S3Obj {
	messages := buildObjectHeader(ctx, headers, objectList, opts)
	result := make(chan S3Obj)
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
	parts := []S3Obj{}
	for r := range result {
		parts = append(parts, r)
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNum < parts[j].PartNum
	})
	return parts
}

func buildObjectHeader(ctx context.Context, headers []S3Obj, objectList []types.Object, opts *SSTarS3Options) <-chan PartsMessage {
	// pairs
	pairsChan := make(chan PartsMessage)
	go func() {
		defer close(pairsChan)
		for i := 1; i < len(objectList); i++ {
			concatParts := []S3Obj{
				{Bucket: opts.SrcBucket, Key: *objectList[i].Key, Etag: *objectList[i].ETag, Size: objectList[i].Size},
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

func digesterPairs(ctx context.Context, pairsChan <-chan PartsMessage, result chan<- S3Obj, opts *SSTarS3Options) {
	svc := GetS3Client(ctx)
	for m := range pairsChan {
		filename := filepath.Base(m.Parts[0].Key)
		parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
		partKey := filepath.Join(parentPartsKey, filename+".part")
		newObject, err := concatObjects(ctx, svc, 0, m.Parts, opts.DstBucket, partKey)
		if err != nil {
			panic(err)
		}
		newObject.PartNum = m.PartNum
		result <- newObject
	}
}

func buildHeaders(objectList []types.Object, opts *SSTarS3Options) []S3Obj {
	headers := []S3Obj{}
	pad := make([]byte, beginningPad)
	for i := 0; i < len(objectList); i++ {
		o := objectList[i]
		name := *o.Key
		filename := filepath.Base(name)
		if string(name[len(name)-1]) == "/" {
			continue
		}
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
		if i == 0 {
			buff.Write(pad)
		} else {
			prev := objectList[i-1]
			padSize := findPadding(prev.Size)
			buff.Write(pad[:padSize])
		}
		if err := tw.WriteHeader(hdr); err != nil {
			log.Fatal(err)
		}
		tw.Flush()
		data := buff.Bytes()
		atomic.AddInt64(&accum, int64(len(data)+int(o.Size)))
		headerKey := filepath.Join(opts.DstPrefix, "headers", filename+".hdr")
		etag := fmt.Sprintf("%x", md5.Sum(data))
		headers = append(headers, S3Obj{
			Bucket:  opts.SrcBucket,
			Key:     headerKey,
			Etag:    etag,
			Size:    int64(len(data)),
			PartNum: i,
			Data:    data,
		})

	}

	return headers
}

func processHeaders(ctx context.Context, opts *SSTarS3Options) ([]S3Obj, []types.Object) {
	client := GetS3Client(ctx)
	objectList := listAllObjects(ctx, client, opts.SrcBucket, opts.SrcPrefix)
	headers := buildHeaders(objectList, opts)
	sort.Slice(headers, func(i, j int) bool {
		return headers[i].PartNum < headers[j].PartNum
	})

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
	headers = append(headers, S3Obj{
		Bucket:  opts.SrcBucket,
		Key:     lastHeaderKey,
		Etag:    *res.ETag,
		Size:    lastblockSize,
		PartNum: len(headers) + 1,
	})
	return headers, objectList
}

func concatObjects(ctx context.Context, client *s3.Client, trimFirstBytes int, objectList []S3Obj, bucket, key string) (S3Obj, error) {
	complete := S3Obj{}
	output, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return complete, err
	}
	var accumSize int64 = 0
	uploadId := *output.UploadId
	parts := []types.CompletedPart{}
	m := sync.RWMutex{}
	swg := sizedwaitgroup.New(100)
	for i, object := range objectList {
		partNum := int32(i + 1)
		if len(object.Data) > 0 {
			accumSize += int64(len(object.Data))
			input := &s3.UploadPartInput{
				Bucket:     &bucket,
				Key:        &key,
				PartNumber: partNum,
				UploadId:   &uploadId,
				Body:       bytes.NewReader(object.Data),
			}
			swg.Add()
			go func(input *s3.UploadPartInput) {
				defer swg.Done()
				log.Printf("UploadPart: %s/%s", *input.Bucket, *input.Key)
				r, err := client.UploadPart(ctx, input)
				if err != nil {
					log.Printf("error for s3://%s/%s", *input.Bucket, *input.Key)
					panic(err)
				}
				m.Lock()
				parts = append(parts, types.CompletedPart{
					ETag:       r.ETag,
					PartNumber: input.PartNumber})
				m.Unlock()
			}(input)
		} else {
			copySourceRange := fmt.Sprintf("bytes=0-%d", object.Size-1)
			if i == 0 && trimFirstBytes > 0 {
				copySourceRange = fmt.Sprintf("bytes=%d-%d", trimFirstBytes, object.Size-1)
			}
			input := s3.UploadPartCopyInput{
				Bucket:          &bucket,
				Key:             &key,
				PartNumber:      partNum,
				UploadId:        &uploadId,
				CopySource:      aws.String(object.Bucket + "/" + object.Key),
				CopySourceRange: aws.String(copySourceRange),
			}
			accumSize += object.Size
			swg.Add()
			go func(input s3.UploadPartCopyInput) {
				defer swg.Done()
				// time.Sleep(time.Duration(rand.Intn(3) * time.Now().Second()))
				log.Printf("UploadPartCopy: %s/%s - Range: %s CopySource: %s\n", *input.Bucket, *input.Key, *input.CopySourceRange, *input.CopySource)
				r, err := client.UploadPartCopy(ctx, &input)
				if err != nil {
					log.Printf("error for s3://%s/%s", *input.Bucket, *input.Key)
					panic(err)
				}
				m.Lock()
				parts = append(parts, types.CompletedPart{
					ETag:       r.CopyPartResult.ETag,
					PartNumber: input.PartNumber})
				m.Unlock()
			}(input)
		}
	}

	swg.Wait()
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	completeOutput, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return complete, err
	}
	// fmt.Printf("%+v", completeOutput)
	complete = S3Obj{
		Bucket: *completeOutput.Bucket,
		Key:    *completeOutput.Key,
		Etag:   *completeOutput.ETag,
		Size:   accumSize,
	}
	return complete, nil
}
