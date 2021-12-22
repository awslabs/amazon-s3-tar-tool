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
	pad         = make([]byte, beginningPad)
)

type contextKey string

const (
	contextKeyS3Client = contextKey("s3-client")
)

func ServerSideTar(incoming context.Context, svc *s3.Client, opts *SSTarS3Options) {

	ctx := context.WithValue(incoming, contextKeyS3Client, svc)
	start := time.Now()
	objectList := listAllObjects(ctx, svc, opts.SrcBucket, opts.SrcPrefix)
	smallFiles := false

	for _, o := range objectList {
		if o.Size < int64(beginningPad) {
			smallFiles = true
			break
		}
	}

	if smallFiles {
		headers := processHeaders(ctx, objectList, false, opts)
		processSmallFiles(ctx, headers, objectList, opts)
		return
	} else {
		headers := processHeaders(ctx, objectList, true, opts)
		processLargeParts(ctx, headers, objectList, opts)
	}

	elapsed := time.Since(start)
	log.Printf("Time elapsed: %s", elapsed)

}

func _processSmallFiles(ctx context.Context, objectList []types.Object, start, end int, opts *SSTarS3Options) (S3Obj, error) {
	client := GetS3Client(ctx)
	parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
	var accumParts S3Obj = startBlock
	for i, partNum := start, 0; i < end; i, partNum = i+1, partNum+1 {
		log.Printf("Processing: %s", *objectList[i].Key)
		istr := fmt.Sprintf("%04d", i)
		var prev types.Object
		if (i - 1) >= 0 {
			prev = objectList[i-1]
		}
		header := buildHeader(objectList[i], prev, false)

		{
			var err error
			pair := []S3Obj{accumParts, header}
			partName := strings.Join([]string{"iteration", "header", istr}, ".")
			dstKey := filepath.Join(parentPartsKey, partName)
			accumParts, err = concatObjects(ctx, client, 0, pair, opts.DstBucket, dstKey)
			if err != nil {
				log.Fatal(err.Error())
			}
		}

		partName := strings.Join([]string{"iteration", "part", istr}, ".")
		dstKey := filepath.Join(parentPartsKey, partName)
		pair := []S3Obj{accumParts, {
			Object:  objectList[i],
			Bucket:  opts.SrcBucket,
			PartNum: partNum,
		}}

		var err error
		accumParts, err = concatObjects(ctx, client, 0, pair, opts.DstBucket, dstKey)
		if err != nil {
			// handle error -- delete all parts
			log.Printf("error recursion on iteration %d\n%s", i+1, err.Error())
			break
		}
		// firstObject = false

	}

	batchName := fmt.Sprintf("%d-%d", start, end)
	dstKey := filepath.Join(parentPartsKey, strings.Join([]string{"iteration", "batch", batchName}, "."))
	finalPart, err := concatObjects(ctx, client, beginningPad, []S3Obj{accumParts}, opts.DstBucket, dstKey)
	if err != nil {
		log.Printf("error recursion on final\n%s", err.Error())
		return S3Obj{}, err
	}

	return finalPart, nil
}

func processSmallFiles(ctx context.Context, headers []S3Obj, objectList []types.Object, opts *SSTarS3Options) error {
	log.Printf("processSmallFiles path")
	client := GetS3Client(ctx)
	parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
	startBlock = createFirstBlock(ctx, client, opts.DstBucket, parentPartsKey)

	allParts := []S3Obj{}
	for i, ctr := 0, 1; i < len(objectList); i, ctr = i+1, ctr+2 {
		headers[i].PartNum = ctr
		allParts = append(allParts,
			headers[i],
			S3Obj{
				Object:  objectList[i],
				Bucket:  opts.SrcBucket,
				PartNum: ctr + 1,
			})
	}
	allParts = append(allParts, headers[len(headers)-1]) // headers has one more item than objectList, the last padding
	allParts[len(allParts)-1].PartNum = len(allParts)
	totalNumParts := len(allParts)
	log.Printf("Number of Parts to concat: %d", totalNumParts)

	indexList := []Index{}
	last := 0
	currSize := objectList[0].Size
	for i := 1; i < len(objectList); i++ {
		currSize += objectList[i].Size
		if currSize > int64(fileSizeMin*2) {
			indexList = append(indexList, Index{Start: last, End: i, Size: int(currSize)})
			last, currSize = i, 0
		}
	}
	if last != len(objectList) {
		indexList = append(indexList, Index{
			Start: last,
			End:   len(objectList),
			Size:  int(currSize),
		})
	}

	m := sync.Mutex{}
	secondBatch := []S3Obj{}
	swg := sizedwaitgroup.New(30)
	log.Printf("Created %d parts", len(indexList))
	for i, p := range indexList {
		log.Printf("%04d\t(%d)\t%d-%d", i, p.Size, p.Start, p.End)
		swg.Add()
		go func(start, end int) {
			defer swg.Done()
			newPart, err := _processSmallFiles(ctx, objectList, start, end, opts)
			if err != nil {
				panic(err)
			}
			m.Lock()
			newPart.PartNum = start
			secondBatch = append(secondBatch, newPart)
			m.Unlock()
		}(p.Start, p.End)
	}

	log.Printf("Waiting for threads")
	swg.Wait()
	sort.Slice(secondBatch, func(i, j int) bool {
		return secondBatch[i].PartNum < secondBatch[j].PartNum
	})

	// figure out if the final concat needs to be recursive
	recursiveConcat := false
	for x := 0; x < len(secondBatch)-1; x++ { //ignore last piece
		if secondBatch[x].Size < int64(fileSizeMin) {
			recursiveConcat = true
			log.Printf("Part %d of %d is %d", x, len(secondBatch), secondBatch[x].Size)
			break
		}
	}

	log.Printf("Concatenating all pieces")
	var finalObject S3Obj
	if recursiveConcat {
		padObject := S3Obj{
			Object: types.Object{
				Key:  aws.String("pad_file"),
				Size: int64(len(pad)),
			},
			Data: pad}

		for i := 0; i < len(secondBatch); i++ {
			var err error
			var pair []S3Obj
			if i == 0 {
				pair = []S3Obj{padObject, secondBatch[i]}
			} else {
				pair = []S3Obj{finalObject, secondBatch[i]}
			}
			trim := 0
			if i == len(secondBatch)-1 {
				trim = beginningPad
			}
			log.Printf("Concat(%s,%s)", *pair[0].Key, *pair[1].Key)
			finalObject, err = concatObjects(ctx, client, trim, pair, opts.DstBucket, opts.DstKey)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	} else {
		var err error
		finalObject, err = concatObjects(ctx, client, 0, secondBatch, opts.DstBucket, opts.DstKey)
		if err != nil {
			log.Printf("error recursion on final\n%s", err.Error())
			return err
		}
		log.Printf("Final piece s3://%s/%s", opts.DstBucket, *finalObject.Key)
	}

	///////////
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
	now := time.Now()
	lastBatch := []S3Obj{
		finalObject,
		{
			Bucket: opts.DstBucket,
			Object: types.Object{
				Key:          &lastHeaderKey,
				ETag:         res.ETag,
				Size:         lastblockSize,
				LastModified: &now,
			},
		},
	}
	finalObject, err = concatObjects(ctx, client, 0, lastBatch, opts.DstBucket, opts.DstKey)
	if err != nil {
		log.Printf("error recursion on final\n%s", err.Error())
		return err
	}
	log.Printf("Final piece s3://%s/%s", opts.DstBucket, *finalObject.Key)
	///////////

	log.Printf("deleting all intermediate objects")
	for _, path := range []string{filepath.Join(opts.DstPrefix, "parts"),
		filepath.Join(opts.DstPrefix, "headers")} {
		deleteList := listAllObjects(ctx, client, opts.DstBucket, path)
		deleteS3ObjectList(ctx, opts, opts.DstBucket, deleteList)
	}
	return nil

}

func processLargeParts(ctx context.Context, headers []S3Obj, objectList []types.Object, opts *SSTarS3Options) {

	svc := GetS3Client(ctx)
	joinedParts := []S3Obj{}
	filename := filepath.Base(*objectList[0].Key)
	parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
	// Join the first parts

	part1Filename := strings.Join([]string{"pad5mb", "tar_hdr1", filename, "hdr2", "part"}, ".")
	tempPart := filepath.Join(parentPartsKey, part1Filename)
	objectsToJoin := []S3Obj{
		headers[0],
		{
			Object: objectList[0],
			Bucket: opts.SrcBucket,
		},
		headers[1],
	}

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

func digesterPairs(ctx context.Context, pairsChan <-chan PartsMessage, result chan<- S3Obj, opts *SSTarS3Options) {
	svc := GetS3Client(ctx)
	for m := range pairsChan {
		filename := filepath.Base(*m.Parts[0].Key)
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

func buildHeaders(objectList []types.Object, frontPad bool, opts *SSTarS3Options) []S3Obj {
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

		if frontPad {
			if i == 0 {
				buff.Write(pad)
			} else {
				prev := objectList[i-1]
				padSize := findPadding(prev.Size)
				buff.Write(pad[:padSize])
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			log.Fatal(err)
		}
		tw.Flush()
		data := buff.Bytes()
		atomic.AddInt64(&accum, int64(len(data)+int(o.Size)))
		headerKey := filepath.Join(opts.DstPrefix, "headers", filename+".hdr")
		ETag := fmt.Sprintf("%x", md5.Sum(data))
		headers = append(headers, S3Obj{
			Bucket: opts.SrcBucket,
			Object: types.Object{
				Key:  &headerKey,
				ETag: &ETag,
				Size: int64(len(data)),
			},
			PartNum: i,
			Data:    data,
		})

	}

	return headers
}

func processHeaders(ctx context.Context, objectList []types.Object, frontPad bool, opts *SSTarS3Options) []S3Obj {
	client := GetS3Client(ctx)
	headers := buildHeaders(objectList, frontPad, opts)
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
				// log.Printf("UploadPart (bytes) into: %s/%s", *input.Bucket, *input.Key)
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
			var copySourceRange string
			if i == 0 && trimFirstBytes > 0 {
				copySourceRange = fmt.Sprintf("bytes=%d-%d", trimFirstBytes, object.Size-1)
				accumSize += object.Size - int64(trimFirstBytes)
			} else {
				copySourceRange = fmt.Sprintf("bytes=0-%d", object.Size-1)
				accumSize += object.Size
			}
			// log.Printf("%s", *object.Key)
			input := s3.UploadPartCopyInput{
				Bucket:          &bucket,
				Key:             &key,
				PartNumber:      partNum,
				UploadId:        &uploadId,
				CopySource:      aws.String(object.Bucket + "/" + *object.Key),
				CopySourceRange: aws.String(copySourceRange),
			}
			swg.Add()
			go func(input s3.UploadPartCopyInput) {
				defer swg.Done()
				// log.Printf("UploadPartCopy Range: %s CopySource: %s into %s/%s\n", *input.CopySourceRange, *input.CopySource, *input.Bucket, *input.Key)
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
	now := time.Now()
	complete = S3Obj{
		Bucket: *completeOutput.Bucket,
		Object: types.Object{
			Key:          completeOutput.Key,
			ETag:         completeOutput.ETag,
			Size:         accumSize,
			LastModified: &now,
		},
	}
	return complete, nil
}
