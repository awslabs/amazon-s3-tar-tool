package s3tar

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/remeh/sizedwaitgroup"
)

const (
	blockSize    = int64(512)
	beginningPad = 5 * 1024 * 1024
	fileSizeMin  = beginningPad
)

var (
	accum int64 = 0
	pad         = make([]byte, beginningPad)
	rc    *RecursiveConcat
)

func ServerSideTar(incoming context.Context, svc *s3.Client, opts *SSTarS3Options) {

	ctx := context.WithValue(incoming, contextKeyS3Client, svc)
	start := time.Now()
	objectList := listAllObjects(ctx, svc, opts.SrcBucket, opts.SrcPrefix)
	smallFiles := false

	totalSize := int64(0)
	for _, o := range objectList {
		totalSize += o.Size
		if o.Size < int64(beginningPad) {
			smallFiles = true
		}
	}
	if totalSize < fileSizeMin {
		log.Fatalf("Total size of all archives is less than 5MB. Include more files")
	}

	var err error
	log.Printf("%s %s %s", opts.DstBucket, opts.DstKey, opts.Region)
	rc, err = NewRecursiveConcat(ctx, RecursiveConcatOptions{
		Bucket: opts.DstBucket,
		Key:    opts.DstPrefix,
		Region: opts.Region,
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	if smallFiles {
		headers := processHeaders(ctx, objectList, false, opts)
		processSmallFiles(ctx, headers, objectList, opts)
		return
	} else {
		headers := processHeaders(ctx, objectList, true, opts)
		processLargeFiles(ctx, headers, objectList, opts)
	}

	elapsed := time.Since(start)
	log.Printf("Time elapsed: %s", elapsed)

}

func processSmallFiles(ctx context.Context, headers []*S3Obj, objectList []types.Object, opts *SSTarS3Options) error {

	log.Printf("processSmallFiles path")
	client := GetS3Client(ctx)

	allParts := []*S3Obj{}
	for i, ctr := 0, 1; i < len(objectList); i, ctr = i+1, ctr+2 {
		headers[i].PartNum = ctr
		allParts = append(allParts,
			headers[i],
			&S3Obj{
				Object:  objectList[i],
				Bucket:  opts.SrcBucket,
				PartNum: ctr + 1,
			})
	}
	allParts = append(allParts, headers[len(headers)-1]) // headers has one more item than objectList, the last padding
	allParts[len(allParts)-1].PartNum = len(allParts)
	totalNumParts := len(allParts)
	log.Printf("Number of Parts to concat: %d", totalNumParts)

	// Walk through all the parts and build groups of 10MB
	// so we can parallelize.
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

	// Make the last part include everything till the end.
	// We don't want something that is less than 5MB
	indexList[len(indexList)-1].End = len(objectList)
	indexList[len(indexList)-1].Size = indexList[len(indexList)-1].Size + int(currSize)

	m := sync.Mutex{}
	groups := []*S3Obj{}
	swg := sizedwaitgroup.New(100)
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
			groups = append(groups, newPart)
			m.Unlock()
		}(p.Start, p.End)
	}

	log.Printf("Waiting for threads")
	swg.Wait()
	sort.Sort(byPartNum(groups))

	// reset partNum counts.
	// Figure out if the final concat needs to be recursive
	recursiveConcat := false
	for x := 0; x < len(groups)-1; x++ { //ignore last piece
		groups[x].PartNum = x + 1
		if groups[x].Size < int64(fileSizeMin) {
			recursiveConcat = true
		}
	}
	groups[len(groups)-1].PartNum = len(groups) // setup the last PartNum since we skipped it

	log.Printf("Concatenating all pieces")
	var finalObject *S3Obj
	if recursiveConcat {
		padObject := &S3Obj{
			Object: types.Object{
				Key:  aws.String("pad_file"),
				Size: int64(len(pad)),
			},
			Data: pad}
		for i := 0; i < len(groups); i++ {
			var err error
			var pair []*S3Obj
			if i == 0 {
				pair = []*S3Obj{padObject, groups[i]}
			} else {
				pair = []*S3Obj{finalObject, groups[i]}
			}
			trim := 0
			if i == len(groups)-1 {
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
		log.Printf("All Concat")
		finalObject, err = concatObjects(ctx, client, 0, groups, opts.DstBucket, opts.DstKey)
		if err != nil {
			log.Printf("error recursion on final\n%s", err.Error())
			return err
		}
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
	lastBatch := []*S3Obj{
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

func _processSmallFiles(ctx context.Context, objectList []types.Object, start, end int, opts *SSTarS3Options) (*S3Obj, error) {
	parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
	parts := []*S3Obj{}
	for i, partNum := start, 0; i < end; i, partNum = i+1, partNum+1 {
		log.Printf("Processing: %s", *objectList[i].Key)
		// istr := fmt.Sprintf("%04d", i)
		var prev types.Object
		if (i - 1) >= 0 {
			prev = objectList[i-1]
		}
		header := buildHeader(objectList[i], prev, false)
		pairs := []*S3Obj{&header, {
			Object:  objectList[i],
			Bucket:  opts.SrcBucket,
			PartNum: partNum,
		}}
		parts = append(parts, pairs...)
	}

	batchName := fmt.Sprintf("%d-%d", start, end)
	dstKey := filepath.Join(parentPartsKey, strings.Join([]string{"iteration", "batch", batchName}, "."))
	finalPart, err := rc.ConcatObjects(ctx, parts, opts.DstBucket, dstKey)
	if err != nil {
		log.Printf("error recursion on final\n%s", err.Error())
		return &S3Obj{}, err
	}

	return finalPart, nil
}

func processLargeFiles(ctx context.Context, headers []*S3Obj, objectList []types.Object, opts *SSTarS3Options) {

	svc := GetS3Client(ctx)
	joinedParts := []*S3Obj{}
	filename := filepath.Base(*objectList[0].Key)
	parentPartsKey := filepath.Join(opts.DstPrefix, "parts")
	// Join the first parts

	part1Filename := strings.Join([]string{"pad5mb", "tar_hdr1", filename, "hdr2", "part"}, ".")
	tempPart := filepath.Join(parentPartsKey, part1Filename)
	objectsToJoin := []*S3Obj{
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
	log.Printf("Finished:\ns3://%s/%s", finalObject.Bucket, *finalObject.Key)
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

func digesterPairs(ctx context.Context, pairsChan <-chan PartsMessage, result chan<- *S3Obj, opts *SSTarS3Options) {
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

func concatObjects(ctx context.Context, client *s3.Client, trimFirstBytes int, objectList []*S3Obj, bucket, key string) (*S3Obj, error) {
	complete := &S3Obj{}
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
	complete = &S3Obj{
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
