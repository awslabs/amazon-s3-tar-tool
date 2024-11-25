// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"container/list"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/sync/errgroup"
)

const (
	blockSize       = int64(512)
	beginningPad    = 5 * 1024 * 1024 // 5MB
	fileSizeMin     = beginningPad
	fileSizeMax     = 1024 * 1024 * 1024 * 1024 * 5 // 5TB
	partSizeMax     = 1024 * 1024 * 1024 * 5        // 5GB
	maxPartNumLimit = 10000
)

var (
	accum     int64 = 0
	pad             = make([]byte, beginningPad)
	tarFormat       = tar.FormatPAX
	rc        *RecursiveConcat
	threads   = 100
)

func ServerSideTar(ctx context.Context, svc *s3.Client, dstSvc *s3.Client, opts *S3TarS3Options) error {

	var objectList []*S3Obj
	var err error
	if opts.SrcManifest != "" {
		Infof(ctx, "using manifest file %s", opts.SrcManifest)
		objectList, _, err = LoadCSV(ctx, svc, opts.SrcManifest, opts.SkipManifestHeader, opts.UrlDecode)
	} else if opts.SrcBucket != "" {
		Infof(ctx, "using source bucket '%s' and prefix '%s'", opts.SrcBucket, opts.SrcPrefix)
		objectList, _, err = ListAllObjects(ctx, svc, opts.SrcBucket, opts.SrcPrefix)
	} else {
		return fmt.Errorf("manifest file or source bucket required")
	}
	if err != nil {
		return err
	}

	return createFromList(ctx, svc, dstSvc, objectList, opts)
}

func createFromList(ctx context.Context, svc *s3.Client, dstSvc *s3.Client, objectList []*S3Obj, opts *S3TarS3Options) error {

	tarFormat = opts.tarFormat
	if tarFormat == tar.FormatUnknown {
		tarFormat = tar.FormatPAX
	}
	threads = opts.Threads
	ctx = context.WithValue(ctx, contextKeyS3Client, dstSvc)
	start := time.Now()

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("%v\n", r)
			fmt.Printf("recovered from a panic. Trying to clean up.\n")
		}
		if !opts.ConcatInMemory {
			cleanUp(ctx, dstSvc, opts)
		}
		elapsed := time.Since(start)
		Infof(ctx, "Time elapsed: %s", elapsed)
	}()

	Infof(ctx, "processing %d Amazon S3 Objects", len(objectList))

	smallFiles := false

	totalSize := int64(0)
	for _, o := range objectList {
		totalSize += *o.Size
		if *o.Size < int64(beginningPad) {
			smallFiles = true
		}
	}
	Infof(ctx, "final size %s (without tar headers + padding)", formatBytes(totalSize))

	if totalSize > fileSizeMax {
		return fmt.Errorf("total size (%d) of all objects is more than 5TB. Reduce the number of objects", totalSize)
	}

	concatObj := NewS3Obj()
	if opts.ConcatInMemory || totalSize < fileSizeMin {
		Debugf(ctx, "Processing small files in-memory")
		var err error
		concatObj, err = buildInMemoryConcat(ctx, dstSvc, objectList, totalSize, opts)
		if err != nil {
			return err
		}
	} else if smallFiles {
		Debugf(ctx, "Processing small files")
		var err error
		rc, err = NewRecursiveConcat(ctx, RecursiveConcatOptions{
			Client:      svc,
			Bucket:      opts.DstBucket,
			DstPrefix:   opts.DstPrefix,
			DstKey:      opts.DstKey,
			Region:      opts.Region,
			EndpointUrl: opts.EndpointUrl,
		})
		if err != nil {
			return err
		}
		headList := make([]*s3.HeadObjectOutput, len(objectList))
		if opts.PreservePOSIXMetadata {
			var wg sync.WaitGroup
			for i, obj := range objectList {
				wg.Add(1)
				go func(i int, obj *S3Obj) {
					defer wg.Done()
					if obj.NoHeaderRequired {
						headList[i] = nil
					} else {
						head := fetchS3ObjectHead(ctx, svc, obj)
						headList[i] = head
					}
				}(i, obj)
			}
			wg.Wait()
		}

		Debugf(ctx, "building toc")
		manifestObj, _, err := buildToc(ctx, objectList)
		if err != nil {
			fmt.Printf("buildToc: %s", err.Error())
			return err
		}
		objectList = append([]*S3Obj{manifestObj}, objectList...)
		headList = append([]*s3.HeadObjectOutput{nil}, headList...)
		Debugf(ctx, "prepended toc: %s Size: %d len.Data: %d", *manifestObj.Key, *manifestObj.Size, len(manifestObj.Data))
		concatObj, err = processSmallFiles(ctx, dstSvc, objectList, headList, opts.DstKey, opts)
		if err != nil {
			return err
		}
	} else {
		Debugf(ctx, "Processing large files")
		var err error
		concatObj, err = processLargeFiles(ctx, svc, dstSvc, objectList, opts)
		if err != nil {
			return err
		}
	}

	Infof(ctx, "Final Object: s3://%s/%s", concatObj.Bucket, *concatObj.Key)
	return nil
}

func cleanUp(ctx context.Context, svc *s3.Client, opts *S3TarS3Options) {
	Infof(ctx, "deleting all intermediate objects")
	scratchDirs := []string{
		filepath.Join(opts.DstPrefix, opts.DstKey+".parts"),
		filepath.Join(opts.DstPrefix, opts.DstKey, "headers"),
	}
	for _, path := range scratchDirs {
		if path == "" || path == "/" {
			continue
		}
		deleteList, _, _ := ListAllObjects(ctx, svc, opts.DstBucket, path)
		err := deleteObjectList(ctx, svc, opts, deleteList)
		if err != nil {
			Warnf(ctx, "Unable to delete intermediate objects at: %s %s", opts.DstBucket, path)
		}
	}
}

func generateLastBlock(s int64, opts *S3TarS3Options) *S3Obj {
	lastBlockSize := findPadding(s)
	if lastBlockSize == 0 {
		lastBlockSize = blockSize
	}
	lastBlockSize += blockSize * 2
	lastBytes := make([]byte, lastBlockSize)
	eofPadding := NewS3Obj()
	eofPadding.AddData(lastBytes)
	eofPadding.NoHeaderRequired = true
	return eofPadding
}

type concatresult struct {
	result *S3Obj
	err    error
}

// concatObjAndHeader will only perform pair (obj1 + hdr2) concatenation
func concatObjAndHeader(ctx context.Context, svc *s3.Client, dstSvc *s3.Client, objectList []*S3Obj, opts *S3TarS3Options) ([]*S3Obj, error) {

	ctx = context.WithValue(ctx, contextKeyS3Client, dstSvc)
	concater, err := NewRecursiveConcat(ctx, RecursiveConcatOptions{
		Client:      svc,
		Bucket:      opts.DstBucket,
		DstPrefix:   opts.DstPrefix,
		DstKey:      opts.DstKey,
		Region:      opts.Region,
		EndpointUrl: opts.EndpointUrl,
	})
	if err != nil {
		return nil, err
	}
	manifestObj, _, err := buildToc(ctx, objectList)
	if err != nil {
		return nil, err
	}
	firstPart := buildFirstPart(manifestObj.Data)
	firstPart.Bucket = opts.DstBucket
	objectList = append([]*S3Obj{firstPart}, objectList...)

	wg := sizedwaitgroup.New(opts.Threads)
	resultsChan := make(chan concatresult)
	var bytesAccum int64
	for i, obj := range objectList {
		nextIndex := i + 1
		var notLastBlock = nextIndex < len(objectList)
		var nextObject *S3Obj
		if notLastBlock {
			nextObject = objectList[nextIndex]
		} else {
			nextObject = nil
		}

		name := fmt.Sprintf("%d.part-%d.hdr", i, nextIndex)
		key := filepath.Join(opts.DstPrefix, opts.DstKey+".parts", name)
		wg.Add()
		go func(nextObject *S3Obj, obj *S3Obj, key string, partNum int) {
			var p1 = obj
			var p2 *S3Obj = nil
			if notLastBlock {
				var head *s3.HeadObjectOutput
				if opts.PreservePOSIXMetadata {
					head = fetchS3ObjectHead(ctx, svc, nextObject)
				} else {
					head = nil
				}

				h := buildHeader(nextObject, p1, false, head)
				p2 = &h
				bytesAccum += *p1.Size + *p2.Size
			} else {
				eofPadding := generateLastBlock(bytesAccum+*obj.Size, opts)
				p2 = eofPadding
			}
			var pairs = []*S3Obj{p1, p2}

			res, err := concater.ConcatObjects(ctx, pairs, opts.DstBucket, key)
			if err != nil {
				Infof(ctx, err.Error())
			}
			res.PartNum = partNum
			resultsChan <- concatresult{res, err}
			wg.Done()
		}(nextObject, obj, key, i+1)

	}
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var results []*S3Obj
	for r := range resultsChan {
		if r.err != nil {
			return nil, err
		}
		results = append(results, r.result)
	}
	sort.Sort(byPartNum(results))
	return results, nil
}

func fetchS3ObjectHead(ctx context.Context, svc *s3.Client, nextObject *S3Obj) *s3.HeadObjectOutput {
	Debugf(ctx, "fetching head for %s/%s", *&nextObject.Bucket, *nextObject.Key)
	head, err := svc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(nextObject.Bucket),
		Key:    nextObject.Key,
	})
	if err != nil {
		Fatalf(ctx, err.Error())
	}
	return head
}

type batchGroup struct {
	Obj *S3Obj
	Err error
}

func breakUpList(ctx context.Context, dstSvc *s3.Client, objectList []*S3Obj, opts *S3TarS3Options) ([]*S3Obj, error) {

	l := list.New()
	for i := 0; i < len(objectList); i++ {
		l.PushBack(objectList[i])
	}

	batchGoupList := [][]*S3Obj{}
	partCounter := 1
	var accum int64 = 0
	var accumList []*S3Obj
	for e := l.Front(); e != nil; {
		o := e.Value.(*S3Obj)
		temp := accum + *o.Size
		if temp < partSizeMax {
			accum = temp
			o.PartNum = partCounter
			partCounter += 1
			accumList = append(accumList, o)
			e = e.Next()
		} else {
			batchGoupList = append(batchGoupList, accumList)
			accum = 0
			partCounter = 1
			accumList = []*S3Obj{}
		}
	}

	Debugf(ctx, "finished sending batches...\npushing last batch\n")
	if len(accumList) > 0 {
		batchGoupList = append(batchGoupList, accumList)
	}

	ConcatBatch := func(batchList [][]*S3Obj) ([]*S3Obj, error) {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(opts.Threads)
		results := make([]*S3Obj, len(batchGoupList))
		for i, batch := range batchList {
			i, batch := i, batch
			g.Go(func() error {
				Debugf(ctx, "processing batch: %d\n", i)
				fn, err := randomHex(12)
				if err != nil {
					return err
				}
				tempKey := filepath.Join(opts.DstPrefix, opts.DstKey+".parts", fn)
				obj, err := concatObjects(ctx, dstSvc, 0, batch, opts.DstBucket, tempKey)
				if err == nil {
					obj.PartNum = i + 1
					results[i] = obj
				}
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		return results, nil
	}

	return ConcatBatch(batchGoupList)
}

func processLargeFiles(ctx context.Context, svc *s3.Client, dstSvc *s3.Client, objectList []*S3Obj, opts *S3TarS3Options) (*S3Obj, error) {

	results, err := concatObjAndHeader(ctx, svc, dstSvc, objectList, opts)
	if err != nil {
		return nil, err
	}

	if len(results) > 10000 {
		Infof(ctx, "objectList is larger than 10,000 files. processing in batches\n")
		var err error
		results, err = breakUpList(ctx, dstSvc, results, opts)
		if err != nil {
			return nil, err
		}
	}
	Debugf(ctx, "list reduced\n")

	tempKey := filepath.Join(opts.DstPrefix, opts.DstKey+".parts", "output.temp")
	concatObj, err := concatObjects(ctx, dstSvc, 0, results, opts.DstBucket, tempKey)
	if err != nil {
		return nil, err
	}

	finalObject, err := redistribute(ctx, dstSvc, concatObj, beginningPad, opts.DstBucket, opts.DstKey, opts.storageClass, opts.ObjectTags)
	if err != nil {
		return nil, err
	}

	Infof(ctx, "Finished: s3://%s/%s", finalObject.Bucket, *finalObject.Key)
	return finalObject, nil

}

// redistribute will try to evenly distribute the object into equal size parts.
// it will also trim whatever offset passed, helpful to remove the front padding
func redistribute(ctx context.Context, client *s3.Client, obj *S3Obj, trimoffset int64, bucket, key string, storageClass types.StorageClass, tagSet types.Tagging) (*S3Obj, error) {
	finalSize := *obj.Size - trimoffset
	min, max, mid := findMinMaxPartRange(finalSize)
	var r int64 = 0
	for i := max; i >= min; i-- {
		r = finalSize % i
		if r == 0 {
			mid = i
			break
		}
	}

	partSize := finalSize / mid
	Warnf(ctx, "redistribute calculations")
	Warnf(ctx, "parts: %d", mid)
	Warnf(ctx, "FinalSize:\t%d", finalSize)
	Warnf(ctx, "total:\t%d", partSize*mid)
	Warnf(ctx, "PartSize:\t%d", partSize)
	var start int64 = 0
	type IndexLoc struct {
		Start int64
		End   int64
		Size  int64
	}
	indexList := []IndexLoc{}
	for start = 0; start < finalSize; start = start + partSize {
		i := IndexLoc{
			Start: trimoffset + start,
			End:   trimoffset + start + partSize,
			Size:  partSize,
		}
		indexList = append(indexList, i)
		Debugf(ctx, "%v-%v", i.Start, i.End)
	}
	if indexList[len(indexList)-1].End != *obj.Size {
		indexList[len(indexList)-1].End = *obj.Size
	}

	complete := NewS3Obj()
	tags := TagsToUrlEncodedString(tagSet)
	output, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		StorageClass: storageClass,
		Tagging:      &tags,
		ACL:          types.ObjectCannedACLBucketOwnerFullControl,
	})
	if err != nil {
		Infof(ctx, err.Error())
		return nil, err
	}
	uploadId := *output.UploadId

	Redistribute := func(ctx context.Context, indexList []IndexLoc) ([]types.CompletedPart, error) {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(threads)
		parts := make([]types.CompletedPart, len(indexList))
		for i, r := range indexList {
			i, r := i, r
			g.Go(func() error {
				partNum := int32(i + 1)
				copySourceRange := fmt.Sprintf("bytes=%d-%d", r.Start, r.End-1)
				input := s3.UploadPartCopyInput{
					Bucket:          &bucket,
					Key:             &key,
					PartNumber:      &partNum,
					UploadId:        &uploadId,
					CopySource:      aws.String(obj.Bucket + "/" + *obj.Key),
					CopySourceRange: aws.String(copySourceRange),
				}
				Debugf(ctx, "UploadPartCopy (s3://%s/%s) into:\n\ts3://%s/%s", *input.Bucket, *input.Key, bucket, key)
				rc, err := client.UploadPartCopy(ctx, &input)
				if err != nil {
					Debugf(ctx, "error for s3://%s/%s", *input.Bucket, *input.Key)
					Debugf(ctx, "CopySourceRange %s", *input.CopySourceRange)
					return err
				}
				parts[i] = types.CompletedPart{
					ETag:       rc.CopyPartResult.ETag,
					PartNumber: input.PartNumber}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		return parts, nil
	}

	parts, err := Redistribute(ctx, indexList)
	if err != nil {
		return nil, err
	}
	Debugf(ctx, "len parts: %d\n", len(parts))

	completeOutput, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		Infof(ctx, err.Error())
		return nil, err
	}
	now := time.Now()
	complete = &S3Obj{
		Bucket: *completeOutput.Bucket,
		Object: types.Object{
			Key:          completeOutput.Key,
			ETag:         completeOutput.ETag,
			Size:         &finalSize,
			LastModified: &now,
		},
	}
	return complete, nil

}

func processSmallFiles(ctx context.Context, client *s3.Client, objectList []*S3Obj, headList []*s3.HeadObjectOutput, dstKey string, opts *S3TarS3Options) (*S3Obj, error) {

	Debugf(ctx, "processSmallFiles path")

	indexList, totalSize := createGroups(ctx, objectList)
	eofPadding := generateLastBlock(totalSize, opts)
	objectList = append(objectList, eofPadding)
	headList = append(headList, nil)
	indexList[len(indexList)-1].End = len(objectList) - 1

	g := new(errgroup.Group)
	g.SetLimit(opts.Threads)
	groups := make([]*S3Obj, len(indexList))

	Debugf(ctx, "Created %d parts", len(indexList))
	for i, p := range indexList {
		i, p := i, p
		start := p.Start
		end := p.End
		Debugf(ctx, "Part %06d range: %d - %d", i+1, p.Start, p.End)
		g.Go(func() error {
			newPart, err := _processSmallFiles(ctx, objectList, headList, start, end, opts)
			if err != nil {
				return err
			}
			newPart.PartNum = start
			groups[i] = newPart
			return nil
		})
	}

	Debugf(ctx, "Waiting for threads")
	//swg.Wait()
	if err := g.Wait(); err != nil {
		return nil, err
	}
	sort.Sort(byPartNum(groups))

	// reset partNum counts.
	// Figure out if the final concat needs to be recursive
	recursiveConcat := false
	for x := 0; x < len(groups)-1; x++ { //ignore last piece
		groups[x].PartNum = x + 1
		// Debugf(ctx,"Group %05d - Size: %d", x, groups[x].Size/1024/1024)
		if *groups[x].Size < int64(fileSizeMin) {
			recursiveConcat = true
		}
	}
	groups[len(groups)-1].PartNum = len(groups) // setup the last PartNum since we skipped it

	finalObject := NewS3Obj()
	if recursiveConcat {
		padObject := &S3Obj{
			Object: types.Object{
				Key:  aws.String("pad_file"),
				Size: aws.Int64(int64(len(pad))),
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
			Debugf(ctx, "Concat(%s,%s)", *pair[0].Key, *pair[1].Key)
			finalObject, err = concatObjects(ctx, client, trim, pair, opts.DstBucket, opts.DstKey)
			if err != nil {
				fmt.Print(err.Error())
				return NewS3Obj(), err
			}
		}
	} else {
		var err error
		finalObject, err = concatObjects(ctx, client, 0, groups, opts.DstBucket, opts.DstKey)
		if err != nil {
			Debugf(ctx, "error recursion on final\n%s", err.Error())
			return NewS3Obj(), err
		}
	}

	return redistribute(ctx, client, finalObject, 0, opts.DstBucket, opts.DstKey, opts.storageClass, opts.ObjectTags)

}

// _processSmallFiles processes a range of small files from the given objectList and headList.
// It generates tar headers for each file and concatenates them into a finalPart.
// If a file does not require a tar header, it is appended directly to the parts list.
// The headList is either the results of S3 HEAD requests or nil.
//
//	if present, the head is used to set POSIX file permissions, owner and group.
//
// The generated parts are then concatenated using the rc.ConcatObjects function.
// The resulting finalPart is returned along with any error encountered during the process.
//
// Parameters:
//   - ctx: The context.Context for the operation.
//   - objectList: A slice of S3Obj representing the list of objects to process.
//   - headList: A slice of s3.HeadObjectOutput or nil, used to set permissions, uid and gid
//   - start: The starting index of the range of files to process.
//   - end: The ending index of the range of files to process.
//   - opts: A pointer to S3TarS3Options containing the options for S3 operations.
//
// Returns:
//   - *S3Obj: The final concatenated part.
//   - error: Any error encountered during the process.
func _processSmallFiles(ctx context.Context, objectList []*S3Obj, headList []*s3.HeadObjectOutput, start, end int, opts *S3TarS3Options) (*S3Obj, error) {
	parentPartsKey := filepath.Join(opts.DstPrefix, opts.DstKey+".parts")
	parts := []*S3Obj{}
	for i, partNum := start, 0; i <= end; i, partNum = i+1, partNum+1 {
		Debugf(ctx, "Processing: %s", *objectList[i].Key)
		// some objects my not need a tar header generated (like the last piece)
		if objectList[i].NoHeaderRequired {
			parts = append(parts, objectList[i])
		} else {
			prev := NewS3Obj()
			if (i - 1) >= 0 {
				prev = objectList[i-1]
			}
			header := buildHeader(objectList[i], prev, false, headList[i])
			header.Bucket = opts.DstBucket
			pairs := []*S3Obj{&header, {
				Object:  objectList[i].Object, // fix this
				Bucket:  objectList[i].Bucket,
				Data:    objectList[i].Data,
				PartNum: partNum,
			}}
			parts = append(parts, pairs...)
		}

	}

	batchName := fmt.Sprintf("%d-%d", start, end)
	dstKey := filepath.Join(parentPartsKey, strings.Join([]string{"iteration", "batch", batchName}, "."))
	finalPart, err := rc.ConcatObjects(ctx, parts, opts.DstBucket, dstKey)
	if err != nil {
		Debugf(ctx, "%s", dstKey)
		Debugf(ctx, "error recursion on final\n%s", err.Error())
		return NewS3Obj(), err
	}

	return finalPart, nil
}

// findMinimumPartSize is for the case when we want to optimize as many parts
// as possible. This is helpful to parallelize the workload even more.
// findMinimumPartSize will start at 5MB and increment by 5MB until we're
// within the 10,000 MPU part limit
func findMinimumPartSize(finalSizeBytes, userMaxSize int64) int64 {

	const fiveMB = beginningPad
	partSize := int64(fiveMB)

	if userMaxSize > 0 {
		partSize = userMaxSize * 1024 * 1024
	}

	for ; partSize <= partSizeMax; partSize = partSize + fiveMB {
		if finalSizeBytes/int64(partSize) < maxPartNumLimit {
			break
		}
	}

	if partSize > partSizeMax {
		log.Fatal("part size maximum cannot exceed 5GiB")
	}

	return partSize
}

// estimateFinalSize takes the total of all object
// then multiplies the number of objects by the header size
// then multiplies 512 by every object (the padding -- worst case scenario)
func estimateFinalSize(objectList []*S3Obj) int64 {
	headerSize := paxTarHeaderSize
	if tarFormat == tar.FormatGNU {
		headerSize = gnuTarHeaderSize
	}
	estimatedSize := int64(0)
	for _, o := range objectList {
		estimatedSize += *o.Size + int64(headerSize+blockSize)
	}
	return estimatedSize
}

func createGroups(ctx context.Context, objectList []*S3Obj) ([]Index, int64) {

	// Walk through all the parts and build groups of 500MB
	// so we can parallelize.
	indexList := []Index{}
	last := 0

	estimatedSize := estimateFinalSize(objectList)
	partSize := findMinimumPartSize(estimatedSize, 0)
	Infof(ctx, "estimated final size: %d bytes (with headers + padding)\nmultipart part-size: %d bytes\n", estimatedSize, partSize)

	// passing nil for head, header is only used to estimate size, so permissions are not needed
	h := buildHeader(objectList[0], nil, false, nil)
	currSize := *h.Size + *objectList[0].Size
	var totalSize int64 = currSize
	for i := 1; i < len(objectList); i++ {
		var prev *S3Obj
		if (i - 1) >= 0 {
			prev = objectList[i-1]
		}
		// passing nil for head, header is only used to estimate size, so permissions are not needed
		header := buildHeader(objectList[i], prev, false, nil)
		l := int64(len(header.Data)) + *objectList[i].Size
		currSize += l
		totalSize += l
		if currSize > partSize {
			indexList = append(indexList, Index{Start: last, End: i, Size: int(currSize)})
			last, currSize = i+1, 0
		}
	}

	if len(indexList) == 0 {
		indexList = []Index{
			{
				Start: 0,
				End:   len(objectList) - 1,
				Size:  int(totalSize),
			},
		}
	}

	// Make the last part include everything till the end.
	// We don't want something that is less than 5MB
	indexList[len(indexList)-1].End = len(objectList) - 1
	indexList[len(indexList)-1].Size = indexList[len(indexList)-1].Size + int(currSize)
	return indexList, totalSize
}

func concatObjects(ctx context.Context, client *s3.Client, trimFirstBytes int, objectList []*S3Obj, bucket, key string) (*S3Obj, error) {
	complete := NewS3Obj()
	output, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
		ACL:    types.ObjectCannedACLBucketOwnerFullControl,
	})
	if err != nil {
		return complete, err
	}
	var accumSize int64 = 0
	uploadId := *output.UploadId
	var parts []types.CompletedPart
	m := sync.RWMutex{}
	swg := sizedwaitgroup.New(threads)
	for i, object := range objectList {
		partNum := int32(i + 1)
		if len(object.Data) > 0 {
			accumSize += int64(len(object.Data))
			input := &s3.UploadPartInput{
				Bucket:     &bucket,
				Key:        &key,
				PartNumber: &partNum,
				UploadId:   &uploadId,
				Body:       io.ReadSeeker(bytes.NewReader(object.Data)),
			}
			swg.Add()
			go func(input *s3.UploadPartInput) {
				defer swg.Done()
				Debugf(ctx, "UploadPart (bytes) into: %s/%s", *input.Bucket, *input.Key)
				r, err := client.UploadPart(ctx, input)
				if err != nil {
					Debugf(ctx, "error for s3://%s/%s", *input.Bucket, *input.Key)
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
				copySourceRange = fmt.Sprintf("bytes=%d-%d", trimFirstBytes, *object.Size-1)
				accumSize += *object.Size - int64(trimFirstBytes)
			} else {
				copySourceRange = fmt.Sprintf("bytes=0-%d", *object.Size-1)
				accumSize += *object.Size
			}
			sourceKey := object.Bucket + "/" + *object.Key
			input := s3.UploadPartCopyInput{
				Bucket:          &bucket,
				Key:             &key,
				PartNumber:      &partNum,
				UploadId:        &uploadId,
				CopySource:      aws.String(sourceKey),
				CopySourceRange: aws.String(copySourceRange),
			}
			swg.Add()
			go func(input s3.UploadPartCopyInput) {
				defer swg.Done()
				Debugf(ctx, "UploadPartCopy (s3://%s/%s) into:\n\ts3://%s/%s", *input.Bucket, *input.Key, bucket, key)
				r, err := client.UploadPartCopy(ctx, &input)
				if err != nil {
					Debugf(ctx, "error for s3://%s/%s", *input.Bucket, *input.Key)
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
		return *parts[i].PartNumber < *parts[j].PartNumber
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
	now := time.Now()
	complete = &S3Obj{
		Bucket: *completeOutput.Bucket,
		Object: types.Object{
			Key:          completeOutput.Key,
			ETag:         completeOutput.ETag,
			Size:         &accumSize,
			LastModified: &now,
		},
	}
	return complete, nil
}
