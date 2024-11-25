// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type contextKey string

const (
	contextKeyS3Client = contextKey("s3-client")
)

var (
	extractS3 = regexp.MustCompile(`s3://(.[^/]*)/?(.*)`)
)

// S3TarS3Options options to create an archive
type S3TarS3Options struct {
	SrcManifest           string
	SkipManifestHeader    bool
	SrcBucket             string
	SrcPrefix             string
	SrcKey                string
	DstBucket             string
	DstPrefix             string
	DstKey                string
	Threads               int
	DeleteSource          bool
	ExternalToc           string
	tarFormat             tar.Format
	storageClass          types.StorageClass
	extractPrefix         string
	ConcatInMemory        bool
	UrlDecode             bool
	UserMaxPartSize       int64
	ObjectTags            types.Tagging
	KMSKeyID              string
	SSEAlgo               types.ServerSideEncryption
	PreservePOSIXMetadata bool
}

func TagsToUrlEncodedString(tagging types.Tagging) string {

	vals := url.Values{}
	for _, x := range tagging.TagSet {
		vals.Add(*x.Key, *x.Value)
	}
	return vals.Encode()

}

func (o *S3TarS3Options) Copy() S3TarS3Options {
	to := *o
	return to
}

func findMinMaxPartRange(objectSize int64) (int64, int64, int64) {
	const (
		KB          int64 = 1024
		partsLimit  int64 = 10000
		partSizeMin int64 = KB * KB * 5
		partSizeMax int64 = KB * KB * KB * 5
		// optimalSize = 1024 * 1024 * 16
	)

	// partSizeMin = 1000 * 1000 * 5
	// partSizeMax = 5e+9
	// curSize = 5e+12 #5TB

	curSize := objectSize
	nPartsMax := partsLimit
	var nPartsMaxSize int64 = 0
	for {
		nPartsMaxSize = curSize / nPartsMax
		if nPartsMaxSize < partSizeMin {
			nPartsMax = nPartsMax - 1
			continue
		}
		break
	}

	var nPartsMin int64 = 1
	var nPartsMinSize int64 = 0
	for {
		nPartsMinSize = curSize / nPartsMin
		if nPartsMinSize > partSizeMax {
			nPartsMin += 1
			continue
		}
		break
	}
	mid := nPartsMax / 2
	return nPartsMin, nPartsMax, mid
}

type PartsMessage struct {
	Parts   []*S3Obj
	PartNum int
}

func NewS3Obj() *S3Obj {
	now := time.Now()
	return &S3Obj{
		Object: types.Object{
			Key:          aws.String(""),
			ETag:         aws.String(""),
			LastModified: &now,
		},
	}
}

func NewS3ObjOptions(options ...func(*S3Obj)) *S3Obj {
	now := time.Now()
	obj := &S3Obj{
		Object: types.Object{
			Key:          aws.String(""),
			ETag:         aws.String(""),
			LastModified: &now,
		},
	}
	for _, o := range options {
		o(obj)
	}
	return obj
}

func WithBucketAndKey(bucket, key string) func(*S3Obj) {
	return func(o *S3Obj) {
		o.Bucket = bucket
		o.Key = &key
	}
}
func WithSize(size int64) func(*S3Obj) {
	return func(o *S3Obj) {
		o.Size = &size
	}
}
func WithETag(etag string) func(*S3Obj) {
	return func(o *S3Obj) {
		o.ETag = &etag
	}
}

func NewS3ObjFromObject(o types.Object) *S3Obj {
	return &S3Obj{Object: o}
}

func StringToInt64(s string) (int64, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return i, nil
}

type S3Obj struct {
	types.Object
	Bucket           string
	PartNum          int
	Data             []byte
	NoHeaderRequired bool
}

func (s *S3Obj) AddData(data []byte) {
	etag := fmt.Sprintf("%x", md5.Sum(data))
	s.Data = data
	s.Size = aws.Int64(int64(len(data)))
	s.ETag = &etag
}

type VirtualArchive []*S3Obj

type byPartNum []*S3Obj

func (a byPartNum) Len() int           { return len(a) }
func (a byPartNum) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byPartNum) Less(i, j int) bool { return a[i].PartNum < a[j].PartNum }

type Index struct {
	Start int
	End   int
	Size  int
}

func findPadding(offset int64) (n int64) {
	return -offset & (blockSize - 1)
}

type Logger struct {
	Level int
}

// ExtractBucketAndPath helper function to extract bucket and key from s3://bucket/prefix/key URLs
func ExtractBucketAndPath(s3url string) (bucket string, path string) {
	parts := extractS3.FindAllStringSubmatch(s3url, -1)
	if len(parts) > 0 && len(parts[0]) > 2 {
		bucket = parts[0][1]
		path = parts[0][2]
	}
	return
}

func filter[T any](ss []T, test func(T) bool) (ret []T) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

func removeDirs(object types.Object) bool {
	name := *object.Key
	if string(name[len(name)-1]) == "/" {
		return false
	}
	return true
}

func ListAllObjects(ctx context.Context, client *s3.Client, Bucket, Prefix string, filterFns ...func(types.Object) bool) ([]*S3Obj, int64, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: &Bucket,
		Prefix: &Prefix,
	}
	var accum int64

	ctr := 1
	var list []*S3Obj
	var defaultFilter []func(types.Object) bool
	defaultFilter = append(defaultFilter, removeDirs)
	allFilters := append(defaultFilter, filterFns...)

	p := s3.NewListObjectsV2Paginator(client, input)
	for {
		if !p.HasMorePages() {
			break
		}
		output, err := p.NextPage(ctx)
		if err != nil {
			log.Print(err.Error())
			return list, accum, err
		}
		contents := output.Contents
		if len(allFilters) > 0 {
			for _, tf := range allFilters {
				contents = filter(contents, tf)
			}
		}
		for _, o := range contents {
			list = append(list, &S3Obj{
				Object:  o,
				Bucket:  Bucket,
				PartNum: ctr,
			})
			ctr += 1
			accum += estimateObjectSize(*o.Size)
		}
	}

	return list, accum, nil
}

// estimate the object size including header and padding
func estimateObjectSize(size int64) int64 {
	pad := findPadding(size)
	return int64(paxTarHeaderSize) + size + pad
}

func BreakUpList(objectList []*S3Obj, limitSize int64) [][]*S3Obj {

	var list [][]*S3Obj
	var currentList []*S3Obj
	var accum int64 = 0
	for i := 0; i < len(objectList); i++ {
		currentObjectSize := estimateObjectSize(*objectList[i].Size)
		if accum+currentObjectSize < limitSize {
			currentList = append(currentList, objectList[i])
			accum += currentObjectSize
		} else {
			list = append(list, currentList)
			currentList = append([]*S3Obj{}, objectList[i])
			accum = currentObjectSize
		}
	}
	if len(currentList) > 0 {
		list = append(list, currentList)
	}
	return list
}

func putObject(ctx context.Context, svc *s3.Client, bucket, key string, data []byte) (*s3.PutObjectOutput, error) {
	input := &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}
	return svc.PutObject(ctx, input)
}

func getObject(ctx context.Context, svc *s3.Client, bucket, key string) (io.ReadCloser, error) {
	return getObjectRange(ctx, svc, bucket, key, 0, 0)
}
func getObjectRange(ctx context.Context, svc *s3.Client, bucket, key string, start, end int64) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Key:    &key,
		Bucket: &bucket,
	}
	if end != 0 {
		byteRange := fmt.Sprintf("bytes=%d-%d", start, end)
		params.Range = &byteRange
	}
	output, err := svc.GetObject(ctx, params)
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func loadFile(ctx context.Context, svc *s3.Client, path string) (io.ReadCloser, error) {
	if strings.Contains(path, "s3://") {
		bucket, key := ExtractBucketAndPath(path)
		return getObject(ctx, svc, bucket, key)
	} else {
		return os.Open(path)
	}
}

// DeleteAllMultiparts helper function to clear ALL MultipartUploads in a bucket. This will delete all incomplete (or in progress) MPUs for a bucket.
func DeleteAllMultiparts(client *s3.Client, bucket string) error {
	output, err := client.ListMultipartUploads(context.TODO(), &s3.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil {
		return err
	}
	for _, upload := range output.Uploads {
		log.Printf("Aborting %s", *upload.UploadId)
		_, err := client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
		if err != nil {
			Infof(context.Background(), err.Error())
			return err
		}
		// log.Printf("AbortedMultiUpload ok %s", r)
	}
	return nil
}

func _deleteObjectList(ctx context.Context, client *s3.Client, opts *S3TarS3Options, objectList []*S3Obj) error {
	objects := make([]types.ObjectIdentifier, len(objectList))
	for i := 0; i < len(objectList); i++ {
		objects[i] = types.ObjectIdentifier{
			Key: objectList[i].Key,
		}
	}
	params := &s3.DeleteObjectsInput{
		Bucket: &objectList[0].Bucket,
		Delete: &types.Delete{
			Quiet:   aws.Bool(true),
			Objects: objects,
		},
	}
	response, err := client.DeleteObjects(ctx, params)
	if err != nil {
		return err
	}
	if len(response.Errors) > 0 {
		Infof(ctx, "Error deleting objects")
		return err
	}
	return nil

}

func deleteObjectList(ctx context.Context, svc *s3.Client, opts *S3TarS3Options, objectList []*S3Obj) error {
	batch := 1000
	for i := 0; i < len(objectList); i += batch {
		start := i
		end := i + batch
		if end >= len(objectList) {
			end = len(objectList)
		}
		part := objectList[start:end]
		err := _deleteObjectList(ctx, svc, opts, part)
		if err != nil {
			return err
		}
	}
	return nil
}

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func formatBytes(contentLength int64) string {
	if contentLength < 0 {
		return "Invalid size"
	}

	// Define SI unit symbols
	units := []string{"Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

	// Convert to float64 for calculation
	size := float64(contentLength)

	// Determine the appropriate unit
	unitIndex := 0
	for size >= 1024 && unitIndex < len(units)-1 {
		size /= 1024
		unitIndex++
	}

	// Format the result with two decimal places
	msg := fmt.Sprintf("%.4f %s", size, units[unitIndex])
	if units[unitIndex] == "Bytes" {
		msg = fmt.Sprintf("%.0f %s", size, units[unitIndex])
	}
	return msg
}
