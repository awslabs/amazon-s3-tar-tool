package s3tar

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	blockSize    = int64(512)
	beginningPad = 5 * 1024 * 1024
	fileSizeMin  = beginningPad
)

var (
	extractS3  = regexp.MustCompile(`s3:\/\/(.[^\/]*)\/(.*)`)
	startBlock S3Obj
)

type SSTarS3Options struct {
	SrcBucket    string
	SrcPrefix    string
	DstBucket    string
	DstPrefix    string
	DstKey       string
	Threads      uint
	DeleteSource bool
	SmallFiles   bool
}

type PartsMessage struct {
	Parts   []S3Obj
	PartNum int
}

type S3Obj struct {
	types.Object
	Bucket  string
	PartNum int
	Data    []byte
}

type Index struct {
	Start int
	End   int
	Size  int
}

func findPadding(offset int64) (n int64) {
	return -offset & (blockSize - 1)
}

func ExtractBucketAndPath(s3url string) (bucket string, path string) {
	parts := extractS3.FindAllStringSubmatch(s3url, -1)
	if len(parts) > 0 && len(parts[0]) > 2 {
		bucket = parts[0][1]
		path = parts[0][2]
	}
	return
}

func createFirstBlock(ctx context.Context, svc *s3.Client, bucket, parts string) S3Obj {
	key := filepath.Join(parts, "min-size-block")
	now := time.Now()
	output, err := putObject(ctx, svc, bucket, key, pad)
	if err != nil {
		log.Fatal(err.Error())
	}
	return S3Obj{
		Bucket: bucket,
		Object: types.Object{
			Key:          &key,
			Size:         int64(len(pad)),
			LastModified: &now,
			ETag:         output.ETag,
		},
	}
}

func listAllObjects(ctx context.Context, client *s3.Client, Bucket, Prefix string) []types.Object {
	var objectList []types.Object
	input := &s3.ListObjectsV2Input{
		Bucket: &Bucket,
		Prefix: &Prefix,
	}
	p := s3.NewListObjectsV2Paginator(client, input)
	for {
		if p.HasMorePages() == false {
			break
		}
		output, err := p.NextPage(ctx)
		if err != nil {
			log.Printf(err.Error())
			break
		}
		objectList = append(objectList, output.Contents...)
	}

	// filter paths out
	cleanList := make([]types.Object, len(objectList))
	ctr := 0
	for _, obj := range objectList {
		name := *obj.Key
		if string(name[len(name)-1]) == "/" {
			continue
		}
		cleanList[ctr] = obj
		ctr++
	}

	return cleanList[0:ctr]
}

func putObject(ctx context.Context, svc *s3.Client, bucket, key string, data []byte) (*s3.PutObjectOutput, error) {
	input := &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: int64(len(data)),
	}
	return svc.PutObject(ctx, input)
}

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
			log.Fatalf(err.Error())
		}
		// log.Printf("AbortedMultiUpload ok %s", r)
	}
	return nil
}

func GetS3Client(ctx context.Context) *s3.Client {
	client, _ := ctx.Value(contextKeyS3Client).(*s3.Client)
	return client
}

func _deleteObjectList(ctx context.Context, opts *SSTarS3Options, objectList []S3Obj) error {
	client := GetS3Client(ctx)
	objects := make([]types.ObjectIdentifier, len(objectList))
	for i := 0; i < len(objectList); i++ {
		objects[i] = types.ObjectIdentifier{
			Key: objectList[i].Key,
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

func deleteS3ObjectList(ctx context.Context, opts *SSTarS3Options, bucket string, objectList []types.Object) error {
	objects := []S3Obj{}
	for _, x := range objectList {
		objects = append(objects, S3Obj{
			Bucket: bucket,
			Object: types.Object{
				Key: x.Key,
			},
		})
	}
	return deleteObjectList(ctx, opts, objects)
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
