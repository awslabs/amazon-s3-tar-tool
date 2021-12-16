package s3tar

import (
	"bytes"
	"context"
	"log"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	extractS3    = regexp.MustCompile(`s3:\/\/(.[^\/]*)\/(.*)`)
	blockSize    = int64(512)
	beginningPad = 5 * 1024 * 1024
)

type SSTarS3Options struct {
	SrcBucket    string
	SrcPrefix    string
	DstBucket    string
	DstPrefix    string
	DstKey       string
	Threads      uint
	DeleteSource bool
}

type PartsMessage struct {
	Parts   []S3Obj
	PartNum int
}

type S3Obj struct {
	Key     string
	Bucket  string
	Etag    string
	Size    int64
	PartNum int
	Data    []byte
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

func deleteAllMultiparts(client *s3.Client, bucket string) error {
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
