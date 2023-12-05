package s3tar

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
)

func buildInMemoryConcat(ctx context.Context, svc *s3.Client, objectList []*S3Obj, opts *S3TarS3Options) (*S3Obj, error) {
	log.Fatal("not implemented")
	return nil, nil
}
