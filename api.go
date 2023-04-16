package s3tar

import (
	"archive/tar"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"strings"
)

type Archiver interface {
	Create(context.Context, *S3TarS3Options, ...func(options *S3TarS3Options)) error
	Extract(context.Context, *S3TarS3Options, ...func(options *S3TarS3Options)) error
	List(context.Context, string, *S3TarS3Options, ...func(options *S3TarS3Options)) (TOC, error)
}

func NewArchiveClient(client *s3.Client) Archiver {
	return &ArchiveClient{client}
}

type ArchiveClient struct {
	client *s3.Client
}

// Create an archive from existing files in Amazon S3.
func (a *ArchiveClient) Create(ctx context.Context, options *S3TarS3Options, optFns ...func(options *S3TarS3Options)) error {
	opts := options.Copy()

	if err := checkCreateArgs(&opts); err != nil {
		return err
	}

	for _, fn := range optFns {
		fn(&opts)
	}

	if err := validateStorageClass(&opts); err != nil {
		return err
	}

	ServerSideTar(ctx, a.client, &opts)

	return nil
}

func (a *ArchiveClient) Extract(ctx context.Context, options *S3TarS3Options, optFns ...func(options *S3TarS3Options)) error {
	opts := options.Copy()

	if err := checkExtractArgs(&opts); err != nil {
		return err
	}

	for _, fn := range optFns {
		fn(&opts)
	}

	return Extract(ctx, a.client, opts.extractPrefix, &opts)
}

func (a *ArchiveClient) List(ctx context.Context, archiveS3Url string, options *S3TarS3Options, optFns ...func(options *S3TarS3Options)) (TOC, error) {
	opts := options.Copy()

	opts.SrcBucket, opts.SrcKey = ExtractBucketAndPath(archiveS3Url)

	if err := checkListArgs(&opts); err != nil {
		return TOC{}, err
	}

	for _, fn := range optFns {
		fn(&opts)
	}

	return List(ctx, a.client, opts.SrcBucket, opts.SrcKey, &opts)
}

func WithStorageClass(sc string) func(*S3TarS3Options) {
	return func(opts *S3TarS3Options) {
		c := strings.ToUpper(sc)
		opts.storageClass = types.StorageClass(c)
	}
}

func WithExtractPrefix(prefix string) func(*S3TarS3Options) {
	return func(opts *S3TarS3Options) {
		opts.extractPrefix = prefix
	}
}

func validateStorageClass(opts *S3TarS3Options) error {
	if !containsClass(string(opts.storageClass)) {
		return fmt.Errorf("storage class not valid")
	}
	return nil
}

func containsClass(val string) bool {
	for _, v := range types.StorageClassStandard.Values() {
		if string(v) == val {
			return true
		}
	}
	return false
}

func WithTarFormat(format string) func(options *S3TarS3Options) {
	return func(opts *S3TarS3Options) {
		switch format {
		case "", "pax":
			opts.tarFormat = tar.FormatPAX
		case "gnu":
			opts.tarFormat = tar.FormatGNU
		default:
			Fatalf(context.TODO(), "tar format not supported")
		}
	}
}

func checkCreateArgs(opts *S3TarS3Options) error {
	if opts.SrcBucket == "" && opts.SrcManifest == "" {
		return fmt.Errorf("src bucket or src manifest required")
	}
	if opts.DstBucket == "" {
		return fmt.Errorf("destination bucket required")
	}
	if opts.DstKey == "" {
		return fmt.Errorf("destination key required")
	}
	if opts.storageClass == "" {
		opts.storageClass = types.StorageClassStandard
	}
	opts.tarFormat = tar.FormatPAX
	return nil
}
func checkExtractArgs(opts *S3TarS3Options) error {
	if opts.SrcBucket == "" && opts.SrcManifest == "" {
		return fmt.Errorf("src bucket or src manifest required")
	}
	if opts.DstBucket == "" {
		return fmt.Errorf("destination bucket required")
	}
	if opts.DstPrefix == "" {
		return fmt.Errorf("destination prefix required")
	}
	return nil
}
func checkListArgs(opts *S3TarS3Options) error {
	if opts.SrcBucket == "" && opts.SrcKey == "" {
		return fmt.Errorf("s3url required s3://bucket/key.tar")
	}
	return nil
}
