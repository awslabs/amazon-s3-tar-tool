// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	s3tar "github.com/awslabs/amazon-s3-tar-tool"
	"github.com/urfave/cli/v2"
)

var (
	Version          = "0.0.0"
	Commit           = ""
	VersionMsg       = fmt.Sprintf("%s-%s", Version, Commit)
	newArchiveClient = s3tar.NewArchiveClient
	listAllObjects   = s3tar.ListAllObjects
	loadCSV          = s3tar.LoadCSV
)

const (
	maxSize = 1024 * 1024 * 1024 * 1024 * 5
)

func main() {
	err := run(os.Args)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func run(args []string) error {
	ctx := s3tar.SetupLogger(context.Background())
	var create bool
	var extract bool
	var list bool
	var generateToc bool
	var generateManifest bool
	var region string
	var endpointUrl string
	var archiveFile string // file flag
	var destination string
	var threads int
	var skipManifestHeader bool
	var manifestPath string
	var tarFormat string
	var extended bool
	var externalToc string
	var storageClass string
	var sizeLimit int64
	var maxAttempts int
	var concatInMemory bool
	var urlDecode bool
	var userPartMaxSize int64
	var awsProfile string
	var tagSetInput string
	var kmsKeyID string
	var sseAlgo string
	var preservePosixMetadata bool

	var tagSet types.Tagging
	var err error

	cli.VersionFlag = &cli.BoolFlag{
		Name:    "print-version",
		Aliases: []string{"V"},
		Usage:   "show version:",
	}
	app := &cli.App{
		UseShortOptionHandling: true,
		Authors: []*cli.Author{
			&cli.Author{
				Name:  "Yanko Bolanos",
				Email: "bolyanko@amazon.com",
			},
		},
		Version:     VersionMsg,
		UsageText:   "s3tar --region us-west-2 [--endpointUrl s3.us-west-2.amazonaws.com] [-c --create] | [-x --extract] [-v] -f s3://bucket/prefix/file.tar s3://bucket/prefix",
		Copyright:   "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.",
		Description: "s3tar helps aggregates existing Amazon S3 objects without the need to download files",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "create",
				Value:       false,
				Usage:       "create an archive",
				Aliases:     []string{"c"},
				Destination: &create,
			},
			&cli.BoolFlag{
				Name:        "extract",
				Value:       false,
				Usage:       "extract an archive",
				Aliases:     []string{"x"},
				Destination: &extract,
			},
			&cli.BoolFlag{
				Name:        "list",
				Value:       false,
				Usage:       "print out the contents in the archive",
				Aliases:     []string{"t"},
				Destination: &list,
			},
			&cli.BoolFlag{
				Name:        "generate-toc",
				Value:       false,
				Usage:       "command to generate a toc.csv for an existing tarball",
				Destination: &generateToc,
			},
			&cli.BoolFlag{
				Name:        "generate-manifest",
				Value:       false,
				Usage:       "lists objects in an S3 Path and generates a for creating an archive later",
				Destination: &generateManifest,
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Value:   false,
				Usage:   "verbose level v, vv, vvv",
				Aliases: []string{"v"},
			},
			&cli.StringFlag{
				Name:        "region",
				Value:       "",
				Usage:       "specify region",
				Destination: &region,
			},
			&cli.StringFlag{
				Name:        "endpointUrl",
				Value:       "",
				Usage:       "specify endpointUrl",
				Destination: &endpointUrl,
			},
			&cli.StringFlag{
				Name:        "file",
				Value:       "",
				Usage:       "file",
				Aliases:     []string{"f"},
				Destination: &archiveFile,
			},
			&cli.StringFlag{
				Name:        "location",
				Value:       "",
				Usage:       "destination to extract | destination of TOC (must be local)",
				Aliases:     []string{"C"},
				Destination: &destination,
			},
			&cli.IntFlag{
				Name:        "goroutines",
				Value:       100,
				Usage:       "number of goroutines",
				Destination: &threads,
			},
			&cli.BoolFlag{
				Name:        "skipManifestHeader",
				Value:       false,
				Usage:       "skip the first line of the manifest",
				Destination: &skipManifestHeader,
			},
			&cli.StringFlag{
				Name:        "manifest",
				Value:       "",
				Usage:       "manifest file with bucket,key per line to process",
				Destination: &manifestPath,
				Aliases:     []string{"m"},
			},
			&cli.StringFlag{
				Name:        "format",
				Value:       "pax",
				Usage:       "tar format can be either pax or gnu",
				Destination: &tarFormat,
			},
			&cli.BoolFlag{
				Name:        "extended",
				Value:       false,
				Usage:       "--extended prints out manifest with: name,byte location,content-length,Etag",
				Destination: &extended,
			},
			&cli.StringFlag{
				Name:        "external-toc",
				Value:       "",
				Usage:       "specifies an external toc for files not containing one",
				Destination: &externalToc,
			},
			&cli.StringFlag{
				Name:        "storage-class",
				Value:       "STANDARD",
				Usage:       "storage class of the object",
				Destination: &storageClass,
			},
			&cli.Int64Flag{
				Name:        "size-limit",
				Value:       maxSize,
				Usage:       "limit the size of tars and break them into several parts (byte units). default 5TB",
				Destination: &sizeLimit,
			},
			&cli.IntFlag{
				Name:        "max-attempts",
				Value:       10,
				Usage:       "number of maxAttempts for AWS Go SDK. 0 is unlimited",
				Destination: &maxAttempts,
			},
			&cli.BoolFlag{
				Name:        "concat-in-memory",
				Value:       false,
				Usage:       "create the tar object in ram; to use with small files and concatenate the part",
				Destination: &concatInMemory,
			},
			&cli.BoolFlag{
				Name:        "urldecode",
				Value:       false,
				Usage:       "url decode the key value from the manifest",
				Destination: &urlDecode,
			},
			&cli.Int64Flag{
				Name:        "max-part-size",
				Value:       0,
				Usage:       "constrain the max part size of MPU, in MB",
				Destination: &userPartMaxSize,
			},
			&cli.StringFlag{
				Name:        "profile",
				Value:       "",
				Usage:       "",
				Destination: &awsProfile,
			},
			&cli.StringFlag{
				Name:        "tagging",
				Usage:       "pass a tag value following awscli syntax: --tagging='{\"TagSet\": [{ \"Key\": \"transition-to\", \"Value\": \"GDA\" }]}'",
				Destination: &tagSetInput,
			},
			&cli.StringFlag{
				Name:        "sse-kms-key-id",
				Usage:       "",
				Destination: &kmsKeyID,
			},
			&cli.StringFlag{
				Name:        "sse-algo",
				Usage:       "aws:kms or AES256",
				Destination: &sseAlgo,
			},
			&cli.BoolFlag{
				Name:        "preserve-posix-metadata",
				Usage:       "Preserve POSIX permisions, uid and gid if present in S3 object metadata. See https://docs.aws.amazon.com/fsx/latest/LustreGuide/posix-metadata-support.html",
				Destination: &preservePosixMetadata,
			},
		},
		Action: func(cCtx *cli.Context) error {
			logLevel := parseLogLevel(cCtx.Count("verbose"))
			if region == "" && !generateToc {
				exitError(1, "region is missing\n")
			}
			if archiveFile == "" {
				exitError(2, "-f is a required flag\n")
			}
			if sizeLimit > maxSize {
				sizeLimit = maxSize
			}

			if tagSetInput != "" {
				tagSet, err = parseTagValues(tagSetInput)
				if err != nil {
					exitError(10, "invalid format for tags")
				}
			}

			var loadOption config.LoadOptionsFunc
			if endpointUrl != "" {
				loadOption = config.WithEndpointResolverWithOptions(
					aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
						return aws.Endpoint{
							URL:               endpointUrl,
							HostnameImmutable: true,
							SigningRegion:     region,
							Source:            aws.EndpointSourceCustom,
						}, nil
					}))
			} else {
				loadOption = config.WithRegion(region)
			}

			retryOption := config.WithRetryer(func() aws.Retryer {
				return retry.AddWithMaxAttempts(retry.NewStandard(), maxAttempts)
			})

			optFns := []func(*config.LoadOptions) error{
				loadOption,
				retryOption,
			}
			if awsProfile != "" {
				optFns = append(optFns, config.WithSharedConfigProfile(awsProfile))
			}

			svc := s3Client(ctx, optFns...)

			if create {
				src := cCtx.Args().First() // TODO implement dir list

				if userPartMaxSize > 0 && (userPartMaxSize < 5 || userPartMaxSize > 5000) {
					exitError(6, "max-part-size should be >= 5 and < 5000")
				}

				s3opts := &s3tar.S3TarS3Options{
					SrcManifest:           manifestPath,
					SkipManifestHeader:    skipManifestHeader,
					Threads:               threads,
					DeleteSource:          false,
					ConcatInMemory:        concatInMemory,
					UrlDecode:             urlDecode,
					UserMaxPartSize:       userPartMaxSize,
					ObjectTags:            tagSet,
					PreservePOSIXMetadata: preservePosixMetadata,
				}
				s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(archiveFile)
				s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
				s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(src)
				if s3opts.SrcBucket == "" && manifestPath == "" {
					exitError(4, "source directory or manifest file is required.\n")
				}

				ctx = s3tar.SetLogLevel(ctx, logLevel)
				archiveClient := newArchiveClient(svc)

				var objectList []*s3tar.S3Obj
				var estimatedSize int64
				var err error
				if s3opts.SrcManifest != "" {
					objectList, estimatedSize, err = loadCSV(ctx, svc, s3opts.SrcManifest, s3opts.SkipManifestHeader, s3opts.UrlDecode)
				} else {
					objectList, estimatedSize, err = listAllObjects(ctx, svc, s3opts.SrcBucket, s3opts.SrcPrefix)
				}
				if err != nil {
					return err
				}

				s3tar.Infof(ctx, "estimated tar size: %d", estimatedSize)
				if estimatedSize > sizeLimit {
					archiveList := s3tar.BreakUpList(objectList, sizeLimit)
					s3tar.Infof(ctx, "breaking up tar into %d parts", len(archiveList))
					padWidth := getPadWidth(len(archiveList))
					for i, archive := range archiveList {
						fn := fmt.Sprintf("%s.%0*d.tar", archiveFile[:len(archiveFile)-4], padWidth, i)
						s3tar.Infof(ctx, "creating %s", fn)
						s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(fn)
						s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
						err := archiveClient.CreateFromList(ctx, archive, s3opts,
							s3tar.WithStorageClass(storageClass),
							s3tar.WithTarFormat(tarFormat),
							s3tar.WithKMS(kmsKeyID, sseAlgo))
						if err != nil {
							return err
						}
					}
					return nil
				} else {
					return archiveClient.CreateFromList(ctx, objectList, s3opts,
						s3tar.WithStorageClass(storageClass),
						s3tar.WithTarFormat(tarFormat),
						s3tar.WithKMS(kmsKeyID, sseAlgo))
				}

			} else if extract {

				if archiveFile == "" {
					exitError(5, "file is missing")
				}
				prefix := cCtx.Args().First()
				if destination == "" {
					log.Fatalf("destination path missing")
				}
				if destination[len(destination)-1] != '/' && !generateManifest {
					destination = destination + "/"
					fmt.Printf("appending '/' to destination path\n")
				}
				s3opts := &s3tar.S3TarS3Options{
					Threads:               threads,
					DeleteSource:          false,
					ExternalToc:           externalToc,
					PreservePOSIXMetadata: preservePosixMetadata,
				}
				s3opts.SrcBucket, s3opts.SrcKey = s3tar.ExtractBucketAndPath(archiveFile)
				s3opts.SrcPrefix = filepath.Dir(s3opts.SrcKey)
				s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(destination)
				s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
				ctx = s3tar.SetLogLevel(ctx, logLevel)
				archiveClient := newArchiveClient(svc)
				return archiveClient.Extract(ctx, s3opts, s3tar.WithExtractPrefix(prefix))
			} else if list {
				s3opts := &s3tar.S3TarS3Options{
					Threads:      threads,
					DeleteSource: false,
					ExternalToc:  externalToc,
				}
				archiveClient := newArchiveClient(svc)
				toc, err := archiveClient.List(ctx, archiveFile, s3opts)
				if err != nil {
					log.Fatal(err.Error())
				}
				for _, f := range toc {
					if extended {
						fmt.Printf("%s,%d,%d,%s\n", f.Filename, f.Start, f.Size, f.Etag)
					} else {
						fmt.Printf("%s\n", f.Filename)
					}
				}
			} else if generateToc {
				// s3tar --generate-toc -f my-previous-archive.tar -C /home/user/my-previous-archive.toc.csv
				bucket, key := s3tar.ExtractBucketAndPath(archiveFile)
				s3opts := &s3tar.S3TarS3Options{
					Threads:      threads,
					DeleteSource: false,
					SrcBucket:    bucket,
					SrcKey:       key,
				}
				err := s3tar.GenerateToc(ctx, svc, archiveFile, destination, s3opts)
				if err != nil {
					log.Fatal(err.Error())
				}
			} else if generateManifest {
				bucket, prefix := s3tar.ExtractBucketAndPath(archiveFile)

				objectList, _, err := s3tar.ListAllObjects(ctx, svc, bucket, prefix)
				if err != nil {
					log.Fatal(err.Error())
				}

				f, err := os.Create(destination)
				if err != nil {
					return err
				}
				defer f.Close()
				w := csv.NewWriter(f)
				defer w.Flush()

				for _, obj := range objectList {
					size := strconv.FormatInt(*obj.Size, 10)
					etag := *obj.ETag
					err = w.Write([]string{obj.Bucket, *obj.Key, size, etag[1 : len(etag)-1]})
					if err != nil {
						return err
					}
				}
			} else {
				exitError(3, "operation not implemented, provide create or extract flag\n")
			}
			return nil
		},
	}

	return app.Run(args)
}

func s3Client(ctx context.Context, opts ...func(*config.LoadOptions) error) *s3.Client {

	uaVersion := Version
	if uaVersion == "0.0.0" { // Version is set at compile time
		uaVersion = "dev-" + Commit
	}
	ua := func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, middleware.AddUserAgentKeyValue("s3tar", Version))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)

	if err != nil {
		log.Fatal(err.Error())
	}
	return s3.NewFromConfig(cfg, ua)

}

func parseTagValues(tagSet string) (types.Tagging, error) {
	tags := types.Tagging{}
	err := json.Unmarshal([]byte(tagSet), &tags)
	if err != nil {
		return tags, err
	}
	return tags, nil
}

func parseLogLevel(count int) int {
	verboseCount := count
	if verboseCount < 0 {
		verboseCount = 0
	}
	if verboseCount > 3 {
		verboseCount = 3
	}
	return verboseCount
}

func exitError(code int, format string, v ...any) {
	fmt.Printf(format, v...)
	os.Exit(code)
}

func getPadWidth(length int) int {
	padWidth := len(strconv.Itoa(length))
	if padWidth == 1 {
		padWidth = 2
	}
	return padWidth
}
