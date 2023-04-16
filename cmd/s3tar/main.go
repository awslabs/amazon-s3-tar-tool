// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3tar "github.com/awslabs/amazon-s3-tar-tool"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"path/filepath"
)

var (
	Version          = "0.0.0"
	Commit           = ""
	VersionMsg       = fmt.Sprintf("%s-%s", Version, Commit)
	newArchiveClient = s3tar.NewArchiveClient
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
	var region string
	var endpointUrl string
	var archiveFile string // file flag
	var destination string
	var threads uint
	var skipManifestHeader bool
	var manifestPath string
	var tarFormat string
	var extended bool
	var externalToc string
	var storageClass string

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
			&cli.UintFlag{
				Name:        "goroutines",
				Value:       20,
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
				Usage:       "tar format can be either pax or gnu. default is pax",
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
		},
		Action: func(cCtx *cli.Context) error {
			logLevel := parseLogLevel(cCtx.Count("verbose"))
			if region == "" && !generateToc {
				exitError(1, "region is missing\n")
			}
			if archiveFile == "" {
				exitError(2, "-f is a required flag\n")
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
			if create {
				src := cCtx.Args().First() // TODO implement dir list

				s3opts := &s3tar.S3TarS3Options{
					SrcManifest:        manifestPath,
					SkipManifestHeader: skipManifestHeader,
					Threads:            threads,
					DeleteSource:       false,
					Region:             region,
					EndpointUrl:        endpointUrl,
				}
				s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(archiveFile)
				s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
				s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(src)
				if s3opts.SrcBucket == "" && manifestPath == "" {
					exitError(4, "source directory or manifest file is required.\n")
				}

				ctx = s3tar.SetLogLevel(ctx, logLevel)
				svc := s3Client(ctx, loadOption)
				archiveClient := newArchiveClient(svc)
				return archiveClient.Create(ctx, s3opts,
					s3tar.WithStorageClass(storageClass),
					s3tar.WithTarFormat(tarFormat))
			} else if extract {

				if archiveFile == "" {
					exitError(5, "file is missing")
				}
				prefix := cCtx.Args().First()
				if destination == "" {
					log.Fatalf("destination path missing")
				}
				if destination[len(destination)-1] != '/' {
					destination = destination + "/"
					fmt.Printf("appending '/' to destination path\n")
				}
				s3opts := &s3tar.S3TarS3Options{
					Threads:      threads,
					DeleteSource: false,
					Region:       region,
					EndpointUrl:  endpointUrl,
					ExternalToc:  externalToc,
				}
				s3opts.SrcBucket, s3opts.SrcKey = s3tar.ExtractBucketAndPath(archiveFile)
				s3opts.SrcPrefix = filepath.Dir(s3opts.SrcKey)
				s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(destination)
				s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
				ctx = s3tar.SetLogLevel(ctx, logLevel)
				svc := s3Client(ctx, loadOption)
				archiveClient := newArchiveClient(svc)
				return archiveClient.Extract(ctx, s3opts, s3tar.WithExtractPrefix(prefix))
			} else if list {
				svc := s3Client(ctx, loadOption)
				s3opts := &s3tar.S3TarS3Options{
					Threads:      threads,
					DeleteSource: false,
					Region:       region,
					EndpointUrl:  endpointUrl,
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
				err := s3tar.GenerateToc(archiveFile, destination, &s3tar.S3TarS3Options{})
				if err != nil {
					log.Fatal(err.Error())
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

	retryer := config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 10
		})
	})
	withDefaultOpts := append([]func(*config.LoadOptions) error{retryer}, opts...)
	cfg, err := config.LoadDefaultConfig(ctx, withDefaultOpts...)

	if err != nil {
		log.Fatal(err.Error())
	}
	return s3.NewFromConfig(cfg)

}

func parseLogLevel(count int) int {
	verboseCount := count - 1
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
