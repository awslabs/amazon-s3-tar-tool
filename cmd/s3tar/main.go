// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3tar "github.com/awslabs/amazon-s3-tar-tool"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"path/filepath"
)

func main() {
	ctx := s3tar.SetupLogger(context.Background())
	var create bool
	var extract bool
	var region string
	var archiveFile string // file flag
	var threads uint
	var skipManifestHeader bool
	var manifestPath string // file flag
	var tarFormat string    // file flag

	app := &cli.App{
		UseShortOptionHandling: true,
		Authors: []*cli.Author{
			&cli.Author{
				Name:  "Yanko Bolanos",
				Email: "bolyanko@amazon.com",
			},
		},
		UsageText: "tar --region us-west-2 [-c --create] | [-x --extract] [-v] -f s3://bucket/prefix/file.tar s3://bucket/prefix",
		Copyright: "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.",
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
				Name:        "file",
				Value:       "",
				Usage:       "file",
				Aliases:     []string{"f"},
				Destination: &archiveFile,
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
		},
		Action: func(cCtx *cli.Context) error {
			logLevel := parseLogLevel(cCtx.Count("verbose"))
			if region == "" {
				exitError(1, "region is missing\n")
			}
			if archiveFile == "" {
				exitError(2, "-f is a required flag\n")
			}
			if create {
				src := cCtx.Args().First() // TODO implement dir list
				if src == "" && manifestPath == "" {
					exitError(4, "source directory or manifest file is required.\n")
				}
				s3opts := &s3tar.S3TarS3Options{
					SrcManifest:        manifestPath,
					SkipManifestHeader: skipManifestHeader,
					Threads:            threads,
					DeleteSource:       false,
					Region:             region,
					TarFormat:          tarFormat,
				}
				s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(archiveFile)
				s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
				s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(src)

				ctx = s3tar.SetLogLevel(ctx, logLevel)

				cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
				if err != nil {
					log.Fatal(err.Error())
				}
				svc := s3.NewFromConfig(cfg)
				s3tar.ServerSideTar(ctx, svc, s3opts)
			} else if extract {

				if archiveFile == "" {
					exitError(5, "file is missing")
				}
				dst := cCtx.Args().First()
				if dst == "" {
					log.Fatalf("dst path missing")
				}
				cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
				if err != nil {
					log.Fatal(err.Error())
				}
				svc := s3.NewFromConfig(cfg)

				s3opts := &s3tar.S3TarS3Options{
					Threads:      threads,
					DeleteSource: false,
					Region:       region,
				}
				s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(archiveFile)
				s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(dst)
				s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
				ctx = s3tar.SetLogLevel(ctx, logLevel)
				return s3tar.Extract(ctx, svc, s3opts)
			} else {
				exitError(3, "operation not implemented, provide create or extract flag\n")
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
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
