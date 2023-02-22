// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	s3tar "github.com/aws-samples/amazon-s3-tar-tool"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/urfave/cli/v2"
)

func main() {
	ctx := s3tar.SetupLogger(context.Background())
	var src string
	var dst string
	var dstRegion string
	var threads uint
	var deleteSource bool
	var region string
	var logLevel int
	var manifestPath string
	var skipManifestHeader bool

	rand.Seed(time.Now().UnixNano())
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "region",
				Value:       "",
				Usage:       "region to initialize the sdk",
				Destination: &region,
				EnvVars:     []string{"AWS_DEFAULT_REGION", "AWS_REGION"},
			},
			&cli.IntFlag{
				Name:        "log-level",
				Value:       1,
				Usage:       "log-level",
				Destination: &logLevel,
				// EnvVars:     []string{""},
			},
		},
		Commands: []*cli.Command{
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "bucket",
						Value:       "",
						Usage:       "bucket to clear multiparts",
						Destination: &src,
					},
				},
				Name:  "delete",
				Usage: "delete all multiparts in a bucket",
				Action: func(c *cli.Context) error {
					cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
					if err != nil {
						log.Fatal(err.Error())
					}
					svc := s3.NewFromConfig(cfg)
					return s3tar.DeleteAllMultiparts(svc, src)
				},
			},
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "src",
						Value:       "",
						Usage:       "local directory or s3 url",
						Destination: &src,
					},
					&cli.StringFlag{
						Name:        "dst",
						Value:       "",
						Usage:       "full path for object as a s3 url",
						Destination: &dst,
					},
				},
				Name:    "extract",
				Usage:   "extract tar file",
				Aliases: []string{"x"},
				Action: func(c *cli.Context) error {
					if src == "" && manifestPath == "" {
						log.Fatalf("src or manifest flag missing")
					}
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
						DeleteSource: deleteSource,
						Region:       region,
					}
					s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(src)
					s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(dst)
					s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
					ctx = s3tar.SetLogLevel(ctx, logLevel)
					return s3tar.Extract(ctx, svc, s3opts)
				},
			},
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "src",
						Value:       "",
						Usage:       "local directory or s3 url",
						Destination: &src,
					},
					&cli.StringFlag{
						Name:        "dst",
						Value:       "",
						Usage:       "full path for object as a s3 url",
						Destination: &dst,
					},
					&cli.StringFlag{
						Name:        "dst-region",
						Value:       "us-west-2",
						Usage:       "destination region",
						Destination: &dstRegion,
					},
					&cli.StringFlag{
						Name:        "manifest",
						Value:       "",
						Usage:       "manifest file with bucket,key per line to process",
						Destination: &manifestPath,
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
					&cli.BoolFlag{
						Name:        "delete-source",
						Value:       false,
						Usage:       "this will delete the original data. TODO implement",
						Destination: &deleteSource,
					},
				},
				Name:    "create",
				Usage:   "specify a source folder in S3 and a destination in a separate folder",
				Aliases: []string{"c"},
				Action: func(c *cli.Context) error {
					if src == "" && manifestPath == "" {
						log.Fatalf("src or manifest flag missing")
					}
					if dst == "" {
						log.Fatalf("dst path missing")
					}

					s3opts := &s3tar.S3TarS3Options{
						SrcManifest:        manifestPath,
						SkipManifestHeader: skipManifestHeader,
						Threads:            threads,
						DeleteSource:       deleteSource,
						Region:             region,
					}
					s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(dst)
					s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)
					if dst != "" {
						s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(src)
					}

					ctx = s3tar.SetLogLevel(ctx, logLevel)

					cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
					if err != nil {
						log.Fatal(err.Error())
					}
					svc := s3.NewFromConfig(cfg)
					s3tar.ServerSideTar(ctx, svc, s3opts)
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
