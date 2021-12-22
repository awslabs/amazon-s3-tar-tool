package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rem7/s3tar"
	"github.com/urfave/cli/v2"
)

func main() {

	var src string
	var dst string
	var dstRegion string
	var threads uint
	var deleteSource bool
	var region string
	rand.Seed(time.Now().UnixNano())
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "bucket",
						Value:       "",
						Usage:       "bucket to clear multiparts",
						Destination: &src,
					},
					&cli.StringFlag{
						Name:        "region",
						Value:       "",
						Usage:       "region to initialize the sdk",
						Destination: &region,
						EnvVars:     []string{"AWS_DEFAULT_REGION", "AWS_REGION"},
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
					&cli.StringFlag{
						Name:        "dst-region",
						Value:       "us-west-2",
						Usage:       "destination region",
						Destination: &dstRegion,
					},
					&cli.UintFlag{
						Name:        "goroutines",
						Value:       20,
						Usage:       "number of goroutines",
						Destination: &threads,
					},
					&cli.BoolFlag{
						Name:        "delete-source",
						Value:       false,
						Usage:       "this will delete the original data. TODO implement",
						Destination: &deleteSource,
					},
					&cli.StringFlag{
						Name:        "region",
						Value:       "",
						Usage:       "region to initialize the sdk",
						Destination: &region,
						EnvVars:     []string{"AWS_DEFAULT_REGION", "AWS_REGION"},
					},
				},
				Name:    "create",
				Usage:   "specify a source folder in S3 and a destination in a separate folder",
				Aliases: []string{"c"},
				Action: func(c *cli.Context) error {
					if src == "" || dst == "" {
						log.Fatalf("src or dst missing")
					}

					s3opts := &s3tar.SSTarS3Options{
						Threads:      threads,
						DeleteSource: deleteSource,
					}
					s3opts.SrcBucket, s3opts.SrcPrefix = s3tar.ExtractBucketAndPath(src)
					s3opts.DstBucket, s3opts.DstKey = s3tar.ExtractBucketAndPath(dst)
					s3opts.DstPrefix = filepath.Dir(s3opts.DstKey)

					cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
					if err != nil {
						log.Fatal(err.Error())
					}
					svc := s3.NewFromConfig(cfg)

					log.Printf("Taring contents of %s to %s", src, dst)
					s3tar.ServerSideTar(context.Background(), svc, s3opts)
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
