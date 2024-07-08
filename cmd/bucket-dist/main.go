// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

var (
	client       *s3.Client
	manifestPath *string
	threads      *int
	withPrefix   *string
	extractS3    = regexp.MustCompile(`s3://(.[^/]*)/?(.*)`)
)

func init() {
	manifestPath = flag.String("manifest", "", "inventory json manifest, s3://bucket/manifest.json")
	withPrefix = flag.String("prefix", "", "only count files in the prefix")
	threads = flag.Int("threads", 5, "how many goroutines")
}

type Bin struct {
	Name       string
	LowerBound int
	UpperBound int
}

var BinLimits = []Bin{
	{"0 bytes", 0, 0},
	{"1 byte to 1 KB", 1, 1024},
	{"1 KB to 2 KB", 1025, 2048},
	{"2 KB to 4 KB", 2049, 4096},
	{"4 KB to 8 KB", 4097, 8192},
	{"8 KB to 16 KB", 8193, 16384},
	{"16 KB to 32 KB", 16385, 32768},
	{"32 KB to 64 KB", 32769, 65536},
	{"64 KB to 128 KB", 65537, 131072},
	{"128 KB to 256 KB", 131073, 262144},
	{"256 KB to 512 KB", 262145, 524288},
	{"512 KB to 1 MB", 524289, 1048576},
	{"1 MB to 2 MB", 1048577, 2097152},
	{"Greater than 2 MB", 2097153, -1},
}

func main() {
	flag.Parse()
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Println("error:", err)
		return
	}
	client = s3.NewFromConfig(cfg)
	ctx := context.Background()
	start := time.Now()

	fmt.Printf("loading manifest:\t%s\n", *manifestPath)
	manifest, err := loadManifest(ctx, *manifestPath)
	if err != nil {
		panic(err)
	}
	fmt.Printf("manifest contains %d CSVs\n", len(manifest.Files))

	bins, totalObjects := calculateObjectSizeDistribution(ctx, manifest, *withPrefix, downloadData)
	report := generateReportOutput(bins, totalObjects)
	fmt.Println(report)

	fmt.Printf("total objects: %d\n", totalObjects)
	fmt.Printf("time elapsed: %s\n", time.Now().Sub(start))
}

func calculateObjectSizeDistribution(ctx context.Context, manifest *ManifestFile, withPrefix string, fetchDataFn func(context.Context, string, string) (io.ReadCloser, error)) (map[string]int, uint64) {
	bucket := manifest.ManifestFileBucket()
	var totalLines atomic.Uint64

	ParseAllCSV := func(ctx context.Context, bucket string, CSVList []CSVFile) (map[string]int, error) {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(*threads)
		results := make([]map[string]int, len(CSVList))

		for i, f := range CSVList {
			f := f
			i := i
			g.Go(func() error {
				fmt.Printf("bucket: %s\n", bucket)
				fmt.Printf("downloading (%d/%d) s3://%s/%s\n", i+1, len(CSVList), bucket, f.Key)
				csvgzip, err := fetchDataFn(ctx, bucket, f.Key)
				if err != nil {
					return err
				}
				defer csvgzip.Close()
				res, lineCount, err := binCsvObjects(csvgzip, withPrefix, manifest)
				if err != nil {
					return err
				}
				results[i] = res
				totalLines.Add(lineCount)
				return nil
			})
		}

		bins := make(map[string]int)
		if err := g.Wait(); err != nil {
			return bins, err
		}

		for _, bin := range BinLimits {
			bins[bin.Name] = 0
		}

		for _, result := range results {
			for bin, count := range result {
				bins[bin] += count
			}
		}

		return bins, nil
	}

	allBins, err := ParseAllCSV(ctx, bucket, manifest.Files)
	if err != nil {
		log.Printf("something went wrong: %s", err.Error())
	}

	totalObjects := totalLines.Load()
	return allBins, totalObjects
}

// determineBin function calculates the bin based on the file size
func determineBin(size int) string {
	for _, bin := range BinLimits {
		if size == 0 || size < bin.UpperBound {
			return bin.Name
		}
	}
	return "Greater than 2 MB"
}

func generateReportOutput(bins map[string]int, totalObjects uint64) string {
	var reportOutput bytes.Buffer
	w := tabwriter.NewWriter(&reportOutput, 0, 0, 1, ' ', 0)
	fmt.Fprintln(w, "File Size Distribution:")
	fmt.Fprintln(w, "Bin\tCount\tPercentage\tProgress\t")
	fmt.Fprintln(w, "---\t-----\t----------\t--------\t")
	for _, bin := range BinLimits {
		count := bins[bin.Name]
		percentage := float64(count) / float64(totalObjects) * 100
		bar := strings.Repeat("#", int(percentage/2)) // Adjust bar width if needed
		fmt.Fprintf(w, "%-20s\t%4d\t%6.2f%%\t|%-50s|\t\n", bin.Name, count, percentage, bar)
	}
	w.Flush()
	return reportOutput.String()
}

// binCsvObjects will parse a CSV and place the size in a bin map
func binCsvObjects(r io.ReadCloser, withPrefix string, manifest *ManifestFile) (map[string]int, uint64, error) {
	var totalLines uint64
	bins := make(map[string]int)

	gr, err := gzip.NewReader(r)
	if err != nil {
		return bins, 0, err
	}
	csvreader := csv.NewReader(gr)
	for {
		record, err := csvreader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return bins, 0, err
		}
		if withPrefix != "" && !strings.HasPrefix(record[manifest.KeyColumn], withPrefix) {
			continue
		}
		totalLines += 1

		// find size here
		objectSize, err := strconv.Atoi(record[manifest.SizeColumn])
		if err != nil {
			return bins, 0, err
		}
		bin := determineBin(objectSize)
		bins[bin] += 1
	}
	return bins, totalLines, nil
}

func downloadData(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil

}

type ManifestFile struct {
	SourceBucket      string    `json:"sourceBucket"`
	DestinationBucket string    `json:"destinationBucket"`
	Version           string    `json:"version"`
	CreationTimestamp string    `json:"creationTimestamp"`
	FileFormat        string    `json:"fileFormat"`
	FileSchema        string    `json:"fileSchema"`
	Files             []CSVFile `json:"files"`
	SizeColumn        int
	KeyColumn         int
}

func (m *ManifestFile) ManifestFileBucket() string {
	return m.DestinationBucket[13:]
}

type CSVFile struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5Checksum string `json:"MD5checksum"`
}

func loadManifest(ctx context.Context, path string) (*ManifestFile, error) {
	bucket, key := ExtractBucketAndPath(path)

	r, err := downloadData(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	jdata := new(ManifestFile)
	err = json.Unmarshal(data, jdata)
	if err != nil {
		return nil, err
	}

	fileSchemaList := strings.Split(jdata.FileSchema, ",")

	if val, err := getAttrPosition(fileSchemaList, "size"); err == nil {
		jdata.SizeColumn = val
	} else {
		return nil, err
	}

	if val, err := getAttrPosition(fileSchemaList, "key"); err == nil {
		jdata.KeyColumn = val
	} else {
		return nil, err
	}

	return jdata, nil
}

func getAttrPosition(list []string, attr string) (int, error) {
	var val int = -1
	for i := 0; i < len(list); i++ {
		if strings.Contains(strings.ToLower(list[i]), attr) {
			val = i
			break
		}
	}
	if val < 0 {
		return val, fmt.Errorf("size attribute not found in S3 Inventory manifest")
	}
	return val, nil
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
