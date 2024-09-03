// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// buildHeader builds a tar header for the given S3 object.
//
// Parameters:
//   - o: The S3 object for which the tar header needs to be built.
//   - prev: The previous S3 object.
//   - addZeros: A flag indicating whether to add zeros.
//   - head: The head object containing S3 metadata, used to set file permissions, owner, and group.
//
// Returns:
//   - S3Obj: The S3 object with the built tar header.
//
// Example:
//
//	o := &S3Obj{
//	  Key:           aws.String("example.txt"),
//	  Size:          aws.Int64(1024),
//	  LastModified:  aws.Time(time.Now()),
//	}
//	prev := &S3Obj{
//	  Size: aws.Int64(512),
//	}
//	addZeros := true
//	head := &s3.HeadObjectOutput{
//	  Metadata: map[string]*string{
//	    "file-permissions": aws.String("0644"),
//	    "file-owner":       aws.String("1000"),
//	    "file-group":       aws.String("1000"),
//	  },
//	}
//	result := buildHeader(o, prev, addZeros, head)
//	fmt.Println(result)
func buildHeader(o, prev *S3Obj, addZeros bool, head *s3.HeadObjectOutput) S3Obj {

	name := *o.Key
	var buff bytes.Buffer
	tw := tar.NewWriter(&buff)
	hdr := &tar.Header{
		Name:       name,
		Mode:       0600,
		Size:       *o.Size,
		ModTime:    *o.LastModified,
		ChangeTime: *o.LastModified,
		AccessTime: time.Now(),
		Format:     tarFormat,
	}
	setHeaderPermissionsS3Head(hdr, head)

	if addZeros {
		buff.Write(pad)
	}

	if prev != nil && prev.Size != nil && *prev.Size > 0 {
		padSize := findPadding(*prev.Size)
		buff.Write(pad[:padSize])
	}
	if err := tw.WriteHeader(hdr); err != nil {
		log.Println("here...")
		log.Fatal(err)
	}
	if err := tw.Flush(); err != nil {
		// we ignore this error, the tar library will complain that we
		// didn't write the whole file. This part is already on Amazon S3
	}
	data := buff.Bytes()
	atomic.AddInt64(&accum, int64(len(data)+int(*o.Size)))
	ETag := fmt.Sprintf("%x", md5.Sum(data))
	return S3Obj{
		Object: types.Object{
			Key:  aws.String("header"),
			ETag: &ETag,
			Size: aws.Int64(int64(len(data))),
		},
		Data: data,
	}
}

func setHeaderPermissionsS3Head(hdr *tar.Header, head *s3.HeadObjectOutput) {
	if head != nil {
		setHeaderPermissions(hdr, head.Metadata)
	}
}

// setHeaderPermissions sets the permissions, owner, and group of a tar.Header based on the metadata from s3.HeadObjectOutput.
// If the "file-permissions" metadata is present, it is parsed as an octal string and set as the Mode of the tar.Header.
// If the "file-owner" metadata is present, it is parsed as an integer and set as the Uid of the tar.Header.
// If the "file-group" metadata is present, it is parsed as an integer and set as the Gid of the tar.Header.
// The hdr parameter is a pointer to the tar.Header that will be modified.
// The head parameter is a pointer to the s3.HeadObjectOutput that contains the metadata.
// If head is nil or if the metadata is empty, no modifications will be made to the tar.Header.
func setHeaderPermissions(hdr *tar.Header, s3metadata map[string]string) {
	if len(s3metadata) > 0 {
		if modeStr, ok := s3metadata["file-permissions"]; ok {
			modeInt, err := strconv.ParseInt(modeStr, 8, 64)
			if err != nil {
				log.Fatal(err)
			}
			hdr.Mode = modeInt
		}
		if ownerStr, ok := s3metadata["file-owner"]; ok {
			ownerInt, err := strconv.ParseInt(ownerStr, 10, 32)
			if err != nil {
				log.Fatal(err)
			}
			hdr.Uid = int(ownerInt)
		}
		if groupStr, ok := s3metadata["file-group"]; ok {
			groupInt, err := strconv.ParseInt(groupStr, 10, 32)
			if err != nil {
				log.Fatal(err)
			}
			hdr.Gid = int(groupInt)
		}
		if atimeStr, ok := s3metadata["file-atime"]; ok {
			hdr.AccessTime = s3metadataToTime(atimeStr)
		}
		if mtimeStr, ok := s3metadata["file-mtime"]; ok {
			var timeVal = s3metadataToTime(mtimeStr)
			hdr.ModTime = timeVal
			hdr.ChangeTime = timeVal
		}
	}
}

func s3metadataToTime(timeStr string) time.Time {
	var timeValue time.Time
	if strings.HasSuffix(timeStr, "ns") {
		timeInt, err := strconv.ParseInt(strings.TrimSuffix(timeStr, "ns"), 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		timeValue = time.Unix(0, timeInt)
	} else {
		atimeInt, err := strconv.ParseInt(timeStr, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		timeValue = time.Unix(0, atimeInt*int64(time.Millisecond))
	}
	return timeValue
}

func buildHeaders(objectList []*S3Obj, frontPad bool) []*S3Obj {
	headers := []*S3Obj{}
	for i := 0; i < len(objectList); i++ {
		o := objectList[i]
		name := *o.Key
		filename := filepath.Base(name)
		prev := &S3Obj{Object: types.Object{}}
		addZero := true
		if i > 0 {
			prev = objectList[i-1]
			addZero = false
		}
		if !frontPad {
			addZero = false
		}
		/* buildHeaders is only called from processHeaders which builds the manifest using createCSVTOC.
		 * inspection of createCSVTOC shows that file permissions, uid and gid are not used in the manifest
		 * therefore we do not need to pass in the head object output
		 */
		newObject := buildHeader(o, prev, addZero, nil)
		newObject.PartNum = i
		newObject.Key = aws.String(filename + ".hdr")
		headers = append(headers, &newObject)
	}
	return headers
}

func processHeaders(ctx context.Context, objectList []*S3Obj, frontPad bool) []*S3Obj {
	headers := buildHeaders(objectList, frontPad)
	sort.Sort(byPartNum(headers))

	///////////////////////
	// Create last header
	// remove 5MB
	atomic.AddInt64(&accum, -int64(beginningPad))
	lastblockSize := findPadding(accum)
	if lastblockSize == 0 {
		lastblockSize = blockSize
	}
	lastblockSize += blockSize * 2
	lastBytes := make([]byte, lastblockSize)
	lastHeader := NewS3Obj()
	lastHeader.AddData(lastBytes)
	lastHeader.NoHeaderRequired = true
	lastHeader.Key = aws.String("last.hdr")
	lastHeader.PartNum = len(headers) + 1
	headers = append(headers, lastHeader)
	return headers
}
