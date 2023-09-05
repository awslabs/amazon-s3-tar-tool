# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

COMMIT := `git rev-parse --short HEAD`
VERSION := `git describe --tags || echo "no-tag"`

LDFLAGS=-ldflags "-X=main.Version=$(VERSION) -X=main.Commit=$(COMMIT)"

all: darwin-arm64 darwin-amd64 linux-arm64 linux-amd64

zip: all
	cd bin/;zip s3tar-darwin-arm64.zip s3tar-darwin-arm64
	cd bin/;zip s3tar-darwin-amd64.zip s3tar-darwin-amd64
	cd bin/;zip s3tar-linux-arm64.zip s3tar-linux-arm64
	cd bin/;zip s3tar-linux-amd64.zip s3tar-linux-amd64

darwin-arm64:
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o ./bin/s3tar-darwin-arm64 ./cmd/s3tar/.
darwin-amd64:
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o ./bin/s3tar-darwin-amd64 ./cmd/s3tar/.
linux-arm64:
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o ./bin/s3tar-linux-arm64 ./cmd/s3tar/.
linux-amd64:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o ./bin/s3tar-linux-amd64 ./cmd/s3tar/.
