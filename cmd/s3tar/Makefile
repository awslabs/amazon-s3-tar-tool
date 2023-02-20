all: darwin-arm64 linux-arm64 linux-amd64
darwin-arm64:
	GOOS=darwin GOARCH=arm64 go build -o ./bin/s3tar-darwin-arm64
linux-arm64:
	GOOS=linux GOARCH=arm64 go build -o ./bin/s3tar-linux-arm64 
linux-amd64:
	GOOS=linux GOARCH=arm64 go build -o ./bin/s3tar-linux-amd64
