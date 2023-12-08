# Amazon S3 Tar Tool

[![Go Build status](https://github.com/awslabs/amazon-s3-tar-tool/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/awslabs/amazon-s3-tar-tool/actions/workflows/go.yml)
![go-version](https://img.shields.io/github/go-mod/go-version/awslabs/amazon-s3-tar-tool)
[![API Reference](https://img.shields.io/badge/api-reference-blue.svg)](https://pkg.go.dev/github.com/awslabs/amazon-s3-tar-tool)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/awslabs/amazon-s3-tar-tool/blob/main/LICENSE)

s3tar is utility tool to create a tarball of existing objects in Amazon S3.

s3tar allows customers to group existing Amazon S3 objects into TAR files without having to download the files. This cli tool leverages existing Amazon S3 APIs to create the archives on Amazon S3 that can be later transitioned to any of the cold storage tiers. The files generated follow the tar file format and can be extracted with standard tar tools.

Using the Multipart Uploads API, in particular `UploadPartCopy` API, we can copy existing objects into one object. This utility will create the intermediate TAR header files that go between each file and then concatenate all of the objects into a single tarball. 

## Usage

The tool follows the tar syntax for creation and extraction of tarballs with a few additions to support Amazon S3 operations. 

| flag            | description                                                           | required             |
|-----------------|-----------------------------------------------------------------------|----------------------|
| -c              | create                                                                | yes, unless using -x |
| -x              | extract                                                               | yes, unless using -c |
| -C              | destination to extract                                                | yes when using -x    |
| -f              | file that will be generated or extracted: s3://bucket/prefix/file.tar | yes                  |
| -t              | list files in archive                                                 | no                   |
| --extended      | to use with -t to extend the output to filename,loc,length,etag       | no                   |
| -m              | manifest input                                                        | no                   |
| --region        | aws region where the bucket is                                        | yes                  |
| -v, -vv, -vvv   | level of verbose                                                      | no                   |    
| --format        | Tar format PAX or GNU, default is PAX                                 | no                   |
| --endpointUrl   | specify an Amazon S3 endpoint                                         | no                   |
| --storage-class | specify an Amazon S3 storage class, default is STANDARD               | no                   |
| --size-limit    | This will split the tar files into multiple tars                      | no                   |


The syntax for creating and extracting tarballs remains similar to traditional tar tools:
```bash
   s3tar --region region [-c --create] | [-x --extract] [-v] -f s3://bucket/prefix/file.tar s3://bucket/prefix
```

#### Storage Class Options

| Storage Class       |
|---------------------|
| STANDARD            | 
| REDUCED_REDUNDANCY  | 
| STANDARD_IA         |
| ONEZONE_IA          |
| INTELLIGENT_TIERING |
| GLACIER             |
| DEEP_ARCHIVE        |
| OUTPOSTS            |
| GLACIER_IR          |


### Examples

To create a tarball `s3://bucket/prefix/archive.tar` from all the objects located under `s3://bucket/files/`
```bash
s3tar --region us-west-2 -cvf s3://bucket/prefix/archive.tar s3://bucket/files/
```

```bash
# this will create tarballs that are 1GB in size
s3tar --region us-west-2 --size-limit 1074000000 -cvf s3://bucket/archive.tar s3://bucket/files/
# outputs: 
# s3://bucket/archive.01.tar 
# s3://bucket/archive.02.tar 
# s3://bucket/archive.03.tar 
```

#### Manifest Input

The tool supports an input manifest `-m`. The manifest is a comma-separated-value (csv) file with `bucket,key,content-length` and an optional `etag`. Content-length is the size in bytes of the object. For example:

```bash
$ cat manifest.input.csv
my-bucket,prefix/file.0001.exr,68365312
my-bucket,prefix/file.0002.exr,50172928
my-bucket,prefix/file.0003.exr,67663872

$ s3tar --region us-west-2 -cvf s3://bucket/prefix/archive.tar -m /Users/bolyanko/manifest.input.csv

# the manifest file can be a local file or an object in Amazon S3

$ s3tar --region us-west-2 -cvf s3://bucket/prefix/archive.tar -m s3://bucket/prefix/manifest.input.csv


# The manifest can also contain the etag as a fourth column if its is known
$ cat manifest.input.csv
my-bucket,prefix/file.0001.exr,68365312,45d8e659e74e8596e3a25e80abb14636
my-bucket,prefix/file.0002.exr,50172928,9d972e4a7de1f6791f92f06c1c7bd1ca
my-bucket,prefix/file.0003.exr,67663872,6f2c195e8ab661e1a32410e5022914b7

```

### TOC & Extract
Tarballs created with this tool generate a Table of Contents (TOC). This TOC file is at the beginning of the archive and it contains a csv line per file with the `name, byte location, content-length, Etag`. This added functionality allows archives that are created this way to also be extracted without having to download the tar object. 

You can extract a tarball from Amazon S3 into another Amazon S3 location with the following command:

```bash 
s3tar --region us-west-2 -xvf s3://bucket/prefix/archive.tar -C s3://bucket/destination/
```

To extract a single file in a tar, or a prefix

```bash 
s3tar --region us-west-2 -xvf s3://bucket/prefix/archive.tar -C s3://bucket/destination/ folder/image1.jpg 
# or a dir
s3tar --region us-west-2 -xvf s3://bucket/prefix/archive.tar -C s3://bucket/destination/ folder/ 
```

### List
If you want to list the files in a tar
```bash 
s3tar --region us-west-2 -tf s3://bucket/prefix/archive.tar 
folder/image1.jpg
folder/image2.jpg
folder/image3.jpg
other-folder/image1.jpg
other-folder/image2.jpg
other-folder/image3.jpg
```


### Generating manifest files

We can generate manifest files to pass to s3tar with other tools. This will allow us to apply advanced filtering. For example, using the AWS CLI and jq we can create a file and filter the date with `--query`:

```bash
bucket=my-bucket
prefix=AWSLogs/123456789012/CloudTrail/us-west-2/2023/
date_start='2023-04-04'
date_end='2023-04-05'

$ aws s3api list-objects --bucket $bucket --prefix $prefix \
--query "Contents[?LastModified >= '"$date_start"' && LastModified < '"$date_end"' && Size > \`0\`].{Key: Key,Size: Size}" \
| jq -r '.[] += {"bucket": "'$bucket'"}' \
| jq -r '["bucket","Key","Size"] as $cols | map(. as $row | $cols | map($row[.])) as $rows | $cols, $rows[] | @csv' > manifest.csv
```

Another option is to use a CSV generated by [Amazon S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html). For example, the following command will generate a manifest file for objects `LastModified` between 2022-12-01 and 2023-01-01.
```bash
# Column 1 is the bucket
# Column 2 is the key
# Column 6 has the size
# Column 7 has the LastModified timestamp
# Column 8 is the ETag
awk -F',' 'BEGIN {OFS=","} {gsub(/"/,"",$6);gsub(/"/, "", $7); if($6>0 && $7>="2022-12-01 00:00:00" && $7<"2023-01-01 00:00:00") print $1,$2,$6,$8}' s3-inventory.csv > output.csv
```


### Performance

The tool's performance is bound by the API calls limitations. The table below has a few tests with files of different sizes. 

| Number of Files | Final archive size | Average Object Size | Creation Time | Extraction Time | Estimated Cost (us-west-2) - Standard |
|-----------------|--------------------|---------------------|---------------|-----------------|---------------------------------------|
| 41,593          | 20 GB              | 512 KB              | 6m10s         | 3m11s           | $0.4159                               |
| 124,779         | 61 GB              | 512 KB              | 18m24s        | 10m5s           | $1.2478                               |
| 249,558         | 123 GB             | 512 KB              | 40m56s        | 21m42s          | $2.4956                               |
| 499,116         | 246 GB             | 512 KB              | 1h34m         | 38m58s          | $4.9912                               |
| 748,674         | 369 GB             | 512 KB              | 2h36m         |                 | $7.48674                              |
| 14,400          | 73 GB              | 70 MB               | 2m15s         | 1m20s           | $0.1440                               |
| 69,121          | 3.75 TB            | 70 MB               | 1h11m30s      | 32m20s          | $0.6912                               |


The application is configured to retry every Amazon S3 operation up to 10 times with a Max backoff time of 20 seconds. If you get a timeout error, try reducing the number of files. 

## Installation

A make file is included that helps building the application for `darwin-arm64` `linux-arm64` `linux-amd64`. Place the resulting `s3tar` binary in your `PATH`. 

If you get the error `dial tcp: lookup proxy.golang.org i/o timeout` it means your network is restricting access to that domain. You can bypass the proxy by setting the following variable: `export GOPROXY=direct` 

## How the tool works

This tools utilizes Amazon S3 Multipart Upload (MPU). MPU allows you to upload a single object as a set of parts. Each part is a contiguous portion of the object's data. You can upload these object parts independently and in any order. After all parts of your object are uploaded, Amazon S3 assembles these parts and creates the object. 

Multipart upload is a three-step process: You initiate the upload, you upload the object parts or copy from an existing Amazon S3 Object, and after you have all the parts, you complete the multipart upload. Upon receiving the complete multipart upload request, Amazon S3 constructs the object from all the parts, and you can then access the object just as you would any other object in your bucket. You can learn more about Multipart Upload on the [MPU Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)

There are two Amazon S3 API Operations that allow adding data to a Multipart Upload. [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html) and [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html). This tool generates TAR header files and uses `s3.UploadPart` to upload the header data into a MPU, and then it uses `s3.UploadPartCopy` to copy your existing Amazon S3 Object into the newly created object. 

Currently Multipart Uploads have a minimum requirement of 5MB per part and each part can go up to 5GiB. The total maximum MPU object size is 5TiB. 

s3tar automatically detects the size of the objects it needs to tar. The **total size** of all the files must be greater than 5MB. If the individual files are smaller than the 5MB multipart limitation the tool will recursively concatenate groups of files into 10MB S3 objects. The tool generates an empty 5MB file (zeros) and everything gets appended to this file, on the last file of the group a `CopySourceRange` is performed removing the `5MB` pad. As a last step the tool will merge all the objects together creating the final tar. 

```
Group1 = remove5MB([(((((5MB File) + header1) + file1) + header2) + file2)...])
Group2 = remove5MB([(((((5MB File) + header1) + file1) + header2) + file2)...])
NewObject = Concat(Group1, Group2)
```

If the files being tar-ed are larger than 5MB then it will create pairs of (file + next header) and then merge. The first file will have a 5MB padding, this will be removed at the end:

```
NewS3Object = [(5MB Zeroes + tar_header1) + (S3 Existing Object 1) + tar_header2 + (S3 Existing Object 1) ... (EOF 2x512 blocks)]
```

## Testing & Validation
We encourage the end-user to write validation workflows to verify the data has been properly tared. If objects being tared are smaller than 5GB, users can use Amazon S3 Batch Operations to generate checksums for the individual objects. After the creation of the tar, users can extract the data into a separate bucket/folder and run the same batch operations job on the new data and verify that the checksums match. To learn more about using checksums for data validation, along with some demos, please watch [Get Started With Checksums in Amazon S3 for Data Integrity Checking](https://www.youtube.com/watch?v=JGsdvDPSirU).

## Pricing
It's important to understand that Amazon S3's API has costs associated with it. In particular `PUT`, `COPY`, `POST` are charged at a higher rate than `GET`. The majority of requests performed by this tool are `COPY` and `PUT` operations. Please refer to [the Amazon S3 Pricing page](https://aws.amazon.com/s3/pricing/) for a breakdown of the API costs. You can also use the [AWS Cost Calculator](https://calculator.aws) to help you price your operations.

During the build process the tool uses Amazon S3 Standard to work on files. If you are aggregating 1,000 objects, then it will require at least 1,000 `COPY` operations and 1,000 `PUT` operations for the tar headers. 

Example: If we want to aggregate 10,000 files

    $0.005 PUT, COPY, POST, LIST requests (per 1,000 requests)
    To Copy the 10,000 files to the archive we will do at least 10,000 COPY operations

    10,000 / 1,000 * $0.005 = $0.05 
    We need to generate at least 10,000 header files, that's 10,000 PUT opeartions

    10,000 / 1,000 * $0.005 = $0.05 
    There are other intermidiate operations of creating multipart
    It would cost a little over $0.1 to create an archive of 10,000 files

The cost example above only prices the cost of performing the operation. It doesn't include how much it would cost to store the final object. 

## Limitations of the tool
This tool still has the same limitations of Multipart Object sizes:
- The cumulative size of the TAR must be over 5MB
- The final size cannot be larger than 5TB

---
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

---

## Frequently Asked Questions (FAQ)


**Does the tool download any files?**

No, all files are copied from their current location in Amazon S3 to their destination using the `s3.UploadPartCopy` API call. 

---

**Does the tool upload any files?**

We are using the go `archive/tar` library to generate the Tar headers that go in between the files. These files are uploaded to Amazon S3 and concatenated with the Multipart Upload. 

---

**Does the tool delete any files?**

No, the original files will remain untouched. The user is responsible for the lifecycle of the objects. 

---

**Is compression supported?**

No, the tool is only copying existing data from Amazon S3 to another Amazon S3 location. To compress the objects it would require the tool to download the data, compress and then re-upload to Amazon S3. 

---

**Are Amazon S3 tags and meta-data copied to the tarball** 

No. Currently we're storing the `Etag` in the TOC, there is a possibility that could allow us to expand this. 

--- 

**What size of files are supported?**

Any size that is within the Amazon S3 Multipart Object limitations. On the small side they can be as small as a few bytes, as long as the total archive at the end is over 5MB. On the large side the max size per object is 5GB, and the total archive is 5TB. 

---

**Can I open the resulting tar anywhere?**

Yes, the tarballs are created with either PAX (default) or GNU headers. You can download the tar file generated and extract it using the same tools you use to operate on tar files. 

---

**What type of compute resources do I need to run the tool?**

Since the tool is only doing API calls, any compute that can reach the Amazon S3 API should suffice. This could run on a t4g.nano or Lambda, as long as the number of files is low enough for the 15 minute window.

## License

This project is licensed under the Apache-2.0 License.
