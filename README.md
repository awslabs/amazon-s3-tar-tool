# Amazon S3 Tar Tool

A utility tool to create a tarball of existing objects in Amazon S3.

s3tar allows customers to group existing Amazon S3 objects into TAR files without having to download the files. This cli tool leverages existing Amazon S3 APIs to create the archives on Amazon S3 that can be later transitioned to any of the cold storage tiers. The files generated follow the tar file format and can be extracted with standard tar tools.

Using the Multipart Uploads API, in particular `UploadPartCopy` API, we can copy existing objects into a one object. This utility will create the intermediate TAR header files that go between each file and then concatenate all of the objects into a single tarball. 

## Usage

To create a tarball
```bash
s3tar --region us-west-2 create --src=s3://mybucket/some/files/ --dst=s3://mybucket/archives/some_files.tar
```

## Manifest
Tarballs created with this tool have the option to generate a manifest file. This manifest file is at the beginning of the file and it contains a csv line per file with the `name, byte location, content-length, Etag`. This added functionality allows archives that are created this way to also be extracted without having to download the tar object. 

Extracting a tarball that was created with a manifest file:

```bash 
s3tar --region us-west-2 extract --src s3://mybucket/archives/some_files.tar --dst s3://mybucket/some_files-extract/
```

## How the tool works

This tools utilizes Amazon S3 Multipart Upload (MPU). MPU allows you to upload a single object as a set of parts. Each part is a contiguous portion of the object's data. You can upload these object parts independently and in any order. After all parts of your object are uploaded, Amazon S3 assembles these parts and creates the object. 

Multipart upload is a three-step process: You initiate the upload, you upload the object parts or copy from an existing Amazon S3 Object, and after you have all the parts, you complete the multipart upload. Upon receiving the complete multipart upload request, Amazon S3 constructs the object from the all the parts, and you can then access the object just as you would any other object in your bucket. You can learn more about Multipart Upload on the [MPU Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)

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

### Limitations of the tool
This tool still has the same limitations of Multipart Object sizes:
- The cumulative size of the TAR must be over 5MB
- The final size cannot be larger than 5TiB
