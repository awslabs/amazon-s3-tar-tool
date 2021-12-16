# s3tar
Servier Side S3 TAR Utility

A utility to TAR existing objects in S3.

Using the `UploadPartCopy` API we can copy multiple existing objects into a Multipart Upload. This utility will create the intermediate header files that go between each file and then concatenate all of the objects into a tarball. This will make it easier to archive in AWS S3 Glacier. 

```
NewS3Object = [tar_header1 + (S3 Existing Object 1) + tar_header2 + (S3 Existing Object 1) ... (EOF 2x512 blocks)]
```

```bash
s3tar create --src=s3://mybucket/some/files/ --dst=s3://mybucket/archives/some_files.tar
```

TODO: 
Add a manifest file to be able to download individual files based on range requests.