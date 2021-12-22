# s3tar

A utility to TAR existing objects in S3.

Using the Multipart uploads and the `UploadPartCopy` API we can copy existing objects into a one object. This utility will create the intermediate header files that go between each file and then concatenate all of the objects into a tarball. This will make it easier to archive in AWS S3 Glacier. 

## Usage

```bash
s3tar create --src=s3://mybucket/some/files/ --dst=s3://mybucket/archives/some_files.tar
```

## How the tool works
Currently multipart uploads have the following limitations regarding size:

```5 MB to 5 GB. There is no minimum size limit on the last part of your multipart upload.```

s3tar will automatically detect the size of the objects it needs to tar. If files are smaller than the 5MB multipart limitation the tool will recursively concatenate groups of files into 10MB S3 objects. The first file of a group is a 5MB empty file (zeros), everything gets appended to this file, on the last file of the group a `CopySourceRange` is performed removing the `5MB` pad. As a last step the tool will merge all the objects together creating the final tar. 

```
Group1 = remove5MB([(((((5MB File) + header1) + file1) + header2) + file2)...])
Group2 = remove5MB([(((((5MB File) + header1) + file1) + header2) + file2)...])
NewObject = Concat(Group1, Group2)
```
If the files being tar-ed are larger than 5MB then it will create pairs of (file + next header) and then merge. The first file will have a 5MB padding, this will be removed at the end:

```
NewS3Object = [(5MB Zeroes + tar_header1) + (S3 Existing Object 1) + tar_header2 + (S3 Existing Object 1) ... (EOF 2x512 blocks)]
```


TODO: 
Add a manifest file to be able to download individual files based on range requests.