# Amazon S3 Tar Tool

s3tar is utility tool to create a tarball of existing objects in Amazon S3.

s3tar allows customers to group existing Amazon S3 objects into TAR files without having to download the files. This cli tool leverages existing Amazon S3 APIs to create the archives on Amazon S3 that can be later transitioned to any of the cold storage tiers. The files generated follow the tar file format and can be extracted with standard tar tools.

Using the Multipart Uploads API, in particular `UploadPartCopy` API, we can copy existing objects into one object. This utility will create the intermediate TAR header files that go between each file and then concatenate all of the objects into a single tarball. 

## Usage

The tool follows the tar syntax for creation and extraction of tarballs with a few additions to support Amazon S3 operations. 

| flag          | description                                                           | required             |
|---------------|-----------------------------------------------------------------------|----------------------|
| -c            | create                                                                | yes, unless using -x |
| -x            | extract                                                               | yes, unless using -c |
| -f            | file that will be generated or extracted: s3://bucket/prefix/file.tar | yes                  |
| -m            | manifest input                                                        | no                   |
| --region      | aws region where the bucket is                                        | yes                  |
| -v, -vv, -vvv | level of verbose                                                      | no                   |    
| --format      | Tar format PAX or GNU, default is PAX                                 | no                   |    

The syntax for creating and extracting tarballs remains similar to traditional tar tools:
```bash
   tar --region region [-c --create] | [-x --extract] [-v] -f s3://bucket/prefix/file.tar s3://bucket/prefix
```

### Examples

To create a tarball `s3://bucket/prefix/archive.tar` from all the objects located under `s3://bucket/files/`
```bash
s3tar --region us-west-2 -cvf s3://bucket/prefix/archive.tar s3://bucket/files/
```

The tool supports an input manifest `-m`. The manifest is a comma-separated-value (csv) file with `bucket,key,content-length`. Content-length is the size in bytes of the object. For example:

```bash
$ cat manifest.input.csv
my-bucket,prefix/file.0001.exr,68365312
my-bucket,prefix/file.0002.exr,50172928
my-bucket,prefix/file.0003.exr,67663872

$ s3tar --region us-west-2 -cvf s3://bucket/prefix/archive.tar -m /Users/bolyanko/manifest.input.csv

# the manifest file can be a local file or an object in Amazon S3

$ s3tar --region us-west-2 -cvf s3://bucket/prefix/archive.tar -m s3://bucket/prefix/manifest.input.csv


```

### Manifest & Extract
Tarballs created with this tool have the option to generate a manifest file. This manifest file is at the beginning of the file and it contains a csv line per file with the `name, byte location, content-length, Etag`. This added functionality allows archives that are created this way to also be extracted without having to download the tar object. 

Extracting a tarball that was created with a manifest file:

```bash 
s3tar --region us-west-2 -xvf s3://bucket/prefix/archive.tar s3://bucket/destination/
```

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

### Limitations of the tool
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

**Is compression supported?**

No, the tool is only copying existing data from Amazon S3 to another Amazon S3 location. To compress the objects it would require the tool to download the data, compress and then re-upload to Amazon S3. 

---

**Are Amazon S3 tags and meta-data copied to the tarball** 

No. Currently we're storing the `Etag` in the manifest, there is a possibility that could allow us to expand this. 

--- 

**What size of files are supported?**

Any size that is within the Amazon S3 Multipart Object limitations. On the small side they can be as small as a few bytes, as long as the total archive at the end is over 5MB. On the large side the max size per object is 5GB, and the total archive is 5TB. 

---

**Can I open the resulting tar anywhere?**

Yes, you can download the tar file generated 

## License

This project is licensed under the Apache-2.0 License.