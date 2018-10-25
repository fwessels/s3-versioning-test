package main

import (
	"fmt"
	"log"
	"os"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"time"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/encrypt"
)

func dumpAwsError(err error) {
	if aerr, ok := err.(awserr.Error); ok {
		fmt.Println(aerr.Error())
	} else {
		fmt.Println(err.Error())
	}
}

func createBucket(svc *s3.S3, bucket, location string) {

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(location),
		},
	}

	_, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	//fmt.Println(result)
}

func deleteBucket(svc *s3.S3, bucket string) error {

	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := svc.DeleteBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return err
	}
	return nil
}

func putBucketVersioning(svc *s3.S3, bucket, status string) (bucketMustBeEmptyError bool, err error) {

	input := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String(status),
		},
	}

	_, err = svc.PutBucketVersioning(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "BucketMustBeEmpty":
				return true, nil
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return false, err
	}
	return
}

func getBucketVersioning(svc *s3.S3, bucket string) (bool, error) {

	input := &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	}

	result, err := svc.GetBucketVersioning(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return false, err
	}

	return result.Status != nil && *result.Status == "Enabled", nil
}

func putObject(svc *s3.S3, bucket, key string) (etag, versionId string) {

	result, err := svc.PutObject((&s3.PutObjectInput{}).
		SetBucket(bucket).
		SetKey(key).
		SetBody(strings.NewReader(strings.Repeat(fmt.Sprintf("content-%d", time.Now().UnixNano()), 1024))),
	)

	if err != nil {
		dumpAwsError(err)
		return
	}

	etag = strings.Trim(aws.StringValue(result.ETag), "\"")
	versionId = aws.StringValue(result.VersionId)
	return
}

func getObject(svc *s3.S3, bucket, key, versionId string) (etag string, deleteMarkerReturned bool) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	if versionId != "" {
		input.VersionId = aws.String(versionId)
	}

	result, err := svc.GetObject(input)
	if result.DeleteMarker != nil && *result.DeleteMarker {
		deleteMarkerReturned = true
	}
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				if !deleteMarkerReturned {
					// Ignore error in case a delete marker is returned
					fmt.Println(s3.ErrCodeNoSuchKey, aerr.Error())
				}
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	etag = strings.Trim(aws.StringValue(result.ETag), "\"")
	return
}

func headObject(svc *s3.S3, bucket, key string) (success bool) {

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Println(aerr.Code())
			switch aerr.Code() {
			case "InternalError":
				fmt.Println(aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return false
	}

	return true
}

func copyObject(svc *s3.S3, bucket, src, versionIdSource, key string) (etag, versionId string) {

	copySource := ""
	if versionIdSource == "" {
		copySource = fmt.Sprintf("/%s/%s", bucket, src)
	} else {
		copySource = fmt.Sprintf("/%s/%s?versionId=%s", bucket, src, versionIdSource)
	}

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(key),
	}

	result, err := svc.CopyObject(input)

	if err != nil {
		dumpAwsError(err)
		return
	}

	etag = strings.Trim(aws.StringValue(result.CopyObjectResult.ETag), "\"")
	versionId = aws.StringValue(result.VersionId)

	return
}

func listObjectVersions(svc *s3.S3, bucket, prefix string) (etags, versionIds, deleteMarkers []string, latestVersionId string) {

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	result, err := svc.ListObjectVersions(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	for _, ver := range result.Versions {
		etags = append(etags, strings.Trim(aws.StringValue(ver.ETag), "\""))
		versionIds = append(versionIds, *ver.VersionId)
		if *ver.IsLatest {
			latestVersionId = *ver.VersionId
		}
	}

	for _, ver := range result.DeleteMarkers {
		deleteMarkers = append(deleteMarkers, *ver.VersionId)
		if *ver.IsLatest {
			latestVersionId = *ver.VersionId
		}
	}

	return
}

func listObjects(svc *s3.S3, bucket, prefix string) (etags []string) {

	input := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	result, err := svc.ListObjects(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	for _, obj := range result.Contents {
		etags = append(etags, strings.Trim(aws.StringValue(obj.ETag), "\""))
	}

	return
}

func deleteObject(svc *s3.S3, bucket, key string) (versionId string, deleteMarkerReturned bool) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	versionId = aws.StringValue(result.VersionId)
	if result.DeleteMarker != nil && *result.DeleteMarker {
		deleteMarkerReturned = true
	}

	return
}

func deleteObjectWithVersion(svc *s3.S3, bucket, key, versionIdRequested string) (failure bool) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		VersionId: aws.String(versionIdRequested),
	}

	result, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "InvalidArgument":
				if versionIdRequested != "INVALID-VERSION-ID" {
					fmt.Println(aerr.Error())
				} else {
					return true
				}
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	} else {
		versionIdResponse := aws.StringValue(result.VersionId)

		if versionIdResponse != versionIdRequested {
			fmt.Println("deleteObjectWithVersion: versionIdResponse does not equal versionIdRequested")
			return true
		}
	}
	return false
}

func getObjectWithInvalidVersionId(svc *s3.S3, bucket, key, versionId string) (success bool) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		VersionId: aws.String(versionId),
	}

	_, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "InvalidArgument":
				return true
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}

	return
}

func headObjectWithInvalidVersionId(svc *s3.S3, bucket, key, versionId string) (success bool) {

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		VersionId: aws.String(versionId),
	}

	_, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "BadRequest":
				return true
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}

	return
}

func deleteObjectWithVersionUnversioned(svc *s3.S3, bucket, key, versionIdRequested string) (success bool) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		VersionId: aws.String(versionIdRequested),
	}

	_, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "InvalidArgument":
				return true
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}
	return
}

func copyObjectWithInvalidVersionId(svc *s3.S3, bucket, src, versionIdSource, key string) (success bool) {

	copySource := fmt.Sprintf("/%s/%s?versionId=%s", bucket, src, versionIdSource)

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(key),
	}

	_, err := svc.CopyObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "InvalidArgument":
				return true
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}

	return
}

func listObjectVersionsVerified(svc *s3.S3, ignoreEmptyResponse bool, bucket, prefix string, versionIdsVerify, deleteMarkersIdsVerify [][]string, truncated []bool) (success bool) {

	keyMarker, versionIdMarker := "", ""

	if ignoreEmptyResponse {
		// This is a fix for a minor deviation from AWS S3 behavior since Minio sometimes
		// still sends an `IsTruncated = true` when there is actually no more content.
		// So the next call is effectively a NOOP that return `IsTruncated = false` immediately.
		// So the end result is the same but it takes one additional call for Minio.
		//
		// Extend response for one additional (empty) batch
		versionIdsVerify = append(versionIdsVerify, []string{})
		deleteMarkersIdsVerify = append(deleteMarkersIdsVerify, []string{})
		truncated = append(truncated[:len(truncated)-1], []bool{true, false}...)
	}

	success = true
	for i := 0; ; i++ {

		input := &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
			MaxKeys: aws.Int64(3),
		}

		if i > 0 {
			input.KeyMarker = aws.String(keyMarker)
			input.VersionIdMarker = aws.String(versionIdMarker)
		}

		result, err := svc.ListObjectVersions(input)
		//fmt.Println(result)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return
		}

		success = success && verifyListObjectsVersions(result, versionIdsVerify[i], deleteMarkersIdsVerify[i], truncated[i])

		if result.NextKeyMarker != nil {
			keyMarker = *result.NextKeyMarker
		}
		if result.NextVersionIdMarker != nil {
			versionIdMarker = *result.NextVersionIdMarker
		}

		if !*result.IsTruncated {
			break
		}
	}

	return
}

func verifyListObjectsVersions(result *s3.ListObjectVersionsOutput, versionIdsVerify []string, deleteMarkersIdsVerify []string, truncated bool) (success bool) {
	for i, ver := range result.Versions {
		if *ver.VersionId != versionIdsVerify[i] {
			fmt.Println("MISMATCH FOR ", i, "BETWEEN VERSIONS ", *ver.VersionId, "AND", versionIdsVerify[i], "(EXPECTED)")
			return
		}
	}
	for i, dm := range result.DeleteMarkers {
		if *dm.VersionId != deleteMarkersIdsVerify[i] {
			fmt.Println("MISMATCH FOR ", i, "BETWEEN DELETE MARKERS", *dm.VersionId, "AND", deleteMarkersIdsVerify[i], "(EXPECTED)")
			return
		}
	}
	if  *result.IsTruncated != truncated {
		fmt.Println("MISMATCH BETWEEN ISTRUNCATED", *result.IsTruncated, "AND", truncated)
		return
	}
	return true
}

func listObjectWithInvalidVersionId(svc *s3.S3, bucket, prefix string) (success bool) {

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		MaxKeys: aws.Int64(3),
	}

	result, err := svc.ListObjectVersions(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	keyMarker := ""
	if result.NextKeyMarker != nil {
		keyMarker = *result.NextKeyMarker
	}

	input2nd := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		MaxKeys: aws.Int64(3),
		KeyMarker: aws.String(keyMarker),
		VersionIdMarker: aws.String("INVALID-VERSION-ID"),
	}

	_, err = svc.ListObjectVersions(input2nd)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "InvalidArgument":
				return true
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}

	return
}

func listObjectVersionsAndDelete(svc *s3.S3, bucket, prefix string) (deleted int) {

	keyMarker, versionIdMarker := "", ""

	for i := 0; ; i++ {

		input := &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
			MaxKeys: aws.Int64(3),
		}

		if i > 0 {
			input.KeyMarker = aws.String(keyMarker)
			input.VersionIdMarker = aws.String(versionIdMarker)
		}

		result, err := svc.ListObjectVersions(input)
		//fmt.Println(result)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return
		}

		objident := []*s3.ObjectIdentifier{}

		for _, v := range result.Versions {
			objident = append(objident, &s3.ObjectIdentifier{
				Key:       aws.String(*v.Key),
				VersionId: aws.String(*v.VersionId),
			})
		}
		for _, dm := range result.DeleteMarkers {
			objident = append(objident, &s3.ObjectIdentifier{
				Key:       aws.String(*dm.Key),
				VersionId: aws.String(*dm.VersionId),
			})
		}

		if len(objident) > 0 {
			delete := &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3.Delete{
					Objects: objident,
					Quiet: aws.Bool(false),
				},
			}

			resultDeleted, err := svc.DeleteObjects(delete)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					default:
						fmt.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					fmt.Println(err.Error())
				}
				return
			}
			deleted += len(resultDeleted.Deleted)
			//for _, d := range resultDeleted.Deleted {
			//	//fmt.Println("+++", *d.Key, *d.VersionId)
			//	//deleted++
			//}
		}

		if result.NextKeyMarker != nil {
			keyMarker = *result.NextKeyMarker
		}
		if result.NextVersionIdMarker != nil {
			versionIdMarker = *result.NextVersionIdMarker
		}

		if !*result.IsTruncated {
			break
		}
	}

	return
}

func multiPartInitiate(svc *s3.S3, bucket, key string) (uploadId string) {

	input := &s3.CreateMultipartUploadInput{
		Bucket: 	aws.String(bucket),
		Key:        aws.String(key),
	}

	result, err := svc.CreateMultipartUpload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	uploadId = *result.UploadId
	return
}

func multiPartUpload(svc *s3.S3, bucket, key, uploadId string) (etag string) {

	input := &s3.UploadPartInput{
		Body:       aws.ReadSeekCloser(strings.NewReader("versioning.go")),
		Bucket: 	aws.String(bucket),
		Key:        aws.String(key),
		PartNumber: aws.Int64(1),
		UploadId:   aws.String(uploadId),
	}

	result, err := svc.UploadPart(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	etag = *result.ETag
	return
}

func multiPartComplete(svc *s3.S3, bucket, key, uploadId, etagPart1 string) (etag, versionId string) {
	input := &s3.CompleteMultipartUploadInput{
		Bucket: 	aws.String(bucket),
		Key:        aws.String(key),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: []*s3.CompletedPart{
				{
					ETag:       aws.String(etagPart1),
					PartNumber: aws.Int64(1),
				},
				//{
				//	ETag:       aws.String("\"d8c2eafd90c266e19ab9dcacc479f8af\""),
				//	PartNumber: aws.Int64(2),
				//},
			},
		},
		UploadId:   aws.String(uploadId),
	}

	result, err := svc.CompleteMultipartUpload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	etag = *result.ETag
	versionId = *result.VersionId
	return
}

func putObjectMultipart(svc *s3.S3, bucketName string, objectName string) (etag, versionId string) {
	uploadId := multiPartInitiate(svc, bucketName, objectName)
	etagPart1 := multiPartUpload(svc, bucketName, objectName, uploadId)
	return multiPartComplete(svc, bucketName, objectName, uploadId, etagPart1)
}

func deleteMultipleObjects(svc *s3.S3, bucket string, objident []*s3.ObjectIdentifier) (objs []*s3.DeletedObject, errs []*s3.Error) {

	if len(objident) == 0 {
		return
	}

	delete := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objident,
			Quiet: aws.Bool(false),
		},
	}

	resultDeleted, err := svc.DeleteObjects(delete)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
	return resultDeleted.Deleted, resultDeleted.Errors
}

func putObjectEncrypted(s3Client *minio.Client, bucket, key, password string) {

	filePath := "versioning.go" // Specify a local file that we will upload

	// Open a local file that we will upload
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	// Get file stats.
	fstat, err := file.Stat()
	if err != nil {
		log.Fatalln(err)
	}

	// New SSE-C where the cryptographic key is derived from a password and the objectname + bucketname as salt
	encryption := encrypt.DefaultPBKDF([]byte(password), []byte(bucket+key))

	_, err = s3Client.PutObject(bucket, key, file, fstat.Size(), minio.PutObjectOptions{ServerSideEncryption: encryption})
	if err != nil {
		log.Fatalln(err)
	}
}

func copyObjectInPlaceEncrypted(s3Client *minio.Client, bucket, key, passwordOld, passwordNew string) (etag, versionId string, accessDenied bool) {

	// Source object
	sseSrc := encrypt.DefaultPBKDF([]byte(passwordOld), []byte(bucket+key))
	src := minio.NewSourceInfo(bucket, key, sseSrc)

	// Test key rotation for source object in-place.
	newSSE := encrypt.DefaultPBKDF([]byte(passwordNew), []byte(bucket+key)) // replace key
	dst, err := minio.NewDestinationInfo(bucket, key, newSSE, nil)
	if err != nil {
		return
	}

	err = s3Client.CopyObject(dst, src)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println("Error", aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			if err.Error() == "Access denied." {
				return "", "", true
			}
			fmt.Println("Error", err.Error())
		}
		return
	}

	return "", "", false
}

func copyObjectInPlaceEncryptedWithVersion() {
	
}

func main() {

	profile, region, endpoint, bucketName, objectName := "minio", "us-east-1", "http://localhost:9000", "", "path/object"
//	profile, region, endpoint, bucketName, objectName := "prive", "us-west-1", "https://s3-us-west-1.amazonaws.com", "versioned12345678", ""

	// Specify profile for config and region for requests
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{S3ForcePathStyle: aws.Bool(true), Region: aws.String(region), Endpoint: aws.String(endpoint)},
		Profile: profile,
	}))

	// Create S3 service client
	svc := s3.New(sess)

	basicTests(svc, bucketName, objectName, region)
	unversionedTests(svc, bucketName, objectName, region)
	invalidVersionIdTests(svc, bucketName, objectName, region)
	paginatedListingTests(svc, profile, bucketName, objectName, region)
	deleteMultipleObjectTests(svc, bucketName, objectName, region)
	multipartUploadTests(svc, bucketName, objectName, region)
	//encryptionTests(svc, bucketName, objectName, region)
	//headTests()
	if profile == "minio" {
		bucketTests(svc, bucketName, objectName, region)
	}
}

func basicTests(svc *s3.S3, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	// Upload and verify first version
	etagv1, versionIdv1 := putObject(svc, bucketName, objectName)
	if et, _ := getObject(svc, bucketName, objectName ,""); et != etagv1 {
		fmt.Println("  First version:", "*** WRONG ETAG RETURNED")
	} else {
		fmt.Println("  First version:", "Success")
	}

	// Upload and verify second version
	etagv2, versionIdv2 := putObject(svc, bucketName, objectName)
	if et, _ := getObject(svc, bucketName, objectName, ""); et != etagv2 {
		fmt.Println(" Second version:", "*** WRONG ETAG RETURNED")
	} else {
		fmt.Println(" Second version:", "Success")
	}

	// Implicitly verify second version
	if et, _ := getObject(svc, bucketName, objectName, ""); et != etagv2 {
		fmt.Println(" Second version:", "*** WRONG ETAG RETURNED")
	} else {
		fmt.Println(" Second version:", "Success")
	}

	// Explicitly verify first version
	if et, _ := getObject(svc, bucketName, objectName, versionIdv1); et != etagv1 {
		fmt.Println("  First version:", "*** WRONG ETAG RETURNED")
	} else {
		fmt.Println("  First version:", "Success")
	}

	// Explicitly verify second version
	if et, _ := getObject(svc, bucketName, objectName, versionIdv2); et != etagv2 {
		fmt.Println(" Second version:", "*** WRONG ETAG RETURNED")
	} else {
		fmt.Println(" Second version:", "Success")
	}

	// Copy latest version onto itself // TODO: AWS returns error
	//etagv3, versionIdv3 := copyObject(svc, bucketName, objectName, "", objectName)
	//if etagv3 != etagv2 {
	//	fmt.Println(" Third version:", "Wrong etag returned. Got", etagv3, " Expected", etagv2)
	//} else {
	//	fmt.Println(" Third version:", "Success")
	//}

	// Explicitly copy initial version onto itself
	etagv4, versionIdv4 := copyObject(svc, bucketName, objectName, versionIdv1, objectName)
	if etagv4 != etagv1 {
		fmt.Println(" Fourth version:", "*** WRONG ETAG RETURNED. GOT", etagv4, " EXPECTED", etagv1)
	} else {
		fmt.Println(" Fourth version:", "Success")
	}

	// Explicitly copy second version onto itself
	etagv5, versionIdv5 := copyObject(svc, bucketName, objectName, versionIdv2, objectName)
	if etagv5 != etagv2 {
		fmt.Println("  Fifth version:", "*** WRONG ETAG RETURNED. GOT", etagv5, " EXPECTED", etagv2)
	} else {
		fmt.Println("  Fifth version:", "Success")
	}

	// List object versions and check for matching etags and versionids
	etags, versionIds, _, latestVersionId := listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv5  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv5)
	}
	if etagv5 != etags[0] || etagv4 != etags[1] || etagv2 != etags[2] || etagv1 != etags[3] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if versionIdv5 != versionIds[0] || versionIdv4 != versionIds[1] || versionIdv2 != versionIds[2] || versionIdv1 != versionIds[3] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// List objects and check for match to latest etag
	etags = listObjects(svc, bucketName, objectName)
	if etags[0] != etagv5 {
		fmt.Println("   List objects:", "*** MISMATCH")
	} else {
		fmt.Println("   List objects:", "Success")
	}

	versionIdv6, deleteMarkerv6 := deleteObject(svc, bucketName, objectName)
	if !deleteMarkerv6 {
		fmt.Println("  Sixth version:", "*** MISSING DELETE MARKER")
	} else {
		fmt.Println("  Sixth version:", "Success")
	}

	_, _, deleteMarkers, _ := listObjectVersions(svc, bucketName, objectName)
	if deleteMarkers[0] != versionIdv6 {
		fmt.Println("  Sixth version:", "*** WRONG VERSION-ID RETURNED. GOT", deleteMarkers[0], " EXPECTED", versionIdv6)
	} else {
		fmt.Println("  Sixth version:", "Success")
	}

	// List objects (none are to be found)
	etags = listObjects(svc, bucketName, objectName)
	if len(etags) > 0 {
		fmt.Println("   List objects:", "*** MISMATCH, NOT EXPECTING ANY OBJECTS")
	} else {
		fmt.Println("   List objects:", "Success")
	}

	// Implicitly verify absence of object
	if et, dm := getObject(svc, bucketName, objectName, ""); !dm || et != "" {
		fmt.Println("  Sixth version:", "*** EXPECTING DELETE MARKER")
	} else {
		fmt.Println("  Sixth version:", "Success")
	}

	// Explicitly verify absence of object
	// AWS return MethodNotAllowed: The specified method is not allowed against this resource.
	//if et, dm := getObject(svc, bucketName, objectName, versionIdv6); !dm || et != "" {
	//	fmt.Println(" Sixth version:", "*** EXPECTING DELETE MARKER")
	//} else {
	//	fmt.Println(" Sixth version:", "Success")
	//}

	// Explicitly copy second version onto itself
	etagv7, versionIdv7 := copyObject(svc, bucketName, objectName, versionIdv2, objectName)
	if etagv7 != etagv2 {
		fmt.Println("Seventh version:", "*** WRONG ETAG RETURNED. GOT", etagv7, " EXPECTED", etagv2)
	} else {
		fmt.Println("Seventh version:", "Success")
	}

	// List objects and check for match to latest etag
	etags = listObjects(svc, bucketName, objectName)
	if etags[0] != etagv7 {
		fmt.Println("   List objects:", "*** MISMATCH")
	} else {
		fmt.Println("   List objects:", "Success")
	}

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv7  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv7)
	}
	if etagv7 != etags[0] || etagv5 != etags[1] || etagv4 != etags[2] || etagv2 != etags[3] || etagv1 != etags[4] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if versionIdv7 != versionIds[0] || versionIdv6 != deleteMarkers[0] || versionIdv5 != versionIds[1] || versionIdv4 != versionIds[2] || versionIdv2 != versionIds[3] || versionIdv1 != versionIds[4] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// Delete latest version (and be back to a delete marker)
	deleteObjectWithVersion(svc, bucketName, objectName, versionIdv7)
	etagv7, versionIdv7 = "", ""

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv6  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv6)
	}
	if etagv5 != etags[0] || etagv4 != etags[1] || etagv2 != etags[2] || etagv1 != etags[3] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if versionIdv6 != deleteMarkers[0] || versionIdv5 != versionIds[0] || versionIdv4 != versionIds[1] || versionIdv2 != versionIds[2] || versionIdv1 != versionIds[3] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// List objects (none are to be found)
	etags = listObjects(svc, bucketName, objectName)
	if len(etags) > 0 {
		fmt.Println("   List objects:", "*** MISMATCH, NOT EXPECTING ANY OBJECTS")
	} else {
		fmt.Println("   List objects:", "Success")
	}

	// Delete latest version (which is a delete marker)
	deleteObjectWithVersion(svc, bucketName, objectName, versionIdv6)
	versionIdv6 = ""

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv5  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv5)
	}
	if etagv5 != etags[0] || etagv4 != etags[1] || etagv2 != etags[2] || etagv1 != etags[3] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if len(deleteMarkers) > 0 || versionIdv5 != versionIds[0] || versionIdv4 != versionIds[1] || versionIdv2 != versionIds[2] || versionIdv1 != versionIds[3] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// List objects and check for match to latest etag
	etags = listObjects(svc, bucketName, objectName)
	if etags[0] != etagv5 {
		fmt.Println("   List objects:", "*** MISMATCH")
	} else {
		fmt.Println("   List objects:", "Success")
	}

	// Delete previous to latest version (see latestVersion remains v5)
	deleteObjectWithVersion(svc, bucketName, objectName, versionIdv4)
	etagv4, versionIdv4 = "", ""

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv5  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv5)
	}
	if etagv5 != etags[0] || etagv2 != etags[1] || etagv1 != etags[2] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if len(deleteMarkers) > 0 || versionIdv5 != versionIds[0] || versionIdv2 != versionIds[1] || versionIdv1 != versionIds[2] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// Delete latest version
	deleteObjectWithVersion(svc, bucketName, objectName, versionIdv5)
	etagv5, versionIdv5 = "", ""

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv2  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv2)
	}
	if etagv2 != etags[0] || etagv1 != etags[1] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if len(deleteMarkers) > 0 || versionIdv2 != versionIds[0] || versionIdv1 != versionIds[1] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// Delete initial version
	deleteObjectWithVersion(svc, bucketName, objectName, versionIdv1)
	etagv1, versionIdv1 = "", ""

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if latestVersionId != versionIdv2  {
		fmt.Println("  List versions:", "*** MISMATCH. GOT", latestVersionId, "EXPECTED ", versionIdv2)
	}
	if etagv2 != etags[0] {
		fmt.Println("     List etags:", "*** MISMATCH")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if len(deleteMarkers) > 0 || versionIdv2 != versionIds[0] {
		fmt.Println("  List versions:", "*** MISMATCH")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	deleteObjectWithVersion(svc, bucketName, objectName, versionIdv2)
	etagv2, versionIdv2 = "", ""

	// List object versions and check for matching etags and versionids
	etags, versionIds, deleteMarkers, latestVersionId = listObjectVersions(svc, bucketName, objectName)
	if len(etags) > 0 {
		fmt.Println("     List etags:", "*** NOT EXPECTING ANY")
	} else {
		fmt.Println("     List etags:", "Success")
	}
	if len(deleteMarkers) > 0 {
		fmt.Println("  List versions:", "*** NOT EXPECTING ANY")
	} else {
		fmt.Println("  List versions:", "Success")
	}

	// List objects and check for match to latest etag
	etags = listObjects(svc, bucketName, objectName)
	if len(etags) > 0 {
		fmt.Println("   List objects:", "*** NOT EXPECTING ANY")
	} else {
		fmt.Println("   List objects:", "Success")
	}
}

func unversionedTests(svc *s3.S3, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create regular bucket
		bucketName = fmt.Sprintf("unversioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
	} else {
		bucketName = "un" + bucketName
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	_, versionIdv1 := putObject(svc, bucketName, objectName)
	if versionIdv1 != "" {
		fmt.Println("   Unversioned put:", "*** NOT EXPECTING VERSION-ID")
	} else {
		fmt.Println("   Unversioned put:", "Success")
	}

	etagv2, versionIdv2 := putObject(svc, bucketName, objectName)
	if versionIdv2 != "" {
		fmt.Println("   Unversioned put:", "*** NOT EXPECTING VERSION-ID")
	} else {
		fmt.Println("   Unversioned put:", "Success")
	}

	// Implicitly get most recent version
	if et, _ := getObject(svc, bucketName, objectName, ""); et != etagv2 {
		fmt.Println("       Regular get:", "*** WRONG ETAG RETURNED")
	} else {
		fmt.Println("       Regular get:", "Success")
	}

	// Try to get a non-existing version
	success := getObjectWithInvalidVersionId(svc, bucketName, objectName, "PZbE8N3Tv3HBf1W8CAcCxJ9xgWc");
	if !success {
		fmt.Println("   Unversioned get:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("   Unversioned get:", "Success")
	}

	// Try to delete a non-existing version
	success = deleteObjectWithVersionUnversioned(svc, bucketName, objectName, "Ft7Z9Toaf9bFsAaBCAR7eH9nu3Y")
	if !success {
		fmt.Println("Unversioned delete:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("Unversioned delete:", "Success")
	}

	// Try to copy a non-existing version
	success = copyObjectWithInvalidVersionId(svc, bucketName, objectName, "js4OvwPChitcUV8kKieFuuhg8fQ", objectName)
	if !success {
		fmt.Println("  Unversioned copy:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("  Unversioned copy:", "Success")
	}

	// Do a successful HEAD on the object
	success = headObject(svc, bucketName, objectName);
	if !success {
		fmt.Println("      Regular head:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("      Regular head:", "Success")
	}

	// Try to do a HEAD on a non-existing version
	success = headObjectWithInvalidVersionId(svc, bucketName, objectName, "avZlNEXzv3h8lgKGre2d4M3O27w")
	if !success {
		fmt.Println("  Unversioned head:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("  Unversioned head:", "Success")
	}

}

func invalidVersionIdTests(svc *s3.S3, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	putObject(svc, bucketName, objectName)
	putObject(svc, bucketName, objectName)

	// Expecting invalid version for non-existing version
	failed := deleteObjectWithVersion(svc, bucketName, objectName, "INVALID-VERSION-ID")
	if !failed {
		fmt.Println("Invalid delete:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("Invalid delete:", "Success")
	}

	// Expecting invalid version for non-existing version
	success := getObjectWithInvalidVersionId(svc, bucketName, objectName, "INVALID-VERSION-ID")
	if !success {
		fmt.Println("   Invalid get:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("   Invalid get:", "Success")
	}

	// Expecting bad request for non-existing version
	success = headObjectWithInvalidVersionId(svc, bucketName, objectName, "INVALID-VERSION-ID")
	if !success {
		fmt.Println("  Invalid head:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("  Invalid head:", "Success")
	}

	success = copyObjectWithInvalidVersionId(svc, bucketName, objectName, "INVALID-VERSION-ID", objectName)
	if !success {
		fmt.Println("  Invalid copy:", "*** EXPECTED ERROR MISSING")
	} else {
		fmt.Println("  Invalid copy:", "Success")
	}
}

func paginatedListingTests(svc *s3.S3, profile, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	{ // single version
		objectName += "-1"
		_, vid1 := putObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid1}}, [][]string{{}}, []bool{false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // two versions
		objectName += "-2"
		_, vid1 := putObject(svc, bucketName, objectName)
		_, vid2 := putObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid2, vid1}}, [][]string{{}}, []bool{false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // three versions
		objectName += "-3"
		_, vid1 := putObject(svc, bucketName, objectName)
		_, vid2 := putObject(svc, bucketName, objectName)
		vid3, _ := deleteObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid2, vid1}}, [][]string{{vid3}}, []bool{false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // four versions
		objectName += "-4"
		_, vid1 := putObject(svc, bucketName, objectName)
		vid2, _ := deleteObject(svc, bucketName, objectName)
		_, vid3 := putObject(svc, bucketName, objectName)
		vid4, _ := deleteObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid3}, {vid1}}, [][]string{{vid4, vid2}, {}}, []bool{true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // five versions
		objectName += "-5"
		_, vid1 := putObject(svc, bucketName, objectName)
		vid2, _ := deleteObject(svc, bucketName, objectName)
		_, vid3 := putObject(svc, bucketName, objectName)
		_, vid4 := putObject(svc, bucketName, objectName)
		_, vid5 := putObject(svc, bucketName, objectName)

		// List object versions and check for matching etags and versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid5, vid4, vid3}, {vid1}}, [][]string{{}, {vid2}}, []bool{true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // six versions
		objectName += "-6"
		_, vid1 := putObject(svc, bucketName, objectName)
		vid2, _ := deleteObject(svc, bucketName, objectName)
		_, vid3 := putObject(svc, bucketName, objectName)
		vid4, _ := deleteObject(svc, bucketName, objectName)
		_, vid5 := putObject(svc, bucketName, objectName)
		_, vid6 := putObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, true && profile == "minio", bucketName, objectName, [][]string{{vid6, vid5}, {vid3, vid1}}, [][]string{{vid4}, {vid2}}, []bool{true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // seven versions
		objectName += "-7"
		_, vid1 := putObject(svc, bucketName, objectName)
		_, vid2 := putObject(svc, bucketName, objectName)
		vid3, _ := deleteObject(svc, bucketName, objectName)
		_, vid4 := putObject(svc, bucketName, objectName)
		_, vid5 := putObject(svc, bucketName, objectName)
		_, vid6 := putObject(svc, bucketName, objectName)
		vid7, _ := deleteObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid6, vid5}, {vid4, vid2}, {vid1}}, [][]string{{vid7}, {vid3}, {}}, []bool{true, true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // eight versions
		objectName += "-8"
		_, vid1 := putObject(svc, bucketName, objectName)
		_, vid2 := putObject(svc, bucketName, objectName)
		vid3, _ := deleteObject(svc, bucketName, objectName)
		_, vid4 := putObject(svc, bucketName, objectName)
		_, vid5 := putObject(svc, bucketName, objectName)
		_, vid6 := putObject(svc, bucketName, objectName)
		vid7, _ := deleteObject(svc, bucketName, objectName)
		_, vid8 := putObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid8, vid6}, {vid5, vid4}, {vid2, vid1}}, [][]string{{vid7}, {vid3}, {}}, []bool{true, true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // nine versions
		objectName += "-9"
		_, vid1 := putObject(svc, bucketName, objectName)
		_, vid2 := putObject(svc, bucketName, objectName)
		vid3, _ := deleteObject(svc, bucketName, objectName)
		_, vid4 := putObject(svc, bucketName, objectName)
		_, vid5 := putObject(svc, bucketName, objectName)
		_, vid6 := putObject(svc, bucketName, objectName)
		vid7, _ := deleteObject(svc, bucketName, objectName)
		_, vid8 := putObject(svc, bucketName, objectName)
		_, vid9 := putObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, true && profile == "minio", bucketName, objectName, [][]string{{vid9, vid8}, {vid6, vid5, vid4}, {vid2, vid1}}, [][]string{{vid7}, {}, {vid3}}, []bool{true, true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // test with invalid version id
		objectName += "-a"
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectWithInvalidVersionId(svc, bucketName, objectName)
		if !success {
			fmt.Println("   Invalid list:", "*** MISMATCH")
		} else {
			fmt.Println("   Invalid list:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // test across multiple objects
		_, vid1 := putObject(svc, bucketName, objectName+"-common-b")
		_, vid2 := putObject(svc, bucketName, objectName+"-common-b")
		_, vid3 := putObject(svc, bucketName, objectName+"-common-c")
		_, vid4 := putObject(svc, bucketName, objectName+"-common-c")
		_, vid5 := putObject(svc, bucketName, objectName+"-common-d")
		_, vid6 := putObject(svc, bucketName, objectName+"-common-d")
		_, vid7 := putObject(svc, bucketName, objectName+"-common-e")

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName+"-common", [][]string{{vid2, vid1, vid4}, {vid3, vid6, vid5}, {vid7}}, [][]string{{}, {}, {}}, []bool{true, true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
	}

	{ // test across multiple objects
		_, vid1 := putObject(svc, bucketName, objectName+"-version-v1")
		_, vid2 := putObject(svc, bucketName, objectName+"-version-v2")
		_, vid3 := putObject(svc, bucketName, objectName+"-version-v2")
		_, vid4 := putObject(svc, bucketName, objectName+"-version-v2")
		_, vid5 := putObject(svc, bucketName, objectName+"-version-v2")
		_, vid6 := putObject(svc, bucketName, objectName+"-version-v3")
		_, vid7 := putObject(svc, bucketName, objectName+"-version-v4")
		_, vid8 := putObject(svc, bucketName, objectName+"-version-v4")
		_, vid9 := putObject(svc, bucketName, objectName+"-version-v4")
		_, vid10 := putObject(svc, bucketName, objectName+"-version-v4")
		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName+"-version", [][]string{{vid1, vid5, vid4}, {vid3, vid2, vid6}, {vid10, vid9, vid8}, {vid7}}, [][]string{{}, {}, {}, {}}, []bool{true, true, true, false})
		if !success {
			fmt.Println(" Paginated list:", "*** MISMATCH")
		} else {
			fmt.Println(" Paginated list:", "Success")
		}
	}

	// Delete all versioned object under prefix
	deleted := listObjectVersionsAndDelete(svc, bucketName, objectName+"-version")
	if deleted != 10 {
		fmt.Println("List and delete:", "*** MISMATCH", deleted)
	} else {
		fmt.Println("List and delete:", "Success")
	}
	// Delete all versioned object under prefix
	deleted = listObjectVersionsAndDelete(svc, bucketName, objectName+"-common")
	if deleted != 7 {
		fmt.Println("List and delete:", "*** MISMATCH", deleted)
	} else {
		fmt.Println("List and delete:", "Success")
	}
	// Delete all versioned object from root
	deleted = listObjectVersionsAndDelete(svc, bucketName, "")
	if deleted != 52 {
		fmt.Println("List and delete:", "*** MISMATCH", deleted)
	} else {
		fmt.Println("List and delete:", "Success")
	}
	// Call again, no objects should be left
	if listObjectVersionsAndDelete(svc, bucketName, "") != 0 {
		fmt.Println("List and delete:", "*** MISMATCH")
	} else {
		fmt.Println("List and delete:", "Success")
	}
}

func deleteMultipleObjectTests(svc *s3.S3, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	{ // create single versioned object
		objectName += "-1"
		putObject(svc, bucketName, objectName)
		objectName = objectName[:len(objectName)-2]
	}

	vidPut := ""
	{  // create object with two versions
		objectName += "-2"
		putObject(svc, bucketName, objectName)
		_, vidPut = putObject(svc, bucketName, objectName)
		objectName = objectName[:len(objectName)-2]
	}

	vidDelete := ""
	{ // create object with two versions plus a delete marker
		objectName += "-3"
		putObject(svc, bucketName, objectName)
		putObject(svc, bucketName, objectName)
		vidDelete, _ = deleteObject(svc, bucketName, objectName)
		objectName = objectName[:len(objectName)-2]
	}

	objident := []*s3.ObjectIdentifier{}

	objident = append(objident, &s3.ObjectIdentifier{
		// delete an object in a versioned bucket (add a delete marker)
		Key:       aws.String(objectName + "-1"),
	})
	objident = append(objident, &s3.ObjectIdentifier{
		// delete a version of an object
		Key:       aws.String(objectName + "-2"),
		VersionId: aws.String(vidPut),
	})
	objident = append(objident, &s3.ObjectIdentifier{
		// try to delete a non-existing version of an object (will fail)
		Key:       aws.String(objectName + "-2"),
		VersionId: aws.String("INVALID-VERSION-ID"),
	})
	objident = append(objident, &s3.ObjectIdentifier{
		// delete a delete marker
		Key:       aws.String(objectName + "-3"),
		VersionId: aws.String(vidDelete),
	})
	objident = append(objident, &s3.ObjectIdentifier{
		// delete a non-existing object (succeeds as per S3 spec by creating
		// an object with just a delete marker)
		Key:       aws.String(objectName + "-NON-EXISTING-TO-BE-CREATED"),
	})

	deleted, errors := deleteMultipleObjects(svc, bucketName, objident)
	if len(deleted) != 4 {
		fmt.Println("Multiple Delete:", "*** MISMATCH")
		return
	} else {
		for _, d := range deleted {
			switch *d.Key {
			case objectName + "-1":
			case objectName + "-NON-EXISTING-TO-BE-CREATED":
				// Regular delete, for versioned objects this just adds a delete marker
				if !(*d.DeleteMarker && *d.DeleteMarkerVersionId != "" &&
				     d.VersionId == nil) {
					fmt.Println("Add a delete marker:", "*** MISMATCH")
				}
			case objectName + "-2":
				// Delete a version of an object (not a delete marker), just return requested version id
				if !(d.DeleteMarker == nil && d.DeleteMarkerVersionId == nil &&
					*d.VersionId != "") {
					fmt.Println("Delete a versioned object:", "*** MISMATCH")
				}
			case objectName + "-3":
				// Delete a delete marker, returned DeleteMarkerVersionId equals requested version id
				if !(*d.DeleteMarker && *d.DeleteMarkerVersionId == *d.VersionId) {
					fmt.Println("Delete a delete marker:", "*** MISMATCH")
				}
			default:
				fmt.Println("Multiple Delete:", "*** MISMATCH")
				return
			}
		}
	}
	if len(errors) != 1 {
		fmt.Println("Multiple Delete:", "*** MISMATCH")
	} else {
		for _, e := range errors {
			switch *e.Code {
			case "NoSuchVersion":
				if *e.Message != "The specified version does not exist." {
					fmt.Println("Delete a non-existing object:", "*** MISMATCH")
				}
			default:
				fmt.Println("Multiple Delete:", "*** MISMATCH")
				return
			}
		}
	}
	fmt.Println("Multiple Delete:", "Success")
}

func headTests() {

	// test HeadObject
	// headObject
}

func multipartUploadTests(svc *s3.S3, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	{ // single version
		objectName += "-1"
		_, vid1 := putObjectMultipart(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid1}}, [][]string{{}}, []bool{false})
		if !success {
			fmt.Println("  Multipart put:", "*** MISMATCH")
		} else {
			fmt.Println("  Multipart put:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	{ // two versions
		objectName += "-2"
		_, vid1 := putObjectMultipart(svc, bucketName, objectName)
		_, vid2 := putObjectMultipart(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid2, vid1}}, [][]string{{}}, []bool{false})
		if !success {
			fmt.Println("  Multipart put:", "*** MISMATCH")
		} else {
			fmt.Println("  Multipart put:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}
}

func encryptionTests(svc *s3.S3, bucketName, objectName, region string) {

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().UnixNano())
	}

	{ // single version
		objectName += "-1"
		_, vid1 := putObjectEncrypted(svc, bucketName, objectName)

		// List object versions and check for matching versionids
		success := listObjectVersionsVerified(svc, false, bucketName, objectName, [][]string{{vid1}}, [][]string{{}}, []bool{false})
		if !success {
			fmt.Println("  Encrypted:", "*** MISMATCH")
		} else {
			fmt.Println("  Encrypted:", "Success")
		}
		objectName = objectName[:len(objectName)-2]
	}

	//aws s3api copy-object --copy-source vadmeste/testfile --bucket vadmeste --key testfile
	//  --copy-source-sse-customer-algorithm=AES256
	//  --copy-source-sse-customer-key=MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=
	//  --copy-source-sse-customer-key-md5=7PpPLAK26ONlVUGOWlusfg==
	//  --sse-customer-algorithm=AES256
	//  --sse-customer-key=at1TMx82nEy7SoAK8jHYanMQDVZMSLayXaaUvTc6CP0=
	//  --sse-customer-key-md5="LWkBoT3psNdTYez70TVHUQ=="
	//{
	//	"CopyObjectResult": {
	//	"ETag": "\"03c88e721a47e499dc6bb489daf290c8\"",
	//		"LastModified": "2018-09-12T10:39:27.000Z"
	//},
	//	"SSECustomerKeyMD5": "LWkBoT3psNdTYez70TVHUQ==",
	//	"SSECustomerAlgorithm": "AES256",
	//	"CopySourceVersionId": "BEXop4A53jLGIDYRPsVH.xXlMe8w6eVr",
	//	"VersionId": "DU1kqfvhsC.4aZ11QA9s2i2Gn9wdwrh7"
	//}

	// Copy onto itself
	//
}

func bucketTests(svc *s3.S3, bucketName, objectName, region string) {

	// Create new bucket and enable versioning
	bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
	createBucket(svc, bucketName, region)
	if _, err := putBucketVersioning(svc, bucketName, "Enabled"); err == nil {
		fmt.Println("  Bucket tests:", "Success")
	} else {
		fmt.Println("  Bucket tests:", "*** MISMATCH")
	}

	// Create new bucket, add some content, and try to enable versioning (will fail)
	bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
	createBucket(svc, bucketName, region)
	putObject(svc, bucketName, objectName)
	if bucketMustBeEmtpyError, _ := putBucketVersioning(svc, bucketName, "Enabled"); bucketMustBeEmtpyError {
		fmt.Println("  Bucket tests:", "Success")
	} else {
		fmt.Println("  Bucket tests:", "*** MISMATCH")
	}

	// a) Create new bucket, b) enable versioning, c) delete bucket
	// d) recreate bucket, e) check versioning (should be off)
	bucketName = fmt.Sprintf("versioned-%d", time.Now().UnixNano())
	createBucket(svc, bucketName, region)
	putBucketVersioning(svc, bucketName, "Enabled")
	if enabled, err := getBucketVersioning(svc, bucketName); err != nil || !enabled {
		fmt.Println("  Bucket tests:", "*** EXPECTED TO FIND ENABLED")
	}
	if err := deleteBucket(svc, bucketName); err != nil {
		fmt.Println("  Bucket tests:", "*** MISMATCH")
	}
	createBucket(svc, bucketName, region)
	if enabled, err := getBucketVersioning(svc, bucketName); err != nil || enabled {
		fmt.Println("  Bucket tests:", "*** SHOULD NOT BE ENABLED")
	} else {
		fmt.Println("  Bucket tests:", "Success")
	}
}