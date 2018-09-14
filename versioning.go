package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"time"
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

func putBucketVersioning(svc *s3.S3, bucket, status string) {

	input := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String(status),
		},
	}

	_, err := svc.PutBucketVersioning(input)
	if err != nil {
		dumpAwsError(err)
		return
	}

	//fmt.Println(result)
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
	versionId = strings.Trim(aws.StringValue(result.VersionId), "\"")
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
	versionId = strings.Trim(aws.StringValue(result.VersionId), "\"")

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

	versionId = strings.Trim(aws.StringValue(result.VersionId), "\"")
	if result.DeleteMarker != nil && *result.DeleteMarker {
		deleteMarkerReturned = true
	}

	return
}

func deleteObjectWithVersion(svc *s3.S3, bucket, key, versionIdRequested string) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		VersionId: aws.String(versionIdRequested),
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

	versionIdResponse := strings.Trim(aws.StringValue(result.VersionId), "\"")

	if versionIdResponse != versionIdRequested {
		fmt.Println("deleteObjectWithVersion: versionIdResponse does not equal versionIdRequested")
		return
	}
}

func main() {

	profile, region, endpoint, bucketName, objectName := "minio", "us-east-1", "http://localhost:9000", "", "object"
//	profile, region, endpoint, bucketName, objectName := "prive", "us-west-1", "https://s3-us-west-1.amazonaws.com", "versioned12345678", ""

	// Specify profile for config and region for requests
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{S3ForcePathStyle: aws.Bool(true), Region: aws.String(region), Endpoint: aws.String(endpoint)},
		Profile: profile,
	}))

	// Create S3 service client
	svc := s3.New(sess)

	if bucketName == "" {
		// Create version enabled bucket
		bucketName = fmt.Sprintf("versioned-%d", time.Now().Unix())
		createBucket(svc, bucketName, region)
		putBucketVersioning(svc, bucketName, "Enabled")
	} else {
		objectName = fmt.Sprintf("object-%d", time.Now().Unix())
		fmt.Println(objectName)
	}

	basicTests(svc, bucketName, objectName)
	//encryptionTests()
}

func basicTests(svc *s3.S3, bucketName, objectName string) {

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