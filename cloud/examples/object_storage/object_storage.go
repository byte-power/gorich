package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/byte-power/gorich/cloud/object_storage"
)

// Configure secret_id, secret_key, region and bucket_name to run this example.
func main() {
	optionForTencentCloud := cloud.CommonOption{
		Provider:  cloud.TencentCloudProvider,
		SecretID:  "tencentcloud_secret_id_xxx",
		SecretKey: "tencentcloud_secret_key_xxx",
		Region:    "tencentcloud_region_xxx",
	}
	object_storage_examples("tencentcloud_bucket_name_xxx", optionForTencentCloud)

	optionForAWS := cloud.CommonOption{
		Provider:  cloud.AWSProvider,
		SecretID:  "aws_secret_id_xxx",
		SecretKey: "aws_secret_key_xxx",
		Region:    "aws_region_xxx",
	}
	object_storage_examples("aws_bucket_name_xxx", optionForAWS)

	optionForAliOSS := object_storage.AliCloudStorageOption{
		//CredentialType: "oidc_role_arn",
		//EndPoint:       "oss-cn-zhangjiakou.aliyuncs.com",
		//SessionName:    "test-rrsa-oidc-token",

		CredentialType:  "ak",
		EndPoint:        "oss-cn-beijing.aliyuncs.com",
		AccessKeyID:     "alicloud_access_key_id_xxx",
		AccessKeySecret: "alicloud_access_key_secret_xxx",
	}
	object_storage_examples("alicloud_bucket_name_xxx", optionForAliOSS)
}

func object_storage_examples(bucketName string, option cloud.Option) {
	service, err := object_storage.GetObjectStorageService(bucketName, option)
	if err != nil {
		fmt.Printf("GetObjectStorageService error:%s\n", err)
		return
	}

	files := map[string]*object_storage.PutObjectInput{
		"abc/a.txt":  {Body: []byte("abc"), ContentType: "application/json", Tagging: "Key1=Value1&Key2=Value2"},
		"abc/ab.txt": {Body: []byte("abcdefg"), ContentType: "text/html"},
		"bc/b.txt":   {Body: []byte("xyz")},
	}

	// PUTObject examples
	for name, input := range files {
		err = service.PutObject(context.TODO(), name, input)
		if err != nil {
			fmt.Printf("PutObject %s error %s\n", name, err)
			return
		}
		fmt.Printf("PutObject %s Content: %s ContentType:%s\n", name, string(input.Body), input.ContentType)
	}

	// ListObject examples
	objects, token, err := service.ListObjects(context.TODO(), "abc/", nil, 100)
	if err != nil {
		fmt.Printf("ListObject error %s\n", err)
		return
	}
	fmt.Printf("ListObject token is %v\n", token)
	// Note: content type is empty string since list does not return content type info.
	for _, object := range objects {
		fmt.Printf("ListObject %s  Size: %d LastModified: %+v ContentType: %s\n", object.GetKey(), object.GetObjectSize(), object.GetModifiedTime(), object.GetContentType())
	}

	// HeadObject and GetObject examples
	for name := range files {
		object, err := service.HeadObject(context.TODO(), name)
		if err != nil {
			if !errors.Is(err, object_storage.ErrObjectNotFound) {
				fmt.Printf("HeadObject %s error %s\n", name, err)
				return
			}
			fmt.Printf("HeadObject %s not found\n", name)
		} else {
			fmt.Printf("HeadObject %s Size: %d LastModified: %+v ContentType: %s\n", object.GetKey(), object.GetObjectSize(), object.GetModifiedTime(), object.GetContentType())
		}

		object, err = service.GetObject(context.TODO(), name)
		if err != nil {
			if !errors.Is(err, object_storage.ErrObjectNotFound) {
				fmt.Printf("GetObject %s error %s\n", name, err)
				return
			}
			fmt.Printf("GetObject %s not found\n", name)
		} else {
			content, err := object.GetContent()
			if err != nil {
				fmt.Printf("GetObject %s content error %s\n", name, err)
				return
			}
			fmt.Printf("GetObject %s Content: %s Size: %d, LastModified: %+v, ContentType: %s\n", object.GetKey(), string(content), object.GetObjectSize(), object.GetModifiedTime(), object.GetContentType())
		}
	}

	// GetSignedURLForExistedKey examples
	for name := range files {
		url, err := service.GetSignedURLForExistedKey(context.TODO(), name, 5*time.Second)
		if err != nil {
			if !errors.Is(err, object_storage.ErrObjectNotFound) {
				fmt.Printf("GetSignedURLForExistedKey error %s\n", err)
				return
			}
			fmt.Printf("GetSignedURLForExistedKey not found %s\n", name)
		} else {
			fmt.Printf("GetSignedURLForExistedKey %s URL: %s\n", name, url)
		}
	}

	// DeleteObject examples
	name := "abc/a.txt"
	err = service.DeleteObject(context.TODO(), name)
	if err != nil {
		fmt.Printf("DeleteObject %s error %s\n", name, err)
		return
	}
	fmt.Printf("DeleteObject %s\n", name)

	// DeleteObjects examples
	names := []string{"abc/a.txt", "bc/b.txt", "abc/ab.txt"}
	err = service.DeleteObjects(context.TODO(), names...)
	if err != nil {
		fmt.Printf("DeleteObjects error %s\n", err)
		return
	}
	fmt.Printf("DeleteObjects %+v\n", names)

	// GetSignedURL examples
	for name := range files {
		url, err := service.GetSignedURL(name, 1*time.Hour)
		if err != nil {
			fmt.Printf("GetSignedURL for object %s error %s\n", name, err)
			return
		}
		fmt.Printf("GetSignedURL for object %s %s\n", name, url)
	}

	// HeadObject and GetObject for non-exist keys examples, will return error
	for name := range files {
		object, err := service.HeadObject(context.TODO(), name)
		if err != nil {
			if !errors.Is(err, object_storage.ErrObjectNotFound) {
				fmt.Printf("HeadObject %s error %s\n", name, err)
				return
			}
			fmt.Printf("HeadObject %s not found\n", name)
		} else {
			fmt.Printf("HeadObject %s Size: %d LastModified: %+v ContentType: %s\n", object.GetKey(), object.GetObjectSize(), object.GetModifiedTime(), object.GetContentType())
		}

		object, err = service.GetObject(context.TODO(), name)
		if err != nil {
			if !errors.Is(err, object_storage.ErrObjectNotFound) {
				fmt.Printf("GetObject %s error %s\n", name, err)
				return
			}
			fmt.Printf("GetObject %s not found\n", name)
		} else {
			content, err := object.GetContent()
			if err != nil {
				fmt.Printf("GetObject %s content error %s\n", name, err)
				return
			}
			fmt.Printf("GetObject %s Content: %s Size: %d, LastModified: %+v, ContentType: %s\n", object.GetKey(), string(content), object.GetObjectSize(), object.GetModifiedTime(), object.GetContentType())
		}
	}

	// GetSignedURLForExistedKey examples for non-exist keys, will return error
	for name := range files {
		url, err := service.GetSignedURLForExistedKey(context.TODO(), name, 5*time.Second)
		if err != nil {
			if !errors.Is(err, object_storage.ErrObjectNotFound) {
				fmt.Printf("GetSignedURLForExistedKey error %s\n", err)
				return
			}
			fmt.Printf("GetSignedURLForExistedKey not found %s\n", name)
		} else {
			fmt.Printf("GetSignedURLForExistedKey %s %s\n", name, url)
		}
	}

	// PutSignedURL examples
	for name, item := range files {
		// get pre-signed put url
		opt := object_storage.PutHeaderOption{
			ContentType: aws.String(item.ContentType),
		}
		url, err := service.PutSignedURL(name, 1*time.Hour, opt)
		if err != nil {
			fmt.Printf("GetSignedURL for object %s error %s\n", name, err)
			return
		}
		fmt.Printf("GetSignedURL for put object  %s %s\n", name, url)

		// put content to s3 with signed-url
		if err := uploadContent(url, string(item.Body), item.ContentType); err != nil {
			fmt.Printf("Error uploading content: %v\n", err)
			return
		}

		// get pre-signed url for download and check
		getSignedURL, err := service.GetSignedURL(name, 1*time.Hour)
		if err != nil {
			fmt.Printf("GetSignedURL for object %s error %s\n", name, err)
			return
		}
		fmt.Printf("GetSignedURL for get object %s %s\n", name, getSignedURL)

		// check content
		content, err := accessContentBySignedURL(getSignedURL)
		if err != nil {
			log.Fatalf("Get content err: %v", err)
		}
		fmt.Println("Get content:", content)
	}
}

// uploadContent uploads content to S3 using the provided presigned URL.
func uploadContent(signedURL, content, contentType string) error {
	req, err := http.NewRequest("PUT", signedURL, bytes.NewBuffer([]byte(content)))
	if err != nil {
		return fmt.Errorf("error creating PUT request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error executing PUT request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed with status code %d", resp.StatusCode)
	}
	fmt.Println("Content uploaded successfully")
	return nil
}

// accessContentBySignedURL access content by Signed URL
func accessContentBySignedURL(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("access content failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("access content failed, status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("get content failed: %v", err)
	}
	return string(body), nil
}
