package main

import (
	"context"
	"errors"
	"fmt"
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
}

func object_storage_examples(bucketName string, option cloud.Option) {
	service, err := object_storage.GetObjectStorageService(bucketName, option)
	if err != nil {
		fmt.Printf("GetObjectStorageService error:%s\n", err)
		return
	}

	files := map[string][]byte{
		"a.txt":  []byte("abc"),
		"ab.txt": []byte("abcdefg"),
		"b.txt":  []byte("xyz"),
	}

	// PUTObject examples
	for name, content := range files {
		err = service.PutObject(context.TODO(), name, content)
		if err != nil {
			fmt.Printf("PutObject %s error %s\n", name, err)
			return
		}
		fmt.Printf("PutObject %s content: %s\n", name, string(content))
	}

	// ListObject examples
	objects, token, err := service.ListObjects(context.TODO(), "a", nil, 100)
	if err != nil {
		fmt.Printf("ListObject error %s\n", err)
		return
	}
	fmt.Printf("ListObject token is %v\n", token)
	for _, object := range objects {
		fmt.Printf("ListObject %s  Size: %d LastModified: %+v\n", object.GetKey(), object.GetObjectSize(), object.GetModifiedTime())
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
			fmt.Printf("HeadObject %s Size: %d LastModified: %+v\n", name, object.GetObjectSize(), object.GetModifiedTime())
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
			fmt.Printf("GetObject %s Content: %s Size: %d, LastModified: %+v\n", name, string(content), object.GetObjectSize(), object.GetModifiedTime())
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
	name := "a.txt"
	err = service.DeleteObject(context.TODO(), name)
	if err != nil {
		fmt.Printf("DeleteObject %s error %s\n", name, err)
		return
	}
	fmt.Printf("DeleteObject %s\n", name)

	// DeleteObjects examples
	names := []string{"a.txt", "b.txt", "ab.txt"}
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
			fmt.Printf("HeadObject %s Size: %d LastModified: %+v\n", name, object.GetObjectSize(), object.GetModifiedTime())
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
			fmt.Printf("GetObject %s Content: %s Size: %d, LastModified: %+v\n", name, string(content), object.GetObjectSize(), object.GetModifiedTime())
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
}
