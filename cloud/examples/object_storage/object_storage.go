package main

import (
	"context"
	"fmt"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/byte-power/gorich/cloud/object_storage"
)

// Configure secret_id, secret_key, region and bucket_name to run this example.
func main() {
	optionForTencentCloud := cloud.CommonOption{
		Provider:  cloud.TencentCloudProvider,
		SecretID:  "tencent_cloud_secret_id_xxx",
		SecretKey: "tencent_cloud_secret_key_xxx",
		Region:    "tencent_cloud_region_xxx",
	}
	object_storage_examples("tencent_cloud_bucket_name_xxxx", optionForTencentCloud)

	optionForAWS := cloud.CommonOption{
		Provider:  cloud.AWSProvider,
		SecretID:  "aws_access_key_id_xxx",
		SecretKey: "aws_access_secret_key_xxx",
		Region:    "aws_region_xxx",
	}
	object_storage_examples("aws_bucket_name_xxx", optionForAWS)
}

func object_storage_examples(bucketName string, option cloud.Option) {
	service, err := object_storage.GetObjectStorageService(bucketName, option)
	if err != nil {
		fmt.Printf("get service error:%s\n", err)
		return
	}

	files := map[string][]byte{
		"a.txt":  []byte("abc"),
		"ab.txt": []byte("abcdefg"),
		"b.txt":  []byte("xyz"),
	}
	for name, content := range files {
		err = service.PutObject(context.TODO(), name, content)
		if err != nil {
			fmt.Printf("put object %s error %s\n", name, err)
			return
		}
		fmt.Printf("put object name:%s content:%s\n", name, string(content))
	}

	objects, token, err := service.ListObjects(context.TODO(), "a", nil, 100)
	if err != nil {
		fmt.Printf("list object error %s\n", err)
		return
	}
	fmt.Printf("token is %v\n", token)
	for _, object := range objects {
		fmt.Printf("list object %s, last modified is %+v\n", object.GetKey(), object.GetModifiedTime())
	}

	for name := range files {
		object, err := service.GetObject(context.TODO(), name)
		if err != nil {
			fmt.Printf("get object %s error %s\n", name, err)
			return
		}
		content, err := object.GetContent()
		if err != nil {
			fmt.Printf("get object content error %s\n", err)
			return
		}
		fmt.Printf("get object content is %s, last_modified is %+v\n", string(content), object.GetModifiedTime())
	}

	name := "a.txt"
	err = service.DeleteObject(context.TODO(), name)
	if err != nil {
		fmt.Printf("delete object %s error %s\n", name, err)
		return
	}
	fmt.Printf("delete object %s\n", name)

	names := []string{"a.txt", "b.txt", "ab.txt"}
	err = service.DeleteObjects(context.TODO(), names...)
	if err != nil {
		fmt.Printf("delete objects error %s\n", err)
		return
	}
	fmt.Printf("delete objects %+v\n", names)

	for name := range files {
		url, err := service.GetSignedURL(name, 1*time.Hour)
		if err != nil {
			fmt.Printf("get signed url for object %s error %s\n", name, err)
			return
		}
		fmt.Printf("get signed url for object %s %s\n", name, url)
	}
}
