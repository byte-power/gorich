package object_storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/relvacode/iso8601"
	"github.com/tencentyun/cos-go-sdk-v5"
)

const (
	httpLastModifiedHeader = "Last-Modified"
	httpEtagHeader         = "Etag"
)

func getTencentCloudBucketURL(name, region string) (*url.URL, error) {
	u := fmt.Sprintf("https://%s.cos.%s.myqcloud.com", name, region)
	return url.Parse(u)
}

func getTencentCloudServiceURL(region string) (*url.URL, error) {
	u := fmt.Sprintf("https://cos.%s.myqcloud.com", region)
	return url.Parse(u)
}

type TencentCloudObjectStorageService struct {
	client *cos.Client
}

func GetTencentCloudObjectService(bucketName string, option cloud.Option) (ObjectStorageService, error) {
	if bucketName == "" {
		return nil, ErrBucketNameEmpty
	}
	if err := option.CheckTencentCloud(); err != nil {
		return nil, err
	}
	bucketURL, err := getTencentCloudBucketURL(bucketName, option.GetRegion())
	if err != nil {
		return nil, err
	}
	serviceURL, err := getTencentCloudServiceURL(option.GetRegion())
	if err != nil {
		return nil, err
	}
	baseURL := &cos.BaseURL{BucketURL: bucketURL, ServiceURL: serviceURL}
	client := cos.NewClient(baseURL, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  option.GetSecretID(),
			SecretKey: option.GetSecretKey(),
		}})
	return &TencentCloudObjectStorageService{client: client}, nil
}

func (service *TencentCloudObjectStorageService) ListObjects(ctx context.Context, prefix string, continueToken *string, maxObjects int) ([]*Object, *string, error) {
	var marker string
	if continueToken == nil {
		marker = ""
	} else {
		marker = *continueToken
	}
	opts := &cos.BucketGetOptions{
		Prefix:  prefix,
		MaxKeys: maxObjects,
		Marker:  marker,
	}
	resp, _, err := service.client.Bucket.Get(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	objects := make([]*Object, 0, len(resp.Contents))
	for _, obj := range resp.Contents {
		lastModified, err := iso8601.ParseString(obj.LastModified)
		if err != nil {
			return nil, nil, err
		}
		object := &Object{
			key:          obj.Key,
			eTag:         obj.ETag,
			size:         obj.Size,
			lastModified: lastModified,
		}
		objects = append(objects, object)
	}
	var nextToken *string
	if resp.IsTruncated {
		nextToken = &resp.NextMarker
	}
	return objects, nextToken, nil
}

func (service *TencentCloudObjectStorageService) GetObject(ctx context.Context, key string) (*Object, error) {
	if key == "" {
		return nil, ErrObjectKeyEmpty
	}
	resp, err := service.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	lastModified, err := parseLastModifiedFromHeader(resp.Header)
	if err != nil {
		return nil, fmt.Errorf("parse header %s error: %+v %w", httpLastModifiedHeader, resp.Header[httpLastModifiedHeader], err)
	}
	eTag, err := parseEtagFromHeader(resp.Header)
	if err != nil {
		return nil, fmt.Errorf("parse header %s error: %+v %w", httpEtagHeader, resp.Header[httpEtagHeader], err)
	}
	return &Object{
		key:             key,
		isContentLoaded: true,
		content:         bs,
		eTag:            eTag,
		size:            int64(len(bs)),
		lastModified:    lastModified,
	}, nil
}

func parseLastModifiedFromHeader(headers http.Header) (time.Time, error) {
	header, ok := headers[httpLastModifiedHeader]
	if !ok {
		return time.Time{}, fmt.Errorf("header %s does not exist", httpLastModifiedHeader)
	}
	if len(header) < 1 {
		return time.Time{}, fmt.Errorf("header %s format is invalid: %+v", httpLastModifiedHeader, header)
	}
	return time.Parse(time.RFC1123, header[0])
}

func parseEtagFromHeader(headers http.Header) (string, error) {
	header, ok := headers[httpEtagHeader]
	if !ok {
		return "", fmt.Errorf("header %s does not exist", httpEtagHeader)
	}
	if len(header) < 1 {
		return "", fmt.Errorf("header %s format is invalid: %+v", httpEtagHeader, header)
	}
	return header[0], nil
}

func (service *TencentCloudObjectStorageService) PutObject(ctx context.Context, key string, body []byte) error {
	if key == "" {
		return ErrObjectKeyEmpty
	}
	_, err := service.client.Object.Put(ctx, key, bytes.NewReader(body), nil)
	return err
}

func (service *TencentCloudObjectStorageService) DeleteObject(ctx context.Context, key string) error {
	if key == "" {
		return ErrObjectKeyEmpty
	}
	_, err := service.client.Object.Delete(ctx, key)
	return err
}

func (service *TencentCloudObjectStorageService) DeleteObjects(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return errors.New("parameter keys should not be empty")
	}
	objects := make([]cos.Object, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, cos.Object{Key: key})
	}
	opts := &cos.ObjectDeleteMultiOptions{Objects: objects}
	_, _, err := service.client.Object.DeleteMulti(ctx, opts)
	return err
}

func (service *TencentCloudObjectStorageService) GetSignedURL(key string, duration time.Duration) (string, error) {
	if key == "" {
		return "", ErrObjectKeyEmpty
	}
	url, err := service.client.Object.GetPresignedURL(
		context.Background(), http.MethodGet, key,
		service.client.GetCredential().SecretID,
		service.client.GetCredential().SecretKey,
		duration, nil,
	)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}
