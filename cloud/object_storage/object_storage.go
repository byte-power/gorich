package object_storage

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/tencentyun/cos-go-sdk-v5"
)

var (
	ErrObjectKeyEmpty       = errors.New("object key should not be empty")
	ErrProviderNotSupported = errors.New("provider is not supported, only support aws and tencent cloud")
)

type ObjectStorageService interface {
	ListObjects(ctx context.Context, prefix string, continueToken *string, maxObjects int) ([]*Object, *string, error)
	GetObject(ctx context.Context, key string) (*Object, error)
	PutObject(ctx context.Context, key string, body []byte) error
	DeleteObject(ctx context.Context, key string) error
	DeleteObjects(ctx context.Context, keys ...string) error
	GetSignedURL(ctx context.Context, key string, duration time.Duration) (string, error)
}

type Object struct {
	key             string
	content         []byte
	isContentLoaded bool
	eTag            string
	lastModified    time.Time
	size            int64
}

func (object Object) GetKey() string {
	return object.key
}

func (object Object) GetContent() ([]byte, error) {
	if object.isContentLoaded {
		return object.content, nil
	}
	return nil, errors.New("object content is not loaded")
}

func (object Object) GetModifiedTime() time.Time {
	return object.lastModified
}

func GetObjectStorageService(bucketName string, options cloud.Options) (ObjectStorageService, error) {
	if bucketName == "" {
		return nil, errors.New("bucket name should not be empty")
	}
	if err := options.Check(); err != nil {
		return nil, err
	}
	if options.Provider == cloud.TencentCloudProvider {
		bucketURL, err := getTencentCloudBucketURL(bucketName, options.Region)
		if err != nil {
			return nil, err
		}
		serviceURL, err := getTencentCloudServiceURL(options.Region)
		if err != nil {
			return nil, err
		}
		baseURL := &cos.BaseURL{BucketURL: bucketURL, ServiceURL: serviceURL}
		client := cos.NewClient(baseURL, &http.Client{
			Transport: &cos.AuthorizationTransport{
				SecretID:  options.SecretID,
				SecretKey: options.SecretKey,
			}})
		return &TencentCloudObjectStorageService{client: client}, nil
	}
	return nil, nil
}
