package blob

import (
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

type Bucket interface {
	ListObjects(prefix string, continueToken *string, maxObjects int) ([]*Object, *string, error)
	GetObject(key string) (*Object, error)
	PutObject(key string, body []byte) error
	DeleteObject(key string) error
	DeleteObjects(keys ...string) error
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

func GetBucket(name string, options cloud.Options) (Bucket, error) {
	if name == "" {
		return nil, errors.New("bucket name should not be empty")
	}
	if err := options.Check(); err != nil {
		return nil, err
	}
	if options.Provider == cloud.TencentCloudProvider {
		bucketURL, err := getTencentCloudBucketURL(name, options.Region)
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
		return &TencentCloudBucket{client: client}, nil
	}
	return nil, nil
}
