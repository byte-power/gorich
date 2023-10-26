package object_storage

import (
	"context"
	"errors"
	"time"

	"github.com/byte-power/gorich/cloud"
)

var (
	ErrObjectKeyEmpty  = errors.New("object key is empty")
	ErrBucketNameEmpty = errors.New("bucket name is empty")
	ErrObjectNotFound  = errors.New("object is not found")
)

type ObjectStorageService interface {
	ListObjects(ctx context.Context, prefix string, continueToken *string, maxObjects int) ([]Object, *string, error)
	HeadObject(ctx context.Context, key string) (Object, error)
	GetObject(ctx context.Context, key string) (Object, error)
	PutObject(ctx context.Context, key string, input *PutObjectInput) error
	DeleteObject(ctx context.Context, key string) error
	DeleteObjects(ctx context.Context, keys ...string) error
	GetSignedURL(key string, duration time.Duration) (string, error)
	//GetSignedURLForExistedKey generates signed url if key exists. If key does not exist, return error
	GetSignedURLForExistedKey(ctx context.Context, key string, duration time.Duration) (string, error)
}

type PutObjectInput struct {
	Body        []byte
	ContentType string
}

type Object struct {
	key             string
	content         []byte
	isContentLoaded bool
	eTag            string
	lastModified    time.Time
	size            int64
	contentType     string
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

func (object Object) GetObjectSize() int64 {
	return object.size
}

func (object Object) GetContentType() string {
	return object.contentType
}

func GetObjectStorageService(bucketName string, option cloud.Option) (ObjectStorageService, error) {
	if option.GetProvider() == cloud.TencentCloudProvider {
		return GetTencentCloudObjectService(bucketName, option)
	} else if option.GetProvider() == cloud.AWSProvider {
		return GetAWSObjectService(bucketName, option)
	} else if option.GetProvider() == cloud.AliCloudStorageProvider {
		return GetAliCloudObjectService(bucketName, option)
	}
	return nil, cloud.ErrUnsupportedCloudProvider
}
