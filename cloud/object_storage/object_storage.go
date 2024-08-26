package object_storage

import (
	"context"
	"errors"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/url"
	"strconv"
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
	// GetSignedURLForExistedKey generates signed url if key exists. If key does not exist, return error
	GetSignedURLForExistedKey(ctx context.Context, key string, duration time.Duration) (string, error)
	PutSignedURL(key string, duration time.Duration, option PutHeaderOption) (string, error)
}

type PutObjectInput struct {
	Body        []byte
	ContentType string
	Tagging     string
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

type PutHeaderOption struct {
	ContentDisposition *string
	ContentEncoding    *string
	ContentMD5         *string
	ContentType        *string
	ContentLength      *int64
	Tagging            *string
}

func (o *PutHeaderOption) ToAliCloudOptions() []oss.Option {

	options := make([]oss.Option, 0)
	if o.ContentDisposition != nil {
		options = append(options, oss.ContentDisposition(*o.ContentDisposition))
	}
	if o.ContentEncoding != nil {
		options = append(options, oss.ContentEncoding(*o.ContentEncoding))
	}
	if o.ContentMD5 != nil {
		options = append(options, oss.ContentMD5(*o.ContentMD5))
	}
	if o.ContentType != nil {
		options = append(options, oss.ContentType(*o.ContentType))
	}
	if o.ContentLength != nil {
		options = append(options, oss.ContentLength(*o.ContentLength))
	}
	if o.Tagging != nil {
		options = append(options, oss.SetHeader(oss.HTTPHeaderOssTagging, *o.Tagging))
	}
	return options
}

func (o *PutHeaderOption) ToTencentCloudOptions() *cos.PresignedURLOptions {

	opt := &cos.PresignedURLOptions{
		Query:  &url.Values{},
		Header: &http.Header{},
	}

	if o.ContentDisposition != nil {
		opt.Header.Add("Content-Disposition", *o.ContentDisposition)
	}
	if o.ContentEncoding != nil {
		opt.Header.Add("Content-Encoding", *o.ContentEncoding)
	}
	if o.ContentMD5 != nil {
		opt.Header.Add("Content-MD5", *o.ContentMD5)
	}
	if o.ContentType != nil {
		opt.Header.Add("Content-Type", *o.ContentType)
	}
	if o.ContentLength != nil {
		opt.Header.Add("Content-Length", strconv.FormatInt(*o.ContentLength, 10))
	}
	if o.Tagging != nil {
		opt.Header.Add("x-cos-tagging", *o.Tagging)
	}
	return opt
}
