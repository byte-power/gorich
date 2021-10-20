package blob

import (
	"context"
	"io"

	"github.com/byte-power/gorich/cloud"
)

type Bucket interface {
	ListObjects(prefix string, nextToken *string, maxObjects int64) ([]*Object, error)
	GetObject(key string) (*Object, error)
	PutObject(input PutObjectInput) error
	DeleteObject(key string) error
	DeleteObjects(keys ...string) error
}

type Object struct {
	key     string
	content []byte
}

type ObjectAccessControl string

const (
	// public-read-write
	ObjectAccessControlPublicReadWrite ObjectAccessControl = "public_read_write"
	// public-read
	ObjectAccessControlPublicRead ObjectAccessControl = "public_read"
	// private
	ObjectAccessControlPrivate ObjectAccessControl = "private"
)

type PutObjectInput struct {
	Key           string
	Body          io.ReadSeeker
	AccessControl ObjectAccessControl
}

type S3Bucket struct {
}

type TencentCloudBucket struct {
}

func OpenBucket(context.Context, cloud.Options) (Bucket, error) {
	return nil, nil
}
