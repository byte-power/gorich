package object_storage

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type AWSObjectStorageService struct {
	client     *s3.S3
	bucketName string
}

func (service *AWSObjectStorageService) ListObjects(ctx context.Context, prefix string, continueToken *string, maxObjects int) ([]*Object, *string, error) {
	opts := &s3.ListObjectsV2Input{
		Bucket:            &service.bucketName,
		ContinuationToken: continueToken,
	}
	if prefix != "" {
		opts.Prefix = &prefix
	}
	if maxObjects >= 0 {
		maxKeys := int64(maxObjects)
		opts.MaxKeys = &maxKeys
	}
	resp, err := service.client.ListObjectsV2WithContext(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	objects := make([]*Object, 0, len(resp.Contents))
	for _, obj := range resp.Contents {
		object := &Object{
			key:          aws.StringValue(obj.Key),
			eTag:         aws.StringValue(obj.ETag),
			lastModified: aws.TimeValue(obj.LastModified),
			size:         aws.Int64Value(obj.Size),
		}
		objects = append(objects, object)
	}
	var nextToken *string
	if resp.IsTruncated != nil && *resp.IsTruncated {
		nextToken = resp.NextContinuationToken
	}
	return objects, nextToken, nil
}

func (service *AWSObjectStorageService) GetObject(ctx context.Context, key string) (*Object, error) {
	if key == "" {
		return nil, ErrObjectKeyEmpty
	}
	resp, err := service.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &service.bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &Object{
		key:             key,
		isContentLoaded: true,
		content:         bs,
		eTag:            aws.StringValue(resp.ETag),
		lastModified:    aws.TimeValue(resp.LastModified),
		size:            int64(len(bs)),
	}, nil
}

func (service *AWSObjectStorageService) PutObject(ctx context.Context, key string, body []byte) error {
	if key == "" {
		return ErrObjectKeyEmpty
	}
	opts := &s3.PutObjectInput{
		Bucket: &service.bucketName,
		Key:    &key,
		Body:   bytes.NewReader(body),
	}
	_, err := service.client.PutObjectWithContext(ctx, opts)
	return err
}

func (service *AWSObjectStorageService) DeleteObject(ctx context.Context, key string) error {
	if key == "" {
		return ErrObjectKeyEmpty
	}
	_, err := service.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: &service.bucketName,
		Key:    &key,
	})
	return err
}

func (service *AWSObjectStorageService) DeleteObjects(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return errors.New("parameter keys should not be empty")
	}
	objects := make([]*s3.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(key)})
	}
	_, err := service.client.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: &service.bucketName,
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	})
	return err
}

func (service *AWSObjectStorageService) GetSignedURL(key string, duration time.Duration) (string, error) {
	if key == "" {
		return "", ErrObjectKeyEmpty
	}
	request, _ := service.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &service.bucketName,
		Key:    &key,
	})
	url, err := request.Presign(duration)
	if err != nil {
		return "", err
	}
	return url, err
}
