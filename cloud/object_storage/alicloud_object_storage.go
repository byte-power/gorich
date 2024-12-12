package object_storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/byte-power/gorich/cloud"
	"github.com/byte-power/gorich/utils"
)

var ossClientMap = make(map[string]*oss.Client)

var (
	ErrAliCloudStorageServiceCredentialTypeEmpty  = errors.New("credential_type for alicloud storage service is empty")
	ErrAliCloudStorageServiceEndPointEmpty        = errors.New("endpoint for alicloud storage service is empty")
	ErrAliCloudStorageServiceSessionNameEmpty     = errors.New("session_name for alicloud storage service is empty")
	ErrAliCloudStorageServiceAccessKeyIDEmpty     = errors.New("access_key_id for alicloud storage service is empty")
	ErrAliCloudStorageServiceAccessKeySecretEmpty = errors.New("access_key_secret for alicloud storage service is empty")
)

type AliCloudStorageOption struct {
	CredentialType string `yaml:"credential_type" json:"credential_type"` // eg: "oidc_role_arn" or "ak"
	EndPoint       string `yaml:"endpoint" json:"endpoint"`               // eg: "oss-cn-zhangjiakou.aliyuncs.com"
	SessionName    string `yaml:"session_name" json:"session_name"`       // eg: "test-rrsa-oidc-token"

	// "ak" required
	AccessKeyID     string `yaml:"access_key_id" json:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret" json:"access_key_secret"`
}

func (option AliCloudStorageOption) GetProvider() cloud.Provider {
	return cloud.AliCloudStorageProvider
}

func (option AliCloudStorageOption) GetSecretID() string {
	return option.AccessKeyID
}

func (option AliCloudStorageOption) GetSecretKey() string {
	return option.AccessKeySecret
}

func (option AliCloudStorageOption) GetAssumeRoleArn() string {
	return ""
}

func (option AliCloudStorageOption) GetRegion() string {
	return ""
}

func (option AliCloudStorageOption) GetAssumeRegion() string {
	return ""
}

func (option AliCloudStorageOption) CheckAWS() error {
	return cloud.ErrProviderNotAWS
}

func (option AliCloudStorageOption) CheckTencentCloud() error {
	return cloud.ErrProviderNotTencentCloud
}

func (option AliCloudStorageOption) CheckStandaloneRedis() error {
	return cloud.ErrProviderNotStandaloneRedis
}

func (option AliCloudStorageOption) CheckClusterRedis() error {
	return cloud.ErrProviderNotClusterRedis
}

func (option AliCloudStorageOption) CheckAliCloudStorage() error {
	return option.check()
}

func (option AliCloudStorageOption) check() error {
	if option.CredentialType == "" {
		return ErrAliCloudStorageServiceCredentialTypeEmpty
	}
	if option.EndPoint == "" {
		return ErrAliCloudStorageServiceEndPointEmpty
	}

	if option.CredentialType == "oidc_role_arn" {
		if option.SessionName == "" {
			return ErrAliCloudStorageServiceSessionNameEmpty
		}
	} else if option.CredentialType == "ak" {
		if option.AccessKeyID == "" {
			return ErrAliCloudStorageServiceAccessKeyIDEmpty
		}
		if option.AccessKeySecret == "" {
			return ErrAliCloudStorageServiceAccessKeySecretEmpty
		}
	}
	return nil
}

type AliCloudObjectStorageService struct {
	client     *oss.Client
	bucketName string
}

// GetAliCloudObjectService
// option.credentialType Only Support "oidc_role_arn"
func GetAliCloudObjectService(bucketName string, option cloud.Option) (ObjectStorageService, error) {
	if bucketName == "" {
		return nil, ErrBucketNameEmpty
	}
	if err := option.CheckAliCloudStorage(); err != nil {
		return nil, err
	}
	storageOption, ok := option.(AliCloudStorageOption)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be AliCloudStorageOption", option)
	}

	// one endpoint, one client, return if exist
	if client, ok := ossClientMap[storageOption.EndPoint]; ok {
		return &AliCloudObjectStorageService{client: client, bucketName: bucketName}, nil
	}

	var client *oss.Client
	if storageOption.CredentialType == "oidc_role_arn" {
		cred, err := newOidcCredential(storageOption.CredentialType, storageOption.SessionName)
		if err != nil {
			return nil, err
		}
		provider := &aliCloudCredentialsProvider{
			cred: cred,
		}
		ossClient, err := oss.New(storageOption.EndPoint, "", "", oss.SetCredentialsProvider(provider))
		if err != nil {
			return nil, err
		}
		client = ossClient
	} else if storageOption.CredentialType == "ak" {
		ossClient, err := oss.New(storageOption.EndPoint, storageOption.AccessKeyID, storageOption.AccessKeySecret)
		if err != nil {
			return nil, err
		}
		client = ossClient
	} else {
		return nil, fmt.Errorf("credential type '%s' unsupported", storageOption.CredentialType)
	}

	// cache client
	ossClientMap[storageOption.EndPoint] = client
	return &AliCloudObjectStorageService{client: client, bucketName: bucketName}, nil
}

func (service *AliCloudObjectStorageService) ListObjects(ctx context.Context, prefix string, continueToken *string, maxObjects int) ([]Object, *string, error) {
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return nil, nil, err
	}
	options := make([]oss.Option, 0)
	if prefix != "" {
		options = append(options, oss.Prefix(prefix))
	}
	if continueToken != nil {
		options = append(options, oss.ContinuationToken(tea.StringValue(continueToken)))
	}
	if maxObjects > 0 {
		options = append(options, oss.MaxKeys(maxObjects))
	}
	resp, err := bucket.ListObjectsV2(options...)
	if err != nil {
		return nil, nil, err
	}
	objects := make([]Object, 0, len(resp.Objects))
	for _, obj := range resp.Objects {
		object := Object{
			key:          obj.Key,
			eTag:         obj.ETag,
			lastModified: obj.LastModified,
			size:         obj.Size,
		}
		objects = append(objects, object)
	}
	var nextToken *string
	if resp.IsTruncated {
		nextToken = &resp.NextContinuationToken
	}
	return objects, nextToken, nil
}

func (service *AliCloudObjectStorageService) HeadObject(ctx context.Context, key string) (Object, error) {
	if key == "" {
		return Object{}, ErrObjectKeyEmpty
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return Object{}, err
	}

	isExist, err := bucket.IsObjectExist(key)
	if err != nil {
		return Object{}, err
	}
	if !isExist {
		return Object{}, ErrObjectNotFound
	}

	metadata, err := bucket.GetObjectDetailedMeta(key)
	if err != nil {
		return Object{}, err
	}

	lastModified, err := HTTPHeaderLastModifiedToTime(metadata.Get(oss.HTTPHeaderLastModified))
	if err != nil {
		return Object{}, err
	}
	size, err := utils.StringToInt64(metadata.Get(oss.HTTPHeaderContentLength))
	if err != nil {
		return Object{}, err
	}
	return Object{
		key:          key,
		eTag:         metadata.Get(oss.HTTPHeaderEtag),
		lastModified: lastModified,
		size:         size,
		contentType:  metadata.Get(oss.HTTPHeaderContentType),
	}, nil
}

func (service *AliCloudObjectStorageService) GetObject(ctx context.Context, key string) (Object, error) {
	if key == "" {
		return Object{}, ErrObjectKeyEmpty
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return Object{}, err
	}

	isExist, err := bucket.IsObjectExist(key)
	if err != nil {
		return Object{}, err
	}
	if !isExist {
		return Object{}, ErrObjectNotFound
	}

	metadata, err := bucket.GetObjectDetailedMeta(key)
	if err != nil {
		return Object{}, err
	}

	resp, err := bucket.GetObject(key)
	if err != nil {
		return Object{}, err
	}
	defer resp.Close()

	bs, err := io.ReadAll(resp)
	if err != nil {
		return Object{}, err
	}

	lastModified, err := HTTPHeaderLastModifiedToTime(metadata.Get(oss.HTTPHeaderLastModified))
	if err != nil {
		return Object{}, err
	}
	size, err := utils.StringToInt64(metadata.Get(oss.HTTPHeaderContentLength))
	if err != nil {
		return Object{}, err
	}
	return Object{
		key:             key,
		isContentLoaded: true,
		content:         bs,
		eTag:            metadata.Get(oss.HTTPHeaderEtag),
		lastModified:    lastModified,
		size:            size,
		contentType:     metadata.Get(oss.HTTPHeaderContentType),
	}, nil
}

func (service *AliCloudObjectStorageService) PutObject(ctx context.Context, key string, input *PutObjectInput) error {
	if key == "" {
		return ErrObjectKeyEmpty
	}
	if input == nil {
		return errors.New("parameter input is nil")
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return err
	}
	var opts []oss.Option
	if input.ContentType != "" {
		opts = append(opts, oss.ContentType(input.ContentType))
	}
	if input.Tagging != "" {
		opts = append(opts, oss.SetHeader(oss.HTTPHeaderOssTagging, input.Tagging))
	}
	return bucket.PutObject(key, bytes.NewReader(input.Body), opts...)
}

func (service *AliCloudObjectStorageService) DeleteObject(ctx context.Context, key string) error {
	if key == "" {
		return ErrObjectKeyEmpty
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return err
	}
	return bucket.DeleteObject(key)
}

func (service *AliCloudObjectStorageService) DeleteObjects(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return errors.New("parameter keys should not be empty")
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return err
	}
	_, err = bucket.DeleteObjects(keys)
	return err
}

func (service *AliCloudObjectStorageService) GetSignedURL(key string, duration time.Duration) (string, error) {
	if key == "" {
		return "", ErrObjectKeyEmpty
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return "", err
	}
	return bucket.SignURL(key, oss.HTTPGet, int64(duration.Seconds()))
}

func (service *AliCloudObjectStorageService) GetSignedURLForExistedKey(ctx context.Context, key string, duration time.Duration) (string, error) {
	if key == "" {
		return "", ErrObjectKeyEmpty
	}
	_, err := service.HeadObject(ctx, key)
	if err != nil {
		return "", err
	}
	return service.GetSignedURL(key, duration)
}

func (service *AliCloudObjectStorageService) PutSignedURL(key string, duration time.Duration, option PutHeaderOption) (string, error) {
	if key == "" {
		return "", ErrObjectKeyEmpty
	}
	bucket, err := service.client.Bucket(service.bucketName)
	if err != nil {
		return "", err
	}
	options := option.ToAliCloudOptions()
	return bucket.SignURL(key, oss.HTTPPut, int64(duration.Seconds()), options...)
}
