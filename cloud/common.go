package cloud

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

type Provider string

const (
	AWSProvider          Provider = "aws"
	TencentCloudProvider Provider = "tencentcloud"
)

var (
	ErrUnsupportedCloudProvider = fmt.Errorf("unsupported provider, only support %s and %s", AWSProvider, TencentCloudProvider)
	ErrProviderNotTencentCloud  = errors.New("provider is not tencentcloud")
	ErrProviderNotAWS           = errors.New("provider is not aws")
	ErrEmptySecretID            = errors.New("secret_id is empty")
	ErrEmptySecretKey           = errors.New("secret_key is empty")
	ErrEmptyRegion              = errors.New("region is empty")
)

type Option interface {
	GetProvider() Provider
	GetSecretID() string
	GetSecretKey() string
	GetAssumeRoleArn() string
	GetRegion() string
	GetAssumeRegion() string
	CheckAWS() error
	CheckTencentCloud() error
}

type CommonOption struct {
	Provider      Provider
	SecretID      string
	SecretKey     string
	AssumeRoleArn string
	Region        string
	AssumeRegion  string
}

func (option CommonOption) GetProvider() Provider {
	return option.Provider
}

func (option CommonOption) GetSecretID() string {
	return option.SecretID
}

func (option CommonOption) GetSecretKey() string {
	return option.SecretKey
}

func (option CommonOption) GetRegion() string {
	return option.Region
}

func (option CommonOption) GetAssumeRoleArn() string {
	return option.AssumeRoleArn
}

// GetAssumeRegion 多数情况 region 和 assume region 是同一个region,全球区可能出现不一致的场景
func (option CommonOption) GetAssumeRegion() string {
	if option.AssumeRegion == "" {
		return option.GetRegion()
	}
	return option.AssumeRegion
}

func (option CommonOption) check() error {
	if option.Region == "" {
		return ErrEmptyRegion
	}
	if option.Provider == AWSProvider { // aws 不用 aksk的方式了
		return nil
	}
	if option.SecretID == "" {
		return ErrEmptySecretID
	}
	if option.SecretKey == "" {
		return ErrEmptySecretKey
	}
	return nil
}

func (option CommonOption) CheckAWS() error {
	if option.Provider != AWSProvider {
		return ErrProviderNotAWS
	}
	return option.check()
}

func (option CommonOption) CheckTencentCloud() error {
	if option.Provider != TencentCloudProvider {
		return ErrProviderNotTencentCloud
	}
	return option.check()
}

// AwsNewSession
func AwsNewSession(option Option) (*session.Session, *aws.Config, error) {
	var creds *credentials.Credentials
	if option.GetSecretID() != "" && option.GetSecretKey() != "" {
		creds = credentials.NewStaticCredentials(option.GetSecretID(), option.GetSecretKey(), "")
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			CredentialsChainVerboseErrors: aws.Bool(true),
			Credentials:                   creds, // 可能是nil
			LogLevel:                      aws.LogLevel(aws.LogDebug),
			Region:                        aws.String(option.GetRegion()),
		},
		SharedConfigFiles: []string{},
	})
	if err != nil {
		return nil, nil, err
	}
	if roleArn := option.GetAssumeRoleArn(); roleArn != "" { // 切换 assumeRole
		assumeRoleCreds := stscreds.NewCredentials(sess, roleArn)
		return sess, aws.NewConfig().WithCredentials(assumeRoleCreds).WithRegion(option.GetAssumeRegion()), nil
	}
	return sess, nil, nil
}
