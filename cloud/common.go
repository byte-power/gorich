package cloud

import (
	"errors"
	"fmt"
)

type Provider string

const (
	AWSProvider          Provider = "aws"
	TencentCloudProvider Provider = "tencent"
)

var (
	ErrUnsupportedCloudProvider = fmt.Errorf("unsupported provider, only support %s and %s", AWSProvider, TencentCloudProvider)
	ErrEmptySecretID            = errors.New("secret_id is empty")
	ErrEmptySecretKey           = errors.New("secret_key is empty")
	ErrEmptyRegion              = errors.New("region is empty")
)

type Option interface {
	GetProvider() Provider
	GetSecretID() string
	GetSecretKey() string
	GetRegion() string
	Check() error
}

type CommonOption struct {
	Provider  Provider
	SecretID  string
	SecretKey string
	Region    string
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

func (option CommonOption) Check() error {
	if option.Provider != AWSProvider && option.Provider != TencentCloudProvider {
		return ErrUnsupportedCloudProvider
	}
	if option.SecretID != "" {
		return ErrEmptySecretID
	}
	if option.SecretKey == "" {
		return ErrEmptySecretKey
	}
	if option.Region == "" {
		return ErrEmptyRegion
	}
	return nil
}
