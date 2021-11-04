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
	ErrProviderNotTencentCloud  = errors.New("provider is not tencent cloud")
	ErrProviderNotAWS           = errors.New("provider is not aws")
	ErrEmptySecretID            = errors.New("secret_id is empty")
	ErrEmptySecretKey           = errors.New("secret_key is empty")
	ErrEmptyRegion              = errors.New("region is empty")
)

type Option interface {
	GetProvider() Provider
	GetSecretID() string
	GetSecretKey() string
	GetRegion() string

	CheckAWS() error
	CheckTencentCloud() error
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

func (option CommonOption) check() error {
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
