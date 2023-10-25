package object_storage

import (
	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aliyun/credentials-go/credentials"
	"log"
	"os"
	"time"
)

const (
	EnvRoleArn         = "ALIBABA_CLOUD_ROLE_ARN"
	EnvOidcProviderArn = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN"
	EnvOidcTokenFile   = "ALIBABA_CLOUD_OIDC_TOKEN_FILE"
)

type Credentials struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

func (c *Credentials) GetAccessKeyID() string {
	return c.AccessKeyId
}

func (c *Credentials) GetAccessKeySecret() string {
	return c.AccessKeySecret
}

func (c *Credentials) GetSecurityToken() string {
	return c.SecurityToken
}

type CredentialsProvider struct {
	cred credentials.Credential
}

func (p CredentialsProvider) GetCredentials() oss.Credentials {
	id, err := p.cred.GetAccessKeyId()
	if err != nil {
		log.Printf("get access key id failed: %+v", err)
		return &Credentials{}
	}
	secret, err := p.cred.GetAccessKeySecret()
	if err != nil {
		log.Printf("get access key secret failed: %+v", err)
		return &Credentials{}
	}
	token, err := p.cred.GetSecurityToken()
	if err != nil {
		log.Printf("get access security token failed: %+v", err)
		return &Credentials{}
	}
	return &Credentials{
		AccessKeyId:     tea.StringValue(id),
		AccessKeySecret: tea.StringValue(secret),
		SecurityToken:   tea.StringValue(token),
	}
}

func NewCredential() (credentials.Credential, error) {
	cred, err := credentials.NewCredential(nil)
	return cred, err
}

// NewOidcCredential
// demo: https://github.com/AliyunContainerService/ack-ram-tool/blob/main/examples/rrsa/oss-go-sdk/main.go
func NewOidcCredential(credentialType string, sessionName string) (credentials.Credential, error) {
	config := new(credentials.Config).
		SetType(credentialType).
		SetRoleArn(os.Getenv(EnvRoleArn)).
		SetOIDCProviderArn(os.Getenv(EnvOidcProviderArn)).
		SetOIDCTokenFilePath(os.Getenv(EnvOidcTokenFile)).
		SetRoleSessionName(sessionName)

	oidcCredential, err := credentials.NewCredential(config)
	return oidcCredential, err
}

// -------

func HTTPHeaderLastModifiedToTime(timeStr string) (time.Time, error) {

	layout := "Mon, 02 Jan 2006 15:04:05 GMT"

	// "Fri, 24 Feb 2012 06:07:48 GMT" => "2012-02-24 06:07:48 +0000 UTC"
	t, err := time.Parse(layout, timeStr)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}
