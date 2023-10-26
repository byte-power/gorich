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
	AliCloudEnvRoleArn         = "ALIBABA_CLOUD_ROLE_ARN"
	AliCloudEnvOidcProviderArn = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN"
	AliCloudEnvOidcTokenFile   = "ALIBABA_CLOUD_OIDC_TOKEN_FILE"
)

type aliCloudCredentials struct {
	accessKeyId     string
	accessKeySecret string
	securityToken   string
}

func (c *aliCloudCredentials) GetAccessKeyID() string {
	return c.accessKeyId
}

func (c *aliCloudCredentials) GetAccessKeySecret() string {
	return c.accessKeySecret
}

func (c *aliCloudCredentials) GetSecurityToken() string {
	return c.securityToken
}

type aliCloudCredentialsProvider struct {
	cred credentials.Credential
}

func (p aliCloudCredentialsProvider) GetCredentials() oss.Credentials {
	id, err := p.cred.GetAccessKeyId()
	if err != nil {
		log.Printf("get access key id failed: %+v", err)
		return &aliCloudCredentials{}
	}
	secret, err := p.cred.GetAccessKeySecret()
	if err != nil {
		log.Printf("get access key secret failed: %+v", err)
		return &aliCloudCredentials{}
	}
	token, err := p.cred.GetSecurityToken()
	if err != nil {
		log.Printf("get access security token failed: %+v", err)
		return &aliCloudCredentials{}
	}
	return &aliCloudCredentials{
		accessKeyId:     tea.StringValue(id),
		accessKeySecret: tea.StringValue(secret),
		securityToken:   tea.StringValue(token),
	}
}

func newCredential() (credentials.Credential, error) {
	cred, err := credentials.NewCredential(nil)
	return cred, err
}

// newOidcCredential
// demo: https://github.com/AliyunContainerService/ack-ram-tool/blob/main/examples/rrsa/oss-go-sdk/main.go
func newOidcCredential(credentialType string, sessionName string) (credentials.Credential, error) {
	config := new(credentials.Config).
		SetType(credentialType).
		SetRoleArn(os.Getenv(AliCloudEnvRoleArn)).
		SetOIDCProviderArn(os.Getenv(AliCloudEnvOidcProviderArn)).
		SetOIDCTokenFilePath(os.Getenv(AliCloudEnvOidcTokenFile)).
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
