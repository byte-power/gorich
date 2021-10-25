package email

import (
	"github.com/byte-power/gorich/cloud"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	tencentCloudSes "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ses/v20201002"
)

type EmailService interface {
	SendEmail(from, subject, body string, style EmailStyle, destinations ...string) error
}

const tencentCloudSESSupportedRegion = "ap-hongkong"

type EmailStyle string

const (
	EmailStyleHTML EmailStyle = "html"
	EmailStyleText EmailStyle = "text"
)

func NewEmailService(options cloud.Options) (EmailService, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if options.Provider == cloud.TencentCloudProvider {
		credential := common.NewCredential(options.SecretID, options.SecretKey)
		cpf := profile.NewClientProfile()
		client, err := tencentCloudSes.NewClient(credential, tencentCloudSESSupportedRegion, cpf)
		if err != nil {
			return nil, err
		}
		return &TencentCloudEmail{client: client}, nil
	}
	return nil, nil
}
