package email

import (
	"context"

	"github.com/byte-power/gorich/cloud"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	tencentCloudSes "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ses/v20201002"
)

type EmailService interface {
	SendEmail(ctx context.Context, email Email) error
}

type Email struct {
	From         string     `validate:"required"`
	Subject      string     `validate:"required"`
	Body         string     `validate:"required"`
	Style        EmailStyle `validate:"required"`
	Destinations []string   `validate:"required,dive,required"`
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
