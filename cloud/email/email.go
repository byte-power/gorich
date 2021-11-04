package email

import (
	"context"

	"github.com/byte-power/gorich/cloud"
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

func GetEmailService(option cloud.Option) (EmailService, error) {
	if option.GetProvider() == cloud.TencentCloudProvider {
		return GetTencentCloudEmailService(option)
	}
	return nil, nil
}
