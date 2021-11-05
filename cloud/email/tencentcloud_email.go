package email

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/byte-power/gorich/cloud"
	"github.com/go-playground/validator/v10"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	ses "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ses/v20201002"
)

type TencentCloudEmailService struct {
	client *ses.Client
}

const tencentCloudSESSupportedRegion = "ap-hongkong"

func GetTencentCloudEmailService(option cloud.Option) (EmailService, error) {
	if err := option.CheckTencentCloud(); err != nil {
		return nil, err
	}
	credential := common.NewCredential(option.GetSecretID(), option.GetSecretKey())
	cpf := profile.NewClientProfile()
	client, err := ses.NewClient(credential, tencentCloudSESSupportedRegion, cpf)
	if err != nil {
		return nil, err
	}
	return &TencentCloudEmailService{client: client}, nil
}

func (service *TencentCloudEmailService) SendEmail(ctx context.Context, email Email) error {
	if err := validator.New().Struct(email); err != nil {
		return err
	}
	emailBody := &ses.Simple{}
	b64Body := base64.StdEncoding.EncodeToString([]byte(email.Body))
	if email.Style == EmailStyleHTML {
		emailBody.Html = &b64Body
	} else if email.Style == EmailStyleText {
		emailBody.Text = &b64Body
	} else {
		return fmt.Errorf("invalid body style %s, supported styles are %s, %s", email.Style, EmailStyleHTML, EmailStyleText)
	}
	request := ses.NewSendEmailRequest()
	request.FromEmailAddress = &email.From
	request.Subject = &email.Subject
	request.Simple = emailBody
	for _, dest := range email.Destinations {
		request.Destination = append(request.Destination, &dest)
	}
	_, err := service.client.SendEmail(request)
	return err
}
