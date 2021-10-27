package email

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/go-playground/validator/v10"
	ses "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ses/v20201002"
)

type TencentCloudEmail struct {
	client *ses.Client
}

func (service *TencentCloudEmail) SendEmail(ctx context.Context, email Email) error {
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
