package email

import (
	"encoding/base64"
	"fmt"

	ses "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ses/v20201002"
)

type TencentCloudEmail struct {
	client *ses.Client
}

func (service *TencentCloudEmail) SendEmail(from, subject, body string, style EmailStyle, destinations ...string) error {
	emailBody := &ses.Simple{}
	b64Body := base64.StdEncoding.EncodeToString([]byte(body))
	if style == EmailStyleHTML {
		emailBody.Html = &b64Body
	} else if style == EmailStyleText {
		emailBody.Text = &b64Body
	} else {
		return fmt.Errorf("invalid body style %s, supported styles are %s, %s", style, EmailStyleHTML, EmailStyleText)
	}
	request := ses.NewSendEmailRequest()
	request.FromEmailAddress = &from
	request.Subject = &subject
	request.Simple = emailBody
	for _, dest := range destinations {
		request.Destination = append(request.Destination, &dest)
	}
	_, err := service.client.SendEmail(request)
	return err
}
