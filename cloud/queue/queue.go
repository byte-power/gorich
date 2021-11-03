package queue

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/byte-power/gorich/cloud"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	tdmq "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tdmq/v20200217"
)

type QueueService interface {
	SendMessage(ctx context.Context, body string) error

	ReceiveMessages(ctx context.Context, maxCount int) ([]*Message, error)

	DeleteMessage(ctx context.Context, message *Message) error
}

type Message struct {
	body    string
	id      string
	handler string
}

func GetQueueService(queueID string, options cloud.Options) (QueueService, error) {
	if queueID == "" {
		return nil, errors.New("queue url should not be empty")
	}
	if err := options.Check(); err != nil {
		return nil, err
	}
	if options.Provider == cloud.TencentCloudProvider {
		credential := common.NewCredential(options.SecretID, options.SecretKey)
		cpf := profile.NewClientProfile()
		client, err := tdmq.NewClient(credential, options.Region, cpf)
		if err != nil {
			return nil, err
		}
		return &TencentQueueService{client: client, queueID: queueID}, nil
	} else if options.Provider == cloud.AWSProvider {
		session, err := session.NewSession(&aws.Config{
			Region:      aws.String(options.Region),
			Credentials: credentials.NewStaticCredentials(options.SecretID, options.SecretKey, ""),
		})
		if err != nil {
			return nil, err
		}
		client := sqs.New(session)
		input := &sqs.GetQueueUrlInput{QueueName: aws.String(queueID)}
		output, err := client.GetQueueUrl(input)
		if err != nil {
			return nil, err
		}
		return &AWSQueueService{client: client, queueURL: aws.StringValue(output.QueueUrl)}, nil
	}
	return nil, nil
}
