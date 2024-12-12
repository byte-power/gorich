package queue

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/byte-power/gorich/cloud"
)

type AWSQueueMessage struct {
	message *sqs.Message
}

func (message *AWSQueueMessage) Body() string {
	return aws.StringValue(message.message.Body)
}

type AWSQueueService struct {
	client   *sqs.SQS
	queueURL string
}

var ErrAWSQueueNameEmpty = errors.New("aws queue name is empty")

func getAWSQueueService(queueName string, option cloud.AWSOption) (QueueService, error) {
	if queueName == "" {
		return nil, ErrAWSQueueNameEmpty
	}
	if err := option.Check(); err != nil {
		return nil, err
	}

	sess, cfg, err := cloud.AwsNewSessionWithOption(option)
	if err != nil {
		return nil, err
	}
	var client *sqs.SQS
	// Assume the specified role
	if cfg != nil {
		client = sqs.New(sess, cfg)
	} else {
		client = sqs.New(sess)
	}

	input := &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)}
	output, err := client.GetQueueUrl(input)
	if err != nil {
		return nil, err
	}
	return &AWSQueueService{client: client, queueURL: aws.StringValue(output.QueueUrl)}, nil
}

// GetAWSQueueService is deprecated, use getAWSQueueService instead.
func GetAWSQueueService(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrAWSQueueNameEmpty
	}
	if err := option.CheckAWS(); err != nil {
		return nil, err
	}

	sess, cfg, err := cloud.AwsNewSession(option)
	if err != nil {
		return nil, err
	}
	var client *sqs.SQS
	// Assume the specified role
	if cfg != nil {
		client = sqs.New(sess, cfg)
	} else {
		client = sqs.New(sess)
	}

	input := &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)}
	output, err := client.GetQueueUrl(input)
	if err != nil {
		return nil, err
	}
	return &AWSQueueService{client: client, queueURL: aws.StringValue(output.QueueUrl)}, nil
}

func (service *AWSQueueService) CreateProducer() (Producer, error) {
	return service, nil
}

func (service *AWSQueueService) CreateConsumer() (Consumer, error) {
	return service, nil
}

func (service *AWSQueueService) Close() error {
	return nil
}

func (service *AWSQueueService) SendMessage(ctx context.Context, body string) error {
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(service.queueURL),
		MessageBody: aws.String(body),
	}
	_, err := service.client.SendMessageWithContext(ctx, input)
	return err
}

func (service *AWSQueueService) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {
	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(maxCount)),
		QueueUrl:            aws.String(service.queueURL),
	}
	resp, err := service.client.ReceiveMessage(input)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return make([]Message, 0), nil
	}
	messages := make([]Message, 0, len(resp.Messages))
	for _, message := range resp.Messages {
		messages = append(messages, &AWSQueueMessage{message: message})
	}
	return messages, nil
}

func (service *AWSQueueService) AckMessage(ctx context.Context, message Message) error {
	msg, ok := message.(*AWSQueueMessage)
	if !ok {
		return errors.New("invalid message type, should be AWS message")
	}
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(service.queueURL),
		ReceiptHandle: msg.message.ReceiptHandle,
	}
	_, err := service.client.DeleteMessage(input)
	return err
}
