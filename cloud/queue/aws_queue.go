package queue

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
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

func (service *AWSQueueService) GetProducer() (Producer, error) {
	return service, nil
}

func (service *AWSQueueService) GetConsumer() (Consumer, error) {
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
