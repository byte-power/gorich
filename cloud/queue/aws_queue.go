package queue

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type AWSQueueService struct {
	client   *sqs.SQS
	queueURL string
}

func (service *AWSQueueService) SendMessage(ctx context.Context, body string) error {
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(service.queueURL),
		MessageBody: aws.String(body),
	}
	_, err := service.client.SendMessageWithContext(ctx, input)
	return err
}

func (service *AWSQueueService) ReceiveMessages(ctx context.Context, maxCount int) ([]*Message, error) {
	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(maxCount)),
		QueueUrl:            aws.String(service.queueURL),
	}
	resp, err := service.client.ReceiveMessage(input)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	messages := make([]*Message, 0, len(resp.Messages))
	for _, message := range resp.Messages {
		messages = append(messages, &Message{
			body:    aws.StringValue(message.Body),
			id:      aws.StringValue(message.MessageId),
			handler: aws.StringValue(message.ReceiptHandle),
		})
	}
	return messages, nil
}

func (service *AWSQueueService) DeleteMessage(ctx context.Context, message *Message) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(service.queueURL),
		ReceiptHandle: aws.String(message.handler),
	}
	_, err := service.client.DeleteMessage(input)
	return err
}
