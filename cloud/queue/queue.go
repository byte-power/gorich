package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/byte-power/gorich/cloud"
)

type QueueService interface {
	CreateProducer() (Producer, error)
	CreateConsumer() (Consumer, error)
	Close() error
}

type Producer interface {
	SendMessage(ctx context.Context, body string) error
	Close() error
}

type Consumer interface {
	ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error)
	AckMessage(ctx context.Context, message Message) error
	Close() error
}

type Message interface {
	Body() string
}

func GetQueueService(queueOrTopicSubName string, option cloud.Option) (QueueService, error) {
	if queueOrTopicSubName == "" {
		return nil, errors.New("queue or topic_sub name should not be empty")
	}
	if err := option.Check(); err != nil {
		return nil, err
	}
	if option.GetProvider() == cloud.TencentCloudProvider {
		topic, sub, err := getTopicAndSubName(queueOrTopicSubName)
		if err != nil {
			return nil, fmt.Errorf("parameter queueOrTopicSubName invalid format %w", err)
		}
		if topic == "" {
			return nil, ErrTencentQueueServiceEmptyTopic
		}
		if sub == "" {
			return nil, ErrTencentQueueServiceEmptySubscriptionName
		}
		queueOption, ok := option.(TencentQueueOption)
		if !ok {
			return nil, fmt.Errorf("parameter option %+v should be TencentQueueOption", option)
		}
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:            queueOption.URL,
			Authentication: pulsar.NewAuthenticationToken(queueOption.Token),
		})
		if err != nil {
			return nil, err
		}
		return &TencentQueueService{client: client, topic: topic, sub: sub}, nil
	} else if option.GetProvider() == cloud.AWSProvider {
		session, err := session.NewSession(&aws.Config{
			Region:      aws.String(option.GetRegion()),
			Credentials: credentials.NewStaticCredentials(option.GetSecretID(), option.GetSecretKey(), ""),
		})
		if err != nil {
			return nil, err
		}
		client := sqs.New(session)
		input := &sqs.GetQueueUrlInput{QueueName: aws.String(queueOrTopicSubName)}
		output, err := client.GetQueueUrl(input)
		if err != nil {
			return nil, err
		}
		return &AWSQueueService{client: client, queueURL: aws.StringValue(output.QueueUrl)}, nil
	}
	return nil, nil
}
