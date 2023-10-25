package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/byte-power/gorich/cloud"
)

var (
	ErrTencentCloudQueueServiceTokenEmpty            = errors.New("token for tencentcloud queue service is empty")
	ErrTencentCloudQueueServiceURLEmpty              = errors.New("url for tencentcloud queue service is empty")
	ErrTencentCloudQueueServiceEmptySubscriptionName = errors.New("subscription name for tencentcloud queue service is empty")
	ErrTencentCloudQueueServiceEmptyTopic            = errors.New("topic name for tencentcloud queue service is empty")
)

type TencentCloudQueueOption struct {
	Token string
	URL   string
}

func (option TencentCloudQueueOption) GetProvider() cloud.Provider {
	return cloud.TencentCloudProvider
}

func (option TencentCloudQueueOption) GetSecretID() string {
	return option.Token
}

func (option TencentCloudQueueOption) GetSecretKey() string {
	return option.Token
}

func (option TencentCloudQueueOption) GetAssumeRoleArn() string {
	return ""
}

func (option TencentCloudQueueOption) GetRegion() string {
	return ""
}

func (option TencentCloudQueueOption) GetAssumeRegion() string {
	return ""
}

func (option TencentCloudQueueOption) CheckAWS() error {
	return cloud.ErrProviderNotAWS
}

func (option TencentCloudQueueOption) CheckTencentCloud() error {
	return option.check()
}

func (option TencentCloudQueueOption) CheckStandaloneRedis() error {
	return cloud.ErrProviderNotStandaloneRedis
}

func (option TencentCloudQueueOption) CheckClusterRedis() error {
	return cloud.ErrProviderNotClusterRedis
}

func (option TencentCloudQueueOption) CheckAliCloudStorage() error {
	return cloud.ErrProviderNotAliCloudStorage
}

func (option TencentCloudQueueOption) check() error {
	if option.Token == "" {
		return ErrTencentCloudQueueServiceTokenEmpty
	}
	if option.URL == "" {
		return ErrTencentCloudQueueServiceURLEmpty
	}
	return nil
}

type TencentCloudQueueMessage struct {
	message pulsar.Message
}

func (message *TencentCloudQueueMessage) Body() string {
	return string(message.message.Payload())
}

type TencentCloudQueueService struct {
	client pulsar.Client
	topic  string
	sub    string
}

func GetTencentCloudQueueService(topicSubName string, option cloud.Option) (QueueService, error) {
	topic, sub, err := getTopicAndSubName(topicSubName)
	if err != nil {
		return nil, fmt.Errorf("parameter TopicSubName invalid format %w", err)
	}
	if topic == "" {
		return nil, ErrTencentCloudQueueServiceEmptyTopic
	}
	if sub == "" {
		return nil, ErrTencentCloudQueueServiceEmptySubscriptionName
	}
	if err := option.CheckTencentCloud(); err != nil {
		return nil, err
	}
	queueOption, ok := option.(TencentCloudQueueOption)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be TencentCloudQueueOption", option)
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            queueOption.URL,
		Authentication: pulsar.NewAuthenticationToken(queueOption.Token),
	})
	if err != nil {
		return nil, err
	}
	return &TencentCloudQueueService{client: client, topic: topic, sub: sub}, nil
}

func (service *TencentCloudQueueService) CreateProducer() (Producer, error) {
	producer, err := service.client.CreateProducer(pulsar.ProducerOptions{Topic: service.topic})
	if err != nil {
		return nil, err
	}
	return &TencentCloudQueueProducer{producer: producer}, nil
}

func (service *TencentCloudQueueService) CreateConsumer() (Consumer, error) {
	consumer, err := service.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            service.topic,
		Type:             pulsar.Shared,
		SubscriptionName: service.sub,
	})
	if err != nil {
		return nil, err
	}
	return &TencentCloudQueueConsumer{consumer: consumer}, nil
}

func (service *TencentCloudQueueService) Close() error {
	service.client.Close()
	return nil
}

type TencentCloudQueueProducer struct {
	producer pulsar.Producer
}

func (producer *TencentCloudQueueProducer) SendMessage(ctx context.Context, body string) error {
	_, err := producer.producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte(body)})
	return err
}

func (producer *TencentCloudQueueProducer) Close() error {
	producer.producer.Close()
	return nil
}

type TencentCloudQueueConsumer struct {
	consumer pulsar.Consumer
}

func (consumer *TencentCloudQueueConsumer) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {
	msg, err := consumer.consumer.Receive(ctx)
	if err != nil {
		return nil, err
	}
	message := &TencentCloudQueueMessage{message: msg}
	return []Message{message}, nil
}

func (consumer *TencentCloudQueueConsumer) AckMessage(ctx context.Context, message Message) error {
	msg, ok := message.(*TencentCloudQueueMessage)
	if !ok {
		return errors.New("invalid message type, should be tencentcloud message")
	}
	consumer.consumer.Ack(msg.message)
	return nil
}

func (consumer *TencentCloudQueueConsumer) Close() error {
	consumer.consumer.Close()
	return nil
}

func GenerateTopicAndSubName(topic, subscription string) string {
	return fmt.Sprintf("%s %s", topic, subscription)
}

func getTopicAndSubName(topicAndSub string) (string, string, error) {
	parts := strings.Split(topicAndSub, " ")
	if len(parts) != 2 {
		return "", "", errors.New("parameter is invalid format")
	}
	return parts[0], parts[1], nil
}
