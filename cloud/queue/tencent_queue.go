package queue

import (
	"context"
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/byte-power/gorich/cloud"
)

var (
	ErrTencentQueueServiceTokenEmpty = errors.New("token for tencent queue service is empty")
	ErrTencentQueueServiceURLEmpty   = errors.New("url for tencent queue service is empty")
)

type TencentQueueOption struct {
	Token string
	URL   string
}

func (option TencentQueueOption) GetProvider() cloud.Provider {
	return cloud.TencentCloudProvider
}

func (option TencentQueueOption) GetSecretID() string {
	return option.Token
}

func (option TencentQueueOption) GetSecretKey() string {
	return option.Token
}

func (option TencentQueueOption) GetRegion() string {
	return ""
}

func (option TencentQueueOption) Check() error {
	if option.Token == "" {
		return ErrTencentQueueServiceTokenEmpty
	}
	if option.URL == "" {
		return ErrTencentQueueServiceURLEmpty
	}
	return nil
}

type TencentQueueMessage struct {
	message pulsar.Message
}

func (message *TencentQueueMessage) Body() string {
	return string(message.message.Payload())
}

type TencentQueueService struct {
	client pulsar.Client
	topic  string
}

func (service *TencentQueueService) CreateProducer() (Producer, error) {
	producer, err := service.client.CreateProducer(pulsar.ProducerOptions{Topic: service.topic})
	if err != nil {
		return nil, err
	}
	return &TencentQueueProducer{producer: producer}, nil
}

func (service *TencentQueueService) CreateConsumer() (Consumer, error) {
	consumer, err := service.client.Subscribe(pulsar.ConsumerOptions{
		Topic: service.topic,
		Type:  pulsar.Shared,
	})
	if err != nil {
		return nil, err
	}
	return &TencentQueueConsumer{consumer: consumer}, nil
}

func (service *TencentQueueService) Close() error {
	service.client.Close()
	return nil
}

type TencentQueueProducer struct {
	producer pulsar.Producer
}

func (producer *TencentQueueProducer) SendMessage(ctx context.Context, body string) error {
	_, err := producer.producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte(body)})
	return err
}

func (producer *TencentQueueProducer) Close() error {
	producer.producer.Close()
	return nil
}

type TencentQueueConsumer struct {
	consumer pulsar.Consumer
}

func (consumer *TencentQueueConsumer) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {
	msg, err := consumer.consumer.Receive(ctx)
	if err != nil {
		return nil, err
	}
	message := &TencentQueueMessage{message: msg}
	return []Message{message}, nil
}

func (consumer *TencentQueueConsumer) AckMessage(ctx context.Context, message Message) error {
	msg, ok := message.(*TencentQueueMessage)
	if !ok {
		return errors.New("invalid message type, should be tencent cloud message")
	}
	consumer.consumer.Ack(msg.message)
	return nil
}

func (consumer *TencentQueueConsumer) Close() error {
	consumer.consumer.Close()
	return nil
}
