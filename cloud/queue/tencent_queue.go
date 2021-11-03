package queue

import (
	"context"
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
)

type TencentQueueOption struct {
	secretToken string
	url         string
}

func NewTencentQueueOptions(token, url string) (*TencentQueueOption, error) {
	if token == "" {
		return nil, errors.New("token is empty")
	}
	if url == "" {
		return nil, errors.New("url is empty")
	}
	option := &TencentQueueOption{
		secretToken: token,
		url:         url,
	}
	return option, nil
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

func (service *TencentQueueService) GetProducer() (Producer, error) {
	producer, err := service.client.CreateProducer(pulsar.ProducerOptions{Topic: service.topic})
	if err != nil {
		return nil, err
	}
	return &TencentQueueProducer{producer: producer}, nil
}

func (service *TencentQueueService) GetConsumer() (Consumer, error) {
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
