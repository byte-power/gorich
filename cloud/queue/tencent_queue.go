package queue

import (
	"context"

	tdmq "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tdmq/v20200217"
)

type TencentQueueService struct {
	client  *tdmq.Client
	queueID string
}

func (service *TencentQueueService) SendMessage(ctx context.Context, body string) error {
	request := tdmq.NewSendCmqMsgRequest()
	request.QueueName = &service.queueID
	request.MsgContent = &body
	_, err := service.client.SendCmqMsg(request)
	return err
}

func (service *TencentQueueService) ReceiveMessages(ctx context.Context, maxCount int) ([]*Message, error) {
	return nil, nil
}

func (service *TencentQueueService) DeleteMessage(ctx context.Context, message *Message) error {
	return nil
}
