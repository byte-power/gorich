package main

import (
	"context"
	"fmt"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/byte-power/gorich/cloud/queue"
)

func main() {
	optionForTencentCloud := queue.TencentQueueOption{
		Token: "access_jwt_token_xxx",
		URL:   "http://pulsar-xxxxxxxxx.tdmq.ap-gz.public.tencenttdmq.com:8080",
	}
	topic := "pulsar-xxxxxx/namespace_name/topic_name"
	sub := "subscription_name"
	topicSub := queue.GenerateTopicAndSubName(topic, sub)
	queue_examples(topicSub, optionForTencentCloud)

	optionForAWS := cloud.CommonOption{
		Provider:  cloud.AWSProvider,
		SecretID:  "aws_secret_id_xxxx",
		SecretKey: "aws_secret_key_xxxx",
		Region:    "aws_region_xxx",
	}
	queue_examples("aws_queue_name", optionForAWS)
}

func queue_examples(queueOrTopicName string, option cloud.Option) {
	service, err := queue.GetQueueService(queueOrTopicName, option)
	if err != nil {
		fmt.Printf("get queue service error %s %+v %s\n", queueOrTopicName, option, err)
		return
	}
	defer service.Close()

	producer, err := service.CreateProducer()
	if err != nil {
		fmt.Printf("create producer error %s\n", err)
		return
	}
	defer producer.Close()

	consumer, err := service.CreateConsumer()
	if err != nil {
		fmt.Printf("create consumer error %s\n", err)
		return
	}
	defer consumer.Close()

	ts := int(time.Now().Unix())
	var messages []string
	for i := 0; i < 3; i++ {
		messages = append(messages, fmt.Sprintf("message %d", ts+i))
	}
	for _, message := range messages {
		err = producer.SendMessage(context.TODO(), message)
		if err != nil {
			fmt.Printf("producer send message error %s", err)
			return
		}
		fmt.Printf("producer send message %s\n", message)
	}
	receivedMsgs, err := consumer.ReceiveMessages(context.TODO(), 10)
	if err != nil {
		fmt.Printf("receive messages error %s", err)
		return
	}
	for _, message := range receivedMsgs {
		fmt.Printf("received message %s\n", message.Body())
	}
	for _, message := range receivedMsgs {
		err := consumer.AckMessage(context.TODO(), message)
		if err != nil {
			fmt.Printf("ack message error %s %s\n", message.Body(), err)
			return
		}
		fmt.Printf("ack message %s\n", message.Body())
	}
}
