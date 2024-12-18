package main

import (
	"context"
	"fmt"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/byte-power/gorich/cloud/queue"
)

// Configure Addr/Addrs, Password to run standalone/cluster redis example.
// Configure token, url, topic_name and subscription_name to run tencentcloud example.
// Configure secret_id, secret_key, region, and queue_name to run this example.
func main() {

	option := queue.QueueOption{
		Provider:  cloud.ClusterRedisProvider,
		QueueName: "test_queue_name",
		ClusterRedis: queue.ClusterRedisQueueOption{
			Addrs: []string{
				"localhost:7000",
				"localhost:7001",
				"localhost:7002",
				"localhost:7003",
				"localhost:7004",
				"localhost:7005",
			},
			Password:      "",
			ConsumerGroup: "save_task_consumer_group",
			Idle:          10,
		},
	}
	queue_examples(option)

	topicName := "pulsar-xxxxxx/namespace_name/topic_name"
	subscriptionName := "subscription_name"
	topicSub := queue.GenerateTopicAndSubName(topicName, subscriptionName)
	option = queue.QueueOption{
		Provider:  cloud.TencentCloudProvider,
		QueueName: topicSub,
		Pulsar: queue.TencentCloudQueueOption{
			Token: "access_jwt_token_xxx",
			URL:   "http://pulsar-xxxxxxxxx.tdmq.ap-gz.public.tencenttdmq.com:8080",
		},
	}
	queue_examples(option)

	option = queue.QueueOption{
		Provider:  cloud.AWSProvider,
		QueueName: "aws_queue_name",
		SQS: cloud.AWSOption{
			SecretID:  "aws_secret_id_xxxx",
			SecretKey: "aws_secret_key_xxxx",
			Region:    "aws_region_xxx",
		},
	}

	queue_examples(option)

	dialTimeout := 5 * time.Second
	option = queue.QueueOption{
		Provider:  cloud.ClusterRedisProviderV7,
		QueueName: "redis_cluster_queue_v7",
		ClusterRedisV7: queue.ClusterRedisQueueOptionV7{
			ClusterRedisQueueOption: queue.ClusterRedisQueueOption{
				Addrs: []string{
					"localhost:30001",
					"localhost:30002",
					"localhost:30003",
				},
				ConsumerGroup: "save_task_consumer_group_2",
				DialTimeout:   &dialTimeout,
				Idle:          10,
			},
		},
	}
	queue_examples(option)

	// alicloud MNS queue: access_key CredentialType
	option = queue.QueueOption{
		Provider:  cloud.AliCloudProvider,
		QueueName: "mns_queue_name",
		MNS: queue.AliMNSClientOption{
			EndPoint:        "http://account-id.mns.region.aliyuncs.com",
			CredentialType:  cloud.AliCloudAccessKeyCredentialType,
			AccessKeyId:     "alicloud_access_key_id",
			AccessKeySecret: "alicloud_access_key_secret",
			MessagePriority: 10,
		},
	}
	queue_examples(option)

	// alicloud MNS queue: ecs_ram_role credentialType
	option = queue.QueueOption{
		Provider:  cloud.AliCloudProvider,
		QueueName: "mns_queue_name",
		MNS: queue.AliMNSClientOption{
			EndPoint:       "http://account-id.mns.region.aliyuncs.com",
			CredentialType: cloud.AliCloudECSRamRoleCredentialType,
		}}
	queue_examples(option)
	alicloud_mns_queue_examples(option)
}

func queue_examples(option queue.QueueOption) {
	service, err := queue.GetQueueServiceWithOption(option)
	if err != nil {
		fmt.Printf("get queue service error %s %+v %s\n", option.QueueName, option, err)
		return
	}
	defer service.Close()
	fmt.Printf("get service %+v\n", service)

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

// The following examples show mns queue specific examples: How to set message priority; how to set long polling period seconds.
func alicloud_mns_queue_examples(option queue.QueueOption) {
	service, err := queue.GetQueueServiceWithOption(option)
	if err != nil {
		fmt.Printf("get queue service error %s %+v %s\n", option.QueueName, option, err)
		return
	}
	defer service.Close()
	fmt.Printf("get service %+v\n", service)

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
		// rewrite message priority
		ctx := context.WithValue(context.TODO(), queue.ContextKeyAliMNSMessagePriority, 5)
		err = producer.SendMessage(ctx, message)
		if err != nil {
			fmt.Printf("producer send message error %s", err)
			return
		}
		fmt.Printf("producer send message %s\n", message)
	}
	// rewrite long polling period
	ctx := context.WithValue(context.TODO(), queue.ContextKeyAliMNSLongPollingWaitSeconds, 10)
	receivedMsgs, err := consumer.ReceiveMessages(ctx, 10)
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
