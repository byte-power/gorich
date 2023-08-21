package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/byte-power/gorich/cloud"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultIdle       = 10 * time.Second // 即多长时间后未收到 ACK 的消息被认为是 Pending 状态需要被处理
	DefaultGlobalIdle = 30 * time.Second // 消费者会从全局 Pending 获取超过这个时间的消息
)

var (
	ErrBaseRedisQueueClientEmpty        = errors.New("base-redis queue client is empty")
	ErrBaseRedisQueueConsumerGroupEmpty = errors.New("consumer group for base-redis queue service is empty")
)

type RedisService interface {
	Cmdable() redis.Cmdable
}

type BaseRedisQueueOption struct {
	Client            RedisService
	ConsumerGroupName string
	Idle              int
}

func (option BaseRedisQueueOption) GetProvider() cloud.Provider {
	return cloud.BaseRedisProvider
}

func (option BaseRedisQueueOption) GetSecretID() string {
	return ""
}

func (option BaseRedisQueueOption) GetSecretKey() string {
	return ""
}

func (option BaseRedisQueueOption) GetAssumeRoleArn() string {
	return ""
}

func (option BaseRedisQueueOption) GetRegion() string {
	return ""
}

func (option BaseRedisQueueOption) GetAssumeRegion() string {
	return ""
}

func (option BaseRedisQueueOption) CheckAWS() error {
	return cloud.ErrProviderNotAWS
}

func (option BaseRedisQueueOption) CheckTencentCloud() error {
	return cloud.ErrProviderNotTencentCloud
}

func (option BaseRedisQueueOption) CheckBaseRedis() error {
	return option.check()
}

func (option BaseRedisQueueOption) check() error {
	if option.Client == nil {
		return ErrBaseRedisQueueClientEmpty
	}
	if option.ConsumerGroupName == "" {
		return ErrBaseRedisQueueConsumerGroupEmpty
	}
	return nil
}

type BaseRedisQueueMessage struct {
	message *redis.XMessage
}

func (message *BaseRedisQueueMessage) Body() string {
	if message.message == nil {
		return ""
	}
	xMsg := *message.message
	return xMsg.Values["_body_key"].(string)
}

type BaseRedisQueueService struct {
	client        RedisService
	queueName     string
	consumerGroup string
	idle          int

	consumerSerial int
}

var ErrBaseRedisQueueNameEmpty = errors.New("base-redis queue name is empty")

func GetBaseRedisQueueService(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrBaseRedisQueueNameEmpty
	}
	if err := option.CheckBaseRedis(); err != nil {
		return nil, err
	}
	queueOption, ok := option.(BaseRedisQueueOption)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be BaseRedisQueueOption", option)
	}

	return &BaseRedisQueueService{
		client:        queueOption.Client,
		queueName:     queueName,
		consumerGroup: queueOption.ConsumerGroupName,
		idle:          queueOption.Idle,
	}, nil
}

func (service *BaseRedisQueueService) CreateProducer() (Producer, error) {
	return service, nil
}

func (service *BaseRedisQueueService) IfCreateMkStream(ctx context.Context) error {
	return service.client.Cmdable().XGroupCreateMkStream(ctx, service.queueName, service.consumerGroup, "0").Err()
}

func (service *BaseRedisQueueService) CreateConsumer() (Consumer, error) {
	_ = service.IfCreateMkStream(context.Background()) // 重复创建 XGroup 会报错，忽略
	consumer := &BaseRedisQueueConsumer{
		client:   service.client,
		stream:   service.queueName,
		group:    service.consumerGroup,
		consumer: fmt.Sprintf("consumer-%d", service.consumerSerial),
		idle:     service.idle,
	}
	service.consumerSerial += 1
	return consumer, nil
}

func (service *BaseRedisQueueService) Close() error {
	return nil
}

func (service *BaseRedisQueueService) SendMessage(ctx context.Context, body string) error {
	err := service.client.Cmdable().XAdd(ctx, &redis.XAddArgs{
		Stream: service.queueName,
		Values: map[string]interface{}{"_body_key": body},
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

type BaseRedisQueueConsumer struct {
	client   RedisService
	stream   string
	group    string
	consumer string
	idle     int
}

// ReceiveMessages 首先获取 Pending 状态超过 X 秒的消息，未获取到则到 Stream 队列获取
func (c *BaseRedisQueueConsumer) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {

	// 先获取当前消费者 Pending 消息
	pendingMessages, err := c.getPendingMessages(ctx, true, maxCount, c.getIdle())
	if err != nil {
		return nil, err
	}
	if len(pendingMessages) > 0 {
		messages, err := XMessagesToMessages(pendingMessages)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}

	// 从 Stream 获取消息
	xStreams, err := c.client.Cmdable().XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{c.stream, ">"},
		Group:    c.group,
		Consumer: c.consumer,
		Block:    time.Second,
		Count:    int64(maxCount),
	}).Result()
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0)
	for _, xStream := range xStreams {
		for _, xMsg := range xStream.Messages {
			newMsg, err := DeepCopyXMessage(xMsg)
			if err != nil {
				return nil, err
			}
			messages = append(messages, &BaseRedisQueueMessage{message: &newMsg})
		}
	}
	if len(messages) != 0 {
		return messages, nil
	}

	// 从全局 Pending 获取超时未处理消息
	pendingMessages, err = c.getPendingMessages(ctx, false, maxCount, DefaultGlobalIdle)
	if err != nil {
		return nil, err
	}
	if len(pendingMessages) > 0 {
		messages, err = XMessagesToMessages(pendingMessages)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}

	return []Message{}, nil
}

func (c *BaseRedisQueueConsumer) getIdle() time.Duration {
	if c.idle <= 0 {
		return DefaultIdle
	}
	return time.Duration(c.idle) * time.Second
}

// 获取 Pending 消息
func (c *BaseRedisQueueConsumer) getPendingMessages(ctx context.Context, withConsumer bool, count int, idle time.Duration) ([]redis.XMessage, error) {

	// 查询参数
	xPendingExtArgs := &redis.XPendingExtArgs{
		Stream: c.stream,
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Idle:   idle, // v8 版本 go-redis 库不支持这个参数
		Count:  int64(count),
	}
	if withConsumer {
		xPendingExtArgs.Consumer = c.consumer
	}

	// 查询消息
	pendingResults, err := c.client.Cmdable().XPendingExt(ctx, xPendingExtArgs).Result()
	if err != nil {
		return []redis.XMessage{}, err
	}

	pendingIds := make([]string, 0)
	for _, v := range pendingResults {
		pendingIds = append(pendingIds, v.ID)
	}
	if len(pendingIds) == 0 {
		return []redis.XMessage{}, nil
	}

	// 获取参数
	xClaimArgs := &redis.XClaimArgs{
		Stream:   c.stream,
		Group:    c.group,
		MinIdle:  idle,
		Messages: pendingIds,
	}
	if withConsumer {
		xClaimArgs.Consumer = c.consumer
	}

	// 获取消息
	xMessages, err := c.client.Cmdable().XClaim(ctx, xClaimArgs).Result()
	if err != nil {
		return []redis.XMessage{}, err
	}

	return xMessages, nil
}

func (c *BaseRedisQueueConsumer) AckMessage(ctx context.Context, message Message) error {
	redisMessage, ok := message.(*BaseRedisQueueMessage)
	if !ok {
		return errors.New("invalid message type, should be base-redis message")
	}
	ackIds := []string{redisMessage.message.ID}
	queueName, consumerGroup := c.stream, c.group
	return c.client.Cmdable().XAck(ctx, queueName, consumerGroup, ackIds...).Err()
}

func (c *BaseRedisQueueConsumer) Close() error {
	return c.client.Cmdable().XGroupDelConsumer(context.Background(),
		c.stream,
		c.group,
		c.consumer,
	).Err()
}

func DeepCopyXMessage(m redis.XMessage) (redis.XMessage, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return redis.XMessage{}, err
	}
	var c redis.XMessage
	err = json.Unmarshal(b, &c)
	if err != nil {
		return redis.XMessage{}, err
	}
	return c, nil
}

func XMessagesToMessages(xMessages []redis.XMessage) ([]Message, error) {
	messages := make([]Message, 0, len(xMessages))
	for _, xMsg := range xMessages {
		newMsg, err := DeepCopyXMessage(xMsg)
		if err != nil {
			return nil, err
		}
		messages = append(messages, &BaseRedisQueueMessage{message: &newMsg})
	}
	return messages, nil
}
