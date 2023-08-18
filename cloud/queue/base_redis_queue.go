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
	DefaultIdle = 10 * time.Second // 即多长时间后未收到 ACK 的消息被认为是 Pending 状态需要被处理
)

var (
	ErrBaseRedisQueueServiceAddrEmpty          = errors.New("addr for base-redis queue service is empty")
	ErrBaseRedisQueueServiceConsumerGroupEmpty = errors.New("consumer group for base-redis queue service is empty")
	ErrBaseRedisQueueServiceConsumerEmpty      = errors.New("consumer for base-redis queue service is empty")
)

type BaseRedisQueueOption struct {
	Addr              string
	Password          string
	ConsumerGroupName string
	ConsumerName      string
	Idle              int
}

func (option BaseRedisQueueOption) GetProvider() cloud.Provider {
	return cloud.BaseRedisProvider
}

func (option BaseRedisQueueOption) GetSecretID() string {
	return ""
}

func (option BaseRedisQueueOption) GetSecretKey() string {
	return option.Password
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
	if option.Addr == "" {
		return ErrBaseRedisQueueServiceAddrEmpty
	}
	if option.ConsumerGroupName == "" {
		return ErrBaseRedisQueueServiceConsumerGroupEmpty
	}
	if option.ConsumerName == "" {
		return ErrBaseRedisQueueServiceConsumerEmpty
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
	return xMsg.Values["_body"].(string)
}

type BaseRedisQueueService struct {
	client        *redis.Client
	queueName     string
	consumerGroup string
	consumerName  string
	Idle          int
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
	rdb := redis.NewClient(&redis.Options{
		Addr:     queueOption.Addr,
		Password: queueOption.Password,
	})
	return &BaseRedisQueueService{client: rdb, queueName: queueName,
		consumerGroup: queueOption.ConsumerGroupName,
		consumerName:  queueOption.ConsumerName,
		Idle:          queueOption.Idle,
	}, nil
}

func (service *BaseRedisQueueService) CreateProducer() (Producer, error) {

	return service, nil
}

func (service *BaseRedisQueueService) IfCreateMkStream(ctx context.Context) error {
	err := service.client.XGroupCreateMkStream(ctx, service.queueName, service.consumerGroup, "0").Err()
	if err != nil && errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func (service *BaseRedisQueueService) CreateConsumer() (Consumer, error) {
	err := service.IfCreateMkStream(context.Background())
	if err != nil {
		return nil, err
	}
	return service, nil
}

func (service *BaseRedisQueueService) Close() error {
	return service.client.Close()
}

func (service *BaseRedisQueueService) SendMessage(ctx context.Context, body string) error {
	err := service.client.XAdd(ctx, &redis.XAddArgs{
		Stream: service.queueName,
		Values: map[string]interface{}{"_body": body},
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

// ReceiveMessages 首先获取 Pending 状态超过 X 秒的消息，未获取到则到 Stream 队列获取
func (service *BaseRedisQueueService) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {

	queueName, consumerGroup, consumerName := service.queueName, service.consumerGroup, service.consumerName

	// 先获取 Pending 消息
	pendingMessages, err := service.getPendingMessages(ctx, queueName, consumerGroup, consumerName, maxCount)
	if err != nil {
		return nil, err
	}

	// 如果有 Pending 消息, 直接返回
	if len(pendingMessages) > 0 {
		messages := make([]Message, 0, len(pendingMessages))
		for _, xMsg := range pendingMessages {
			newMsg, err := DeepCopyXMessage(xMsg)
			if err != nil {
				return nil, err
			}
			messages = append(messages, &BaseRedisQueueMessage{message: &newMsg})
		}
		return messages, nil
	}

	// 从 Stream 获取消息
	xStreams, err := service.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{queueName, ">"},
		Group:    consumerGroup,
		Consumer: consumerName,
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
	return messages, nil
}

func (service *BaseRedisQueueService) getIdle() time.Duration {
	if service.Idle <= 0 {
		return DefaultIdle
	}
	return time.Duration(service.Idle) * time.Second
}

// 获取 Pending 消息
func (service *BaseRedisQueueService) getPendingMessages(ctx context.Context, stream, group, consumer string, count int) ([]redis.XMessage, error) {

	// query pending messages
	pendingResults, err := service.client.XPendingExt(context.TODO(), &redis.XPendingExtArgs{
		Stream:   stream,
		Group:    group,
		Start:    "-",
		End:      "+",
		Idle:     service.getIdle(), // v8 版本 go-redis 库不支持这个参数
		Count:    int64(count),
		Consumer: consumer,
	}).Result()
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

	// get pending message by ids
	xMessages, err := service.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  service.getIdle(),
		Messages: pendingIds,
	}).Result()
	if err != nil {
		return []redis.XMessage{}, err
	}

	return xMessages, nil
}

func (service *BaseRedisQueueService) AckMessage(ctx context.Context, message Message) error {
	redisMessage, ok := message.(*BaseRedisQueueMessage)
	if !ok {
		return errors.New("invalid message type, should be base-redis message")
	}
	ackIds := []string{redisMessage.message.ID}
	queueName, consumerGroup := service.queueName, service.consumerGroup
	return service.client.XAck(ctx, queueName, consumerGroup, ackIds...).Err()
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
