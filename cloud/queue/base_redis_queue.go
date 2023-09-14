package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/byte-power/gorich/utils"
	"github.com/go-redis/redis/v8"
	"time"

	"github.com/byte-power/gorich/cloud"
)

const (
	DefaultIdle       = 10 * time.Second // 即多长时间后未收到 ACK 的消息被认为是 Pending 状态需要被处理
	DefaultGlobalIdle = 30 * time.Second // 消费者会从全局 Pending 获取超过这个时间的消息
)

var (
	ErrBaseRedisQueueNameEmpty    = errors.New("standalone-redis queue name is empty")
	ErrClusterRedisQueueNameEmpty = errors.New("cluster-redis queue name is empty")
)

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
	client        redis.Cmdable
	queueName     string
	consumerGroup string
	idle          int
	globalIdle    int
}

func GetStandaloneRedisQueueService(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrBaseRedisQueueNameEmpty
	}
	if err := option.CheckStandaloneRedis(); err != nil {
		return nil, err
	}
	queueOption, ok := option.(StandaloneRedisQueueOption)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be BaseRedisQueueOption", option)
	}

	redisOptions := &redis.Options{
		Addr:     queueOption.Addr,
		Password: queueOption.Password,
	}
	applyCustomOptions(redisOptions, queueOption)
	client := redis.NewClient(redisOptions)

	return &BaseRedisQueueService{
		client:        client,
		queueName:     queueName,
		consumerGroup: queueOption.ConsumerGroup,
		idle:          queueOption.Idle,
		globalIdle:    queueOption.GlobalIdle,
	}, nil
}

func GetClusterRedisQueueService(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrClusterRedisQueueNameEmpty
	}
	if err := option.CheckClusterRedis(); err != nil {
		return nil, err
	}
	queueOption, ok := option.(ClusterRedisQueueOption)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be ClusterRedisQueueOption", option)
	}

	redisOptions := &redis.ClusterOptions{
		Addrs:    queueOption.Addrs,
		Password: queueOption.Password,
	}
	applyCustomClusterOptions(redisOptions, queueOption)
	client := redis.NewClusterClient(redisOptions)

	return &BaseRedisQueueService{
		client:        client,
		queueName:     queueName,
		consumerGroup: queueOption.ConsumerGroup,
		idle:          queueOption.Idle,
		globalIdle:    queueOption.GlobalIdle,
	}, nil
}

func (service *BaseRedisQueueService) CreateProducer() (Producer, error) {
	return service, nil
}

func (service *BaseRedisQueueService) IfCreateMkStream(ctx context.Context) error {
	return service.client.XGroupCreateMkStream(ctx, service.queueName, service.consumerGroup, "0").Err()
}

func (service *BaseRedisQueueService) CreateConsumer() (Consumer, error) {
	_ = service.IfCreateMkStream(context.Background()) // 重复创建 XGroup 会报错，忽略
	consumer := &BaseRedisQueueConsumer{
		client:     service.client,
		stream:     service.queueName,
		group:      service.consumerGroup,
		consumer:   fmt.Sprintf("consumer-%s", utils.RandomString(6)),
		idle:       service.idle,
		globalIdle: service.globalIdle,
	}
	return consumer, nil
}

func (service *BaseRedisQueueService) Close() error {
	return nil
}

func (service *BaseRedisQueueService) SendMessage(ctx context.Context, body string) error {
	err := service.client.XAdd(ctx, &redis.XAddArgs{
		Stream: service.queueName,
		Values: map[string]interface{}{"_body_key": body},
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

type BaseRedisQueueConsumer struct {
	client     redis.Cmdable
	stream     string
	group      string
	consumer   string
	idle       int
	globalIdle int
}

// ReceiveMessages 获取待消费消息
//
//	首先获取当前消费者 Pending 状态超过 X 秒的消息
//	而后获取全局所有的 Pending 状态超过 Y 秒的消息
//	最后获取 Stream 队列的消息
func (c *BaseRedisQueueConsumer) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {

	// 先获取当前消费者 Pending 消息
	pendingMessages, err := c.getPendingMessages(ctx, true, maxCount, c.getIdle())
	if err != nil {
		return nil, err
	}
	if len(pendingMessages) > 0 {
		messages, err := xMessagesToMessages(pendingMessages)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}

	// 从全局 Pending 获取超时未处理消息
	pendingMessages, err = c.getPendingMessages(ctx, false, maxCount, c.getGlobalIdle())
	if err != nil {
		return nil, err
	}
	if len(pendingMessages) > 0 {
		messages, err := xMessagesToMessages(pendingMessages)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}

	// 从 Stream 获取消息
	xStreams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
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
			newMsg, err := deepCopyXMessage(xMsg)
			if err != nil {
				return nil, err
			}
			messages = append(messages, &BaseRedisQueueMessage{message: &newMsg})
		}
	}
	return messages, nil
}

func (c *BaseRedisQueueConsumer) getIdle() time.Duration {
	if c.idle <= 0 {
		return DefaultIdle
	}
	return time.Duration(c.idle) * time.Second
}

func (c *BaseRedisQueueConsumer) getGlobalIdle() time.Duration {
	if c.globalIdle <= 0 {
		return DefaultGlobalIdle
	}
	return time.Duration(c.globalIdle) * time.Second
}

// 获取 Pending 消息
func (c *BaseRedisQueueConsumer) getPendingMessages(ctx context.Context, withConsumer bool, count int, idle time.Duration) ([]redis.XMessage, error) {

	// 查询参数
	xPendingExtArgs := &redis.XPendingExtArgs{
		Stream: c.stream,
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Count:  int64(count),
	}
	if withConsumer {
		xPendingExtArgs.Consumer = c.consumer
	}

	// 查询消息
	pendingResults, err := c.client.XPendingExt(ctx, xPendingExtArgs).Result()
	if err != nil {
		return []redis.XMessage{}, err
	}

	pendingIds := make([]string, 0)
	for _, item := range pendingResults {
		if item.Idle <= idle {
			pendingIds = append(pendingIds, item.ID)
		}
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
	xMessages, err := c.client.XClaim(ctx, xClaimArgs).Result()
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
	return c.client.XAck(ctx, queueName, consumerGroup, ackIds...).Err()
}

func (c *BaseRedisQueueConsumer) Close() error {
	return c.client.XGroupDelConsumer(context.Background(),
		c.stream,
		c.group,
		c.consumer,
	).Err()
}

func deepCopyXMessage(m redis.XMessage) (redis.XMessage, error) {
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

func xMessagesToMessages(xMessages []redis.XMessage) ([]Message, error) {
	messages := make([]Message, 0, len(xMessages))
	for _, xMsg := range xMessages {
		newMsg, err := deepCopyXMessage(xMsg)
		if err != nil {
			return nil, err
		}
		messages = append(messages, &BaseRedisQueueMessage{message: &newMsg})
	}
	return messages, nil
}

func applyCustomOptions(redisOptions *redis.Options, queueOption StandaloneRedisQueueOption) {
	if queueOption.MaxRetries != nil {
		redisOptions.MaxRetries = *queueOption.MaxRetries
	}
	if queueOption.PoolSize != nil {
		redisOptions.PoolSize = *queueOption.PoolSize
	}
	if queueOption.DialTimeout != nil {
		redisOptions.DialTimeout = *queueOption.DialTimeout
	}
	if queueOption.ReadTimeout != nil {
		redisOptions.ReadTimeout = *queueOption.ReadTimeout
	}
	if queueOption.WriteTimeout != nil {
		redisOptions.WriteTimeout = *queueOption.WriteTimeout
	}
	if queueOption.MinIdleConns != nil {
		redisOptions.MinIdleConns = *queueOption.MinIdleConns
	}
}

func applyCustomClusterOptions(redisOptions *redis.ClusterOptions, queueOption ClusterRedisQueueOption) {
	if queueOption.MaxRetries != nil {
		redisOptions.MaxRetries = *queueOption.MaxRetries
	}
	if queueOption.PoolSize != nil {
		redisOptions.PoolSize = *queueOption.PoolSize
	}
	if queueOption.DialTimeout != nil {
		redisOptions.DialTimeout = *queueOption.DialTimeout
	}
	if queueOption.ReadTimeout != nil {
		redisOptions.ReadTimeout = *queueOption.ReadTimeout
	}
	if queueOption.WriteTimeout != nil {
		redisOptions.WriteTimeout = *queueOption.WriteTimeout
	}
	if queueOption.MinIdleConns != nil {
		redisOptions.MinIdleConns = *queueOption.MinIdleConns
	}
}
