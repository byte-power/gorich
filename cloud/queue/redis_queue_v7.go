package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/byte-power/gorich/utils"
	"github.com/redis/go-redis/v9"

	"github.com/byte-power/gorich/cloud"
)

type RedisQueueMessageV7 struct {
	message *redis.XMessage
}

func (message *RedisQueueMessageV7) Body() string {
	if message.message == nil {
		return ""
	}
	xMsg := *message.message
	return xMsg.Values["_body_key"].(string)
}

type RedisQueueServiceV7 struct {
	client        redis.Cmdable
	queueName     string
	consumerGroup string
	idle          int
	globalIdle    int
}

func getStandaloneRedisQueueServiceForV7(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrBaseRedisQueueNameEmpty
	}
	if err := option.CheckStandaloneRedis(); err != nil {
		return nil, err
	}
	queueOption, ok := option.(StandaloneRedisQueueOptionV7)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be BaseRedisQueueOption", option)
	}

	redisOptions := &redis.Options{
		Addr:     queueOption.Addr,
		Password: queueOption.Password,
	}
	applyCustomOptionsV7(redisOptions, queueOption)
	client := redis.NewClient(redisOptions)

	return &RedisQueueServiceV7{
		client:        client,
		queueName:     queueName,
		consumerGroup: queueOption.ConsumerGroup,
		idle:          queueOption.Idle,
		globalIdle:    queueOption.GlobalIdle,
	}, nil
}

func getClusterRedisQueueServiceV7(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrClusterRedisQueueNameEmpty
	}
	if err := option.CheckClusterRedis(); err != nil {
		return nil, err
	}
	queueOption, ok := option.(ClusterRedisQueueOptionV7)
	if !ok {
		return nil, fmt.Errorf("parameter option %+v should be ClusterRedisQueueOptionV7", option)
	}

	redisOptions := &redis.ClusterOptions{
		Addrs:    queueOption.Addrs,
		Password: queueOption.Password,
	}
	applyCustomClusterOptionsV7(redisOptions, queueOption)
	client := redis.NewClusterClient(redisOptions)

	return &RedisQueueServiceV7{
		client:        client,
		queueName:     queueName,
		consumerGroup: queueOption.ConsumerGroup,
		idle:          queueOption.Idle,
		globalIdle:    queueOption.GlobalIdle,
	}, nil
}

func (service *RedisQueueServiceV7) CreateProducer() (Producer, error) {
	return service, nil
}

func (service *RedisQueueServiceV7) IfCreateMkStream(ctx context.Context) error {
	return service.client.XGroupCreateMkStream(ctx, service.queueName, service.consumerGroup, "0").Err()
}

func (service *RedisQueueServiceV7) CreateConsumer() (Consumer, error) {
	_ = service.IfCreateMkStream(context.Background()) // 重复创建 XGroup 会报错，忽略
	consumer := &RedisQueueConsumerV7{
		client:     service.client,
		stream:     service.queueName,
		group:      service.consumerGroup,
		consumer:   fmt.Sprintf("consumer-%s", utils.RandomString(6)),
		idle:       service.idle,
		globalIdle: service.globalIdle,
	}
	return consumer, nil
}

func (service *RedisQueueServiceV7) Close() error {
	return nil
}

func (service *RedisQueueServiceV7) SendMessage(ctx context.Context, body string) error {
	err := service.client.XAdd(ctx, &redis.XAddArgs{
		Stream: service.queueName,
		Values: map[string]interface{}{"_body_key": body},
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

type RedisQueueConsumerV7 struct {
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
func (c *RedisQueueConsumerV7) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {

	// 先获取当前消费者 Pending 消息
	pendingMessages, err := c.getPendingMessages(ctx, true, maxCount, c.getIdle())
	if err != nil {
		return nil, err
	}
	if len(pendingMessages) > 0 {
		messages, err := xMessagesToMessagesV7(pendingMessages)
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
		messages, err := xMessagesToMessagesV7(pendingMessages)
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
			newMsg, err := deepCopyXMessageV7(xMsg)
			if err != nil {
				return nil, err
			}
			messages = append(messages, &RedisQueueMessageV7{message: &newMsg})
		}
	}
	return messages, nil
}

func (c *RedisQueueConsumerV7) getIdle() time.Duration {
	if c.idle <= 0 {
		return DefaultIdle
	}
	return time.Duration(c.idle) * time.Second
}

func (c *RedisQueueConsumerV7) getGlobalIdle() time.Duration {
	if c.globalIdle <= 0 {
		return DefaultGlobalIdle
	}
	return time.Duration(c.globalIdle) * time.Second
}

// 获取 Pending 消息
func (c *RedisQueueConsumerV7) getPendingMessages(ctx context.Context, withConsumer bool, count int, idle time.Duration) ([]redis.XMessage, error) {

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

func (c *RedisQueueConsumerV7) AckMessage(ctx context.Context, message Message) error {
	redisMessage, ok := message.(*RedisQueueMessageV7)
	if !ok {
		return errors.New("invalid message type, should be redis message")
	}
	ackIds := []string{redisMessage.message.ID}
	queueName, consumerGroup := c.stream, c.group
	return c.client.XAck(ctx, queueName, consumerGroup, ackIds...).Err()
}

func (c *RedisQueueConsumerV7) Close() error {
	return c.client.XGroupDelConsumer(context.Background(),
		c.stream,
		c.group,
		c.consumer,
	).Err()
}

func deepCopyXMessageV7(m redis.XMessage) (redis.XMessage, error) {
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

func xMessagesToMessagesV7(xMessages []redis.XMessage) ([]Message, error) {
	messages := make([]Message, 0, len(xMessages))
	for _, xMsg := range xMessages {
		newMsg, err := deepCopyXMessageV7(xMsg)
		if err != nil {
			return nil, err
		}
		messages = append(messages, &RedisQueueMessageV7{message: &newMsg})
	}
	return messages, nil
}

func applyCustomOptionsV7(redisOptions *redis.Options, queueOption StandaloneRedisQueueOptionV7) {
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

func applyCustomClusterOptionsV7(redisOptions *redis.ClusterOptions, queueOption ClusterRedisQueueOptionV7) {
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
