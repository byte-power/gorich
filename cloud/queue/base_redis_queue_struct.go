package queue

import (
	"errors"
	"github.com/byte-power/gorich/cloud"
	"time"
)

var (
	ErrStandaloneRedisQueueAddrEmpty          = errors.New("standalone-redis addr is empty")
	ErrStandaloneRedisQueueConsumerGroupEmpty = errors.New("standalone-redis queue consumer group name is empty")
	ErrClusterRedisQueueAddrsEmpty            = errors.New("cluster-redis addrs is empty")
	ErrClusterRedisQueueConsumerGroupEmpty    = errors.New("cluster-redis queue consumer group name is empty")
)

// StandaloneRedisQueueOption 用于 Redis Standalone 实例

type StandaloneRedisQueueOption struct {
	Addr         string
	Password     string
	DB           *int
	MaxRetries   *int
	PoolSize     *int
	DialTimeout  *time.Duration
	ReadTimeout  *time.Duration
	WriteTimeout *time.Duration
	MinIdleConns *int

	// queue
	ConsumerGroup string
	Idle          int
	GlobalIdle    int
}

func (option StandaloneRedisQueueOption) GetProvider() cloud.Provider {
	return cloud.StandaloneRedisProvider
}

func (option StandaloneRedisQueueOption) GetSecretID() string {
	return ""
}

func (option StandaloneRedisQueueOption) GetSecretKey() string {
	return ""
}

func (option StandaloneRedisQueueOption) GetAssumeRoleArn() string {
	return ""
}

func (option StandaloneRedisQueueOption) GetRegion() string {
	return ""
}

func (option StandaloneRedisQueueOption) GetAssumeRegion() string {
	return ""
}

func (option StandaloneRedisQueueOption) CheckAWS() error {
	return cloud.ErrProviderNotAWS
}

func (option StandaloneRedisQueueOption) CheckTencentCloud() error {
	return cloud.ErrProviderNotTencentCloud
}

func (option StandaloneRedisQueueOption) CheckStandaloneRedis() error {
	return option.check()
}

func (option StandaloneRedisQueueOption) CheckClusterRedis() error {
	return cloud.ErrProviderNotClusterRedis
}

func (option StandaloneRedisQueueOption) CheckAliCloudStorage() error {
	return cloud.ErrProviderNotAliCloudStorage
}

func (option StandaloneRedisQueueOption) check() error {
	if option.Addr == "" {
		return ErrStandaloneRedisQueueAddrEmpty
	}
	if option.ConsumerGroup == "" {
		return ErrStandaloneRedisQueueConsumerGroupEmpty
	}
	return nil
}

// ClusterRedisQueueOption 用于 Redis Cluster 集群

type ClusterRedisQueueOption struct {
	Addrs           []string
	Password        string
	DB              *int
	MaxRetries      *int
	PoolSize        *int
	DialTimeout     *time.Duration
	ReadTimeout     *time.Duration
	WriteTimeout    *time.Duration
	MinIdleConns    *int
	MaxIdleConns    *int
	ConnMaxIdleTime *time.Duration
	ConnMaxLifetime *time.Duration

	// queue
	ConsumerGroup string
	Idle          int
	GlobalIdle    int
}

func (option ClusterRedisQueueOption) GetProvider() cloud.Provider {
	return cloud.ClusterRedisProvider
}

func (option ClusterRedisQueueOption) GetSecretID() string {
	return ""
}

func (option ClusterRedisQueueOption) GetSecretKey() string {
	return ""
}

func (option ClusterRedisQueueOption) GetAssumeRoleArn() string {
	return ""
}

func (option ClusterRedisQueueOption) GetRegion() string {
	return ""
}

func (option ClusterRedisQueueOption) GetAssumeRegion() string {
	return ""
}

func (option ClusterRedisQueueOption) CheckAWS() error {
	return cloud.ErrProviderNotAWS
}

func (option ClusterRedisQueueOption) CheckTencentCloud() error {
	return cloud.ErrProviderNotTencentCloud
}

func (option ClusterRedisQueueOption) CheckStandaloneRedis() error {
	return cloud.ErrProviderNotStandaloneRedis
}

func (option ClusterRedisQueueOption) CheckClusterRedis() error {
	return option.check()
}

func (option ClusterRedisQueueOption) CheckAliCloudStorage() error {
	return cloud.ErrProviderNotAliCloudStorage
}

func (option ClusterRedisQueueOption) check() error {
	if len(option.Addrs) == 0 {
		return ErrClusterRedisQueueAddrsEmpty
	}
	if option.ConsumerGroup == "" {
		return ErrClusterRedisQueueConsumerGroupEmpty
	}
	return nil
}
