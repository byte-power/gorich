package queue

import (
	"errors"
	"time"

	"github.com/byte-power/gorich/cloud"
)

var (
	ErrStandaloneRedisQueueAddrEmpty          = errors.New("standalone-redis addr is empty")
	ErrStandaloneRedisQueueConsumerGroupEmpty = errors.New("standalone-redis queue consumer group name is empty")
	ErrClusterRedisQueueAddrsEmpty            = errors.New("cluster-redis addrs is empty")
	ErrClusterRedisQueueConsumerGroupEmpty    = errors.New("cluster-redis queue consumer group name is empty")
)

// StandaloneRedisQueueOption 用于 Redis Standalone 实例

type StandaloneRedisQueueOption struct {
	Addr         string         `json:"addr" yaml:"addr"`
	Password     string         `json:"password" yaml:"password"`
	DB           *int           `json:"db" yaml:"db"`
	MaxRetries   *int           `json:"max_retries" yaml:"max_retries"`
	PoolSize     *int           `json:"pool_size" yaml:"pool_size"`
	DialTimeout  *time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout  *time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout *time.Duration `json:"write_timeout" yaml:"write_timeout"`
	MinIdleConns *int           `json:"min_idle_conns" yaml:"min_idle_conns"`

	// queue
	ConsumerGroup string `json:"consumer_group" yaml:"consumer_group"`
	Idle          int    `json:"idle" yaml:"idle"`
	GlobalIdle    int    `json:"global_idle" yaml:"global_idle"`
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
	// redis cluster
	Addrs           []string       `json:"addrs" yaml:"addrs"`
	Password        string         `json:"password" yaml:"password"`
	DB              *int           `json:"db" yaml:"db"`
	MaxRetries      *int           `json:"max_retries" yaml:"max_retries"`
	PoolSize        *int           `json:"pool_size" yaml:"pool_size"`
	DialTimeout     *time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout     *time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout    *time.Duration `json:"write_timeout" yaml:"write_timeout"`
	MinIdleConns    *int           `json:"min_idle_conns" yaml:"min_idle_conns"`
	MaxIdleConns    *int           `json:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxIdleTime *time.Duration `json:"conn_max_idle_time" yaml:"conn_max_idle_time"`
	ConnMaxLifetime *time.Duration `json:"conn_max_lifetime" yaml:"conn_max_lifetime"`

	// queue
	ConsumerGroup string `json:"consumer_group" yaml:"consumer_group"`
	Idle          int    `json:"idle" yaml:"idle"`
	GlobalIdle    int    `json:"global_idle" yaml:"global_idle"`
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
