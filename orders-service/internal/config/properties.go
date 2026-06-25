package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server ServerConfig `mapstructure:"server"`
	MySQL  MySQLConfig  `mapstructure:"mysql"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
	Outbox OutboxConfig `mapstructure:"outbox"`
	Otel   OtelConfig   `mapstructure:"otel"`
}

type OtelConfig struct {
	ExporterEndpoint string `mapstructure:"exporter_endpoint"`
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
}

type MySQLConfig struct {
	DSN string `mapstructure:"dsn"`
}

type RedisConfig struct {
	Addr string `mapstructure:"addr"`
	Pass string `mapstructure:"pass"`
	DB   int    `mapstructure:"db"`
}

type KafkaConfig struct {
	Brokers            []string `mapstructure:"brokers"`
	OrdersRepliesTopic string   `mapstructure:"orders_replies_topic"`
	ConfirmOrderTopic  string   `mapstructure:"confirm_order_topic"`
	CancelOrderTopic   string   `mapstructure:"cancel_order_topic"`
}

type OutboxConfig struct {
	BatchSize int `mapstructure:"batch_size"`
}

func Load() *Config {
	v := viper.New()

	// yaml file
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	if err := v.ReadInConfig(); err != nil {
		panic("failed to read config file: " + err.Error())
	}

	// env var bindings (mesmos nomes de antes)
	v.BindEnv("server.port", "SERVER_PORT")
	v.BindEnv("mysql.dsn", "MYSQL_DSN")
	v.BindEnv("redis.addr", "REDIS_ADDR")
	v.BindEnv("redis.pass", "REDIS_PASS")
	v.BindEnv("kafka.brokers", "KAFKA_BROKERS")
	v.BindEnv("kafka.orders_replies_topic", "KAFKA_ORDERS_REPLIES_TOPIC")
	v.BindEnv("kafka.confirm_order_topic", "KAFKA_CONFIRM_ORDER_TOPIC")
	v.BindEnv("kafka.cancel_order_topic", "KAFKA_CANCEL_ORDER_TOPIC")
	v.BindEnv("outbox.batch_size", "OUTBOX_BATCH_SIZE")
	v.BindEnv("otel.exporter_endpoint", "OTEL_EXPORTER_ENDPOINT")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		panic("failed to unmarshal config: " + err.Error())
	}

	// KAFKA_BROKERS como string separada por vírgula (compatibilidade)
	if brokersStr := v.GetString("kafka.brokers"); brokersStr != "" && len(cfg.Kafka.Brokers) == 1 {
		if parts := strings.Split(brokersStr, ","); len(parts) > 1 {
			cfg.Kafka.Brokers = parts
		}
	}

	return &cfg
}
