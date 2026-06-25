package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server ServerConfig `mapstructure:"server"`
	MySQL  MySQLConfig  `mapstructure:"mysql"`
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

type KafkaConfig struct {
	Brokers              []string `mapstructure:"brokers"`
	ProcessPaymentTopic  string   `mapstructure:"process_payment_topic"`
	PaymentsRepliesTopic string   `mapstructure:"payments_replies_topic"`
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
	v.BindEnv("kafka.brokers", "KAFKA_BROKERS")
	v.BindEnv("kafka.process_payment_topic", "KAFKA_PROCESS_PAYMENT_TOPIC")
	v.BindEnv("kafka.payments_replies_topic", "KAFKA_PAYMENTS_REPLIES_TOPIC")
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
