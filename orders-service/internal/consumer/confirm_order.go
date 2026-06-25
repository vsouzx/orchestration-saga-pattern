package consumer

import (
	"context"
	"encoding/json"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type ConfirmOrderConsumer struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	orderService *service.OrderService
}

func NewConfirmOrderConsumer(logger *zap.Logger, reader *kafka.Reader, orderService *service.OrderService) *ConfirmOrderConsumer {
	return &ConfirmOrderConsumer{
		logger:       logger,
		reader:       reader,
		orderService: orderService,
	}
}

func (c *ConfirmOrderConsumer) Start(ctx context.Context) {
	c.logger.Info("confirm-order consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("confirm-order consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *ConfirmOrderConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("confirm-order-consumer")
	ctx, span := tracer.Start(ctx, "orders.commands.confirm-order process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	var cmd domain.ConfirmOrderCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Error("error unmarshalling message", zap.Error(err))
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing confirm-order command",
		zap.String("saga_id", cmd.SagaID),
		zap.String("order_id", cmd.OrderID),
	)

	if err := c.orderService.ConfirmOrder(ctx, cmd.SagaID, cmd.OrderID); err != nil {
		log.Error("error confirming order", zap.String("order_id", cmd.OrderID), zap.Error(err))
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *ConfirmOrderConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}
