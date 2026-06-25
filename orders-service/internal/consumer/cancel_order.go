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

type CancelOrderConsumer struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	orderService *service.OrderService
}

func NewCancelOrderConsumer(logger *zap.Logger, reader *kafka.Reader, orderService *service.OrderService) *CancelOrderConsumer {
	return &CancelOrderConsumer{
		logger:       logger,
		reader:       reader,
		orderService: orderService,
	}
}

func (c *CancelOrderConsumer) Start(ctx context.Context) {
	c.logger.Info("cancel-order consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("cancel-order consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *CancelOrderConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("cancel-order-consumer")
	ctx, span := tracer.Start(ctx, "orders.commands.cancel-order process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	var cmd domain.CancelOrderCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Error("error unmarshalling message", zap.Error(err))
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing cancel-order command",
		zap.String("saga_id", cmd.SagaID),
		zap.String("order_id", cmd.OrderID),
		zap.String("reason", cmd.Reason),
	)

	if err := c.orderService.CancelOrder(ctx, cmd.SagaID, cmd.OrderID, cmd.Reason); err != nil {
		log.Error("error canceling order", zap.String("order_id", cmd.OrderID), zap.Error(err))
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *CancelOrderConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}
