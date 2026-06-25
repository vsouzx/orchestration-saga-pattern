package consumer

import (
	"context"
	"encoding/json"
	"payments-service/internal/domain"
	"payments-service/internal/logger"
	"payments-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

type ProcessPaymentConsumer struct {
	logger         *zap.Logger
	reader         *kafka.Reader
	paymentService *service.PaymentService
}

func NewProcessPaymentConsumer(logger *zap.Logger, reader *kafka.Reader, paymentService *service.PaymentService) *ProcessPaymentConsumer {
	return &ProcessPaymentConsumer{
		logger:         logger,
		reader:         reader,
		paymentService: paymentService,
	}
}

func (c *ProcessPaymentConsumer) Start(ctx context.Context) {
	c.logger.Info("process-payment consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("process-payment consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *ProcessPaymentConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	traceParent := extractTraceParent(msg.Headers)
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("process-payment-consumer")
	ctx, span := tracer.Start(ctx, "payments.commands.process-payment process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	var cmd domain.ProcessPaymentCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Error("error unmarshalling message", zap.Error(err))
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing process-payment command",
		zap.String("saga_id", cmd.SagaID),
		zap.String("order_id", cmd.OrderID),
		zap.Int("amount", cmd.Amount),
	)

	if err := c.paymentService.ProcessPayment(ctx, cmd, traceParent); err != nil {
		log.Error("error processing payment", zap.String("order_id", cmd.OrderID), zap.Error(err))
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *ProcessPaymentConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}

func extractTraceParent(headers []kafka.Header) string {
	for _, h := range headers {
		if h.Key == "traceparent" {
			return string(h.Value)
		}
	}
	return ""
}

func extractTraceContext(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := make(propagation.MapCarrier)
	for _, h := range headers {
		carrier.Set(h.Key, string(h.Value))
	}
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}
