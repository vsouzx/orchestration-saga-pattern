package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/repository"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type OrderService struct {
	logger           *zap.Logger
	db               *sql.DB
	orderRepository  repository.OrdersRepositoryInterface
	outboxRepository repository.OutboxRepositoryInterface
}

func NewOrderService(logger *zap.Logger, db *sql.DB,
	orderRepository repository.OrdersRepositoryInterface,
	outboxRepository repository.OutboxRepositoryInterface) *OrderService {
	return &OrderService{
		logger:           logger,
		db:               db,
		orderRepository:  orderRepository,
		outboxRepository: outboxRepository,
	}
}

func (os *OrderService) CreateOrder(ctx context.Context, request domain.OrderRequest) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	orderId, err := os.orderRepository.Save(ctx, tx, request)
	if err != nil {
		return fmt.Errorf("creating order: %w", err)
	}

	reply := domain.OrderCreatedReply{
		OrderID:     orderId,
		UserID:      request.UserID,
		ProductID:   request.ProductID,
		Quantity:    request.Quantity,
		PaymentType: request.PaymentType,
		Status:      "CREATED",
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling order created reply: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.replies",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("creating order: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.With(zap.String("order_id", orderId)).Info("Order created")

	return nil
}

func (os *OrderService) CancelOrder(ctx context.Context, sagaId string, orderId string, reason string) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	if err := os.orderRepository.UpdateStatusToCanceled(ctx, tx, orderId, reason); err != nil {
		return fmt.Errorf("canceling order: %w", err)
	}

	reply := domain.SagaReply{
		SagaID:  sagaId,
		OrderID: orderId,
		Status:  "SUCCESS",
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling reply: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.replies",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("saving outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Info("order canceled", zap.String("order_id", orderId), zap.String("reason", reason))
	return nil
}

func (os *OrderService) ConfirmOrder(ctx context.Context, sagaId string, orderId string) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	updated, err := os.orderRepository.ConfirmOrder(ctx, tx, orderId)
	if err != nil {
		return fmt.Errorf("confirming order: %w", err)
	}

	if !updated {
		log.Info("order already processed, skipping", zap.String("order_id", orderId))
		return nil
	}

	reply := domain.SagaReply{
		SagaID:  sagaId,
		OrderID: orderId,
		Status:  "SUCCESS",
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling reply: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.replies",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("saving outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Info("order confirmed", zap.String("order_id", orderId))
	return nil
}

func (os *OrderService) ListOrders(ctx context.Context) ([]domain.OrderResponse, error) {
	orders, err := os.orderRepository.FindAll(ctx, os.db)
	if err != nil {
		return nil, fmt.Errorf("listing orders: %w", err)
	}
	return orders, nil
}

func traceParentFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if !sc.IsValid() {
		return ""
	}

	return fmt.Sprintf("00-%s-%s-%s",
		sc.TraceID().String(),
		sc.SpanID().String(),
		sc.TraceFlags().String(),
	)
}
