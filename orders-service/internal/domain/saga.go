package domain

type ConfirmOrderCommand struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
}

type CancelOrderCommand struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

type SagaReply struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}

type OrderCreatedReply struct {
	OrderID     string          `json:"orderId"`
	UserID      string          `json:"userId"`
	ProductID   int             `json:"productId"`
	Quantity    int             `json:"quantity"`
	PaymentType PaymentTypeEnum `json:"paymentType"`
	Status      string          `json:"status"`
}
