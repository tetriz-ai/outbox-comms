// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package model holds the GORM models for the seven tables behind the delivery
// pipeline. Enum-valued columns store the proto enum's value name, and jsonb
// columns store protojson, through the two serializers RegisterSerializers adds.
// The migrations package holds the schema.
package model

import (
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// CommsRequest is one producer submission: the durable handle, the dedup anchor,
// the shared params and the producer-resolved entries batch. One request fans
// out to N message rows.
type CommsRequest struct {
	ID                string               `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	OrganizationID    string               `gorm:"column:organization_id;type:uuid;not null"`
	IdempotencyKey    string               `gorm:"column:idempotency_key;not null"`
	Usecase           string               `gorm:"column:usecase;not null"`
	Params            *structpb.Struct     `gorm:"column:params;type:jsonb;not null;default:'{}';serializer:outboxcomms_protojson"`
	Content           *pb.MessageContent   `gorm:"column:content;type:jsonb;serializer:outboxcomms_protojson"`
	Entries           *pb.CommsEntries     `gorm:"column:entries;type:jsonb;not null;default:'{}';serializer:outboxcomms_protojson"`
	DeliveryGuarantee pb.DeliveryGuarantee `gorm:"column:delivery_guarantee;not null;serializer:outboxcomms_protoenum"`
	Status            pb.RequestStatus     `gorm:"column:status;not null;serializer:outboxcomms_protoenum"`
	AnalyticsID       *string              `gorm:"column:analytics_id"`
	CreatedAt         time.Time            `gorm:"column:created_at;not null"`
	UpdatedAt         time.Time            `gorm:"column:updated_at;not null"`
}

// TableName returns the database table name for GORM.
func (CommsRequest) TableName() string { return "comms_requests" }

// RenderableContentForEntry resolves the renderable body for one of the request's
// entries: the entry's own MessageContent when set, else the request-level shared
// content, narrowed to the entry's channel (override, then base). nil when no
// content was supplied; the dispatcher then renders the usecase's typed renderer
// over the params.
func (r *CommsRequest) RenderableContentForEntry(e *pb.CommsEntry) *pb.RenderableContent {
	mc := e.GetContent()
	if mc == nil {
		mc = r.Content
	}
	if mc == nil {
		return nil
	}
	switch e.GetChannel() {
	case pb.Channel_CHANNEL_EMAIL:
		if mc.GetEmail() != nil {
			return mc.GetEmail()
		}
	case pb.Channel_CHANNEL_SLACK:
		if mc.GetSlack() != nil {
			return mc.GetSlack()
		}
	default:
		// Any other channel has no per-channel override; use the base body.
	}
	return mc.GetBase()
}

// CommsMessage is one comm to one recipient on one channel, materialized by the
// fan-out from a request's entries. Identity columns are written once at fan-out;
// the lifecycle columns are written by the dispatcher, reconciler and feedback
// consumer.
type CommsMessage struct {
	ID                string                `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	OrganizationID    string                `gorm:"column:organization_id;type:uuid;not null"`
	RequestID         string                `gorm:"column:request_id;type:uuid;not null"`
	Channel           pb.Channel            `gorm:"column:channel;not null;serializer:outboxcomms_protoenum"`
	Vendor            pb.Vendor             `gorm:"column:vendor;not null;serializer:outboxcomms_protoenum"`
	Recipient         string                `gorm:"column:recipient;not null"`
	SlackDelivery     pb.SlackDelivery      `gorm:"column:slack_delivery;not null;serializer:outboxcomms_protoenum"`
	RecipientContext  *structpb.Struct      `gorm:"column:recipient_context;type:jsonb;not null;default:'{}';serializer:outboxcomms_protojson"`
	Content           *pb.RenderableContent `gorm:"column:content;type:jsonb;serializer:outboxcomms_protojson"`
	Status            pb.MessageStatus      `gorm:"column:status;not null;serializer:outboxcomms_protoenum"`
	SkipReason        *string               `gorm:"column:skip_reason"`
	DeadReason        *string               `gorm:"column:dead_reason"`
	UnconfirmedReason *string               `gorm:"column:unconfirmed_reason"`
	Attempts          int                   `gorm:"column:attempts;not null;default:0"`
	VerificationLevel pb.VerificationLevel  `gorm:"column:verification_level;not null;serializer:outboxcomms_protoenum"`
	DeliveryGuarantee pb.DeliveryGuarantee  `gorm:"column:delivery_guarantee;not null;serializer:outboxcomms_protoenum"`
	NextRetryAt       *time.Time            `gorm:"column:next_retry_at"`
	RenderedContent   *pb.RenderedPayload   `gorm:"column:rendered_content;type:jsonb;serializer:outboxcomms_protojson"`
	ProviderMessageID *string               `gorm:"column:provider_message_id"`
	// ClaimID names the claim that moved the row to SENDING. Every move out of
	// SENDING by a worker must carry it, so a worker whose claim was taken over
	// cannot move another worker's row. It means something only while the row
	// is SENDING: some moves out of SENDING clear it and others leave the last
	// value, which nothing reads.
	ClaimID   *string   `gorm:"column:claim_id;type:uuid"`
	LastError *string   `gorm:"column:last_error"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null"`
}

// TableName returns the database table name for GORM.
func (CommsMessage) TableName() string { return "comms_messages" }

// CommsDeliveryEvent is an append-only provider event (sent, failed, delivered,
// bounce, complaint) for a message. ProviderEventID is the provider's stable
// event id the feedback consumer dedups on.
type CommsDeliveryEvent struct {
	ID              string               `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	OrganizationID  string               `gorm:"column:organization_id;type:uuid;not null"`
	MessageID       string               `gorm:"column:message_id;type:uuid;not null"`
	EventType       pb.DeliveryEventType `gorm:"column:event_type;not null;serializer:outboxcomms_protoenum"`
	ProviderEventID *string              `gorm:"column:provider_event_id"`
	// ProviderPayload is the provider's raw event JSON, kept verbatim because its
	// shape is the vendor's, not ours.
	ProviderPayload []byte    `gorm:"column:provider_payload;type:jsonb"`
	OccurredAt      time.Time `gorm:"column:occurred_at;not null"`
	CreatedAt       time.Time `gorm:"column:created_at;not null"`
}

// TableName returns the database table name for GORM.
func (CommsDeliveryEvent) TableName() string { return "comms_delivery_events" }

// CommsSuppression is a global sender-reputation fact keyed by email address. It
// has no organization: a hard-bounced address bounces for every sender.
// Un-suppress is a soft delete.
type CommsSuppression struct {
	ID        string     `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	Email     string     `gorm:"column:email;not null"`
	Reason    string     `gorm:"column:reason;not null"`
	CreatedAt time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt time.Time  `gorm:"column:updated_at;not null"`
	DeletedAt *time.Time `gorm:"column:deleted_at"`
}

// TableName returns the database table name for GORM.
func (CommsSuppression) TableName() string { return "comms_suppressions" }

// CommsPurgeTombstone is what a purge keeps of one email message that reached the
// provider: its provider message id and lowercased address. It has no
// organization, like a suppression.
type CommsPurgeTombstone struct {
	ProviderMessageID string    `gorm:"column:provider_message_id;primaryKey"`
	Recipient         string    `gorm:"column:recipient;not null"`
	CreatedAt         time.Time `gorm:"column:created_at;not null"`
}

// TableName returns the database table name for GORM.
func (CommsPurgeTombstone) TableName() string { return "comms_purge_tombstones" }

// CommsPreference is one cell of the sparse preference gate
// (entity × category × channel → enabled), PERSON beating ORG.
type CommsPreference struct {
	ID             string             `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	OrganizationID string             `gorm:"column:organization_id;type:uuid;not null"`
	EntityType     pb.EntityType      `gorm:"column:entity_type;not null;serializer:outboxcomms_protoenum"`
	EntityID       string             `gorm:"column:entity_id;type:uuid;not null"`
	Category       pb.MessageCategory `gorm:"column:category;not null;serializer:outboxcomms_protoenum"`
	Channel        pb.Channel         `gorm:"column:channel;not null;serializer:outboxcomms_protoenum"`
	Enabled        bool               `gorm:"column:enabled;not null"`
	CreatedAt      time.Time          `gorm:"column:created_at;not null"`
	UpdatedAt      time.Time          `gorm:"column:updated_at;not null"`
}

// TableName returns the database table name for GORM.
func (CommsPreference) TableName() string { return "comms_preferences" }

// ToProto converts the preference row to its API message.
func (r *CommsPreference) ToProto() *pb.Preference {
	return &pb.Preference{
		EntityType: r.EntityType, EntityId: r.EntityID, Category: r.Category, Channel: r.Channel, Enabled: r.Enabled,
	}
}

// CommsChannelDestination is one routing destination (usecase → chat channel),
// N rows per usecase. Chat-only: email recipients are per send.
type CommsChannelDestination struct {
	ID              string             `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	OrganizationID  string             `gorm:"column:organization_id;type:uuid;not null"`
	EntityType      pb.EntityType      `gorm:"column:entity_type;not null;serializer:outboxcomms_protoenum"`
	EntityID        string             `gorm:"column:entity_id;type:uuid;not null"`
	Usecase         string             `gorm:"column:usecase;not null"`
	Channel         pb.Channel         `gorm:"column:channel;not null;serializer:outboxcomms_protoenum"`
	DestinationKind pb.DestinationKind `gorm:"column:destination_kind;not null;serializer:outboxcomms_protoenum"`
	DestinationRef  string             `gorm:"column:destination_ref;not null"`
	CreatedAt       time.Time          `gorm:"column:created_at;not null"`
	UpdatedAt       time.Time          `gorm:"column:updated_at;not null"`
}

// TableName returns the database table name for GORM.
func (CommsChannelDestination) TableName() string { return "comms_channel_destinations" }

// ToProto converts the destination row to its API message.
func (r *CommsChannelDestination) ToProto() *pb.Destination {
	return &pb.Destination{
		EntityType: r.EntityType, EntityId: r.EntityID, Usecase: r.Usecase,
		Channel: r.Channel, DestinationKind: r.DestinationKind, DestinationRef: r.DestinationRef,
	}
}
