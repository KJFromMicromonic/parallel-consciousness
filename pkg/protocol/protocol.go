// Package protocol defines the wire contract for inter-agent communication.
//
// The design goal is conversation, not transport. A bus moves bytes; this
// package defines the speech acts, threading, and addressing that let agents
// negotiate like coworkers instead of dropping notes in a shared file.
package protocol

import (
	"time"

	"github.com/google/uuid"
)

// Intent is a speech act. Agents react to intent, not to free-form prose.
// This is what keeps multi-agent chatter parseable and prevents it degrading
// into an unstructured wall of text that no agent can reliably act on.
type Intent string

const (
	// Coordination / work negotiation
	IntentRequest  Intent = "request"  // "do this and tell me when done"
	IntentPropose  Intent = "propose"  // "I suggest we do X" (needs agree/disagree)
	IntentAgree    Intent = "agree"    // accept a proposal
	IntentDisagree Intent = "disagree" // reject a proposal (carries a reason)
	IntentInform   Intent = "inform"   // FYI, no response required
	IntentBlock    Intent = "block"    // "I am blocked on you" (escalates)
	IntentDone     Intent = "done"     // a request was completed
	IntentReady    Intent = "ready"    // "I'm at a compatible state for a gate"

	// Control plane
	IntentAck   Intent = "ack"   // I received your message
	IntentNack  Intent = "nack"  // I cannot handle this (with reason)
	IntentYield Intent = "yield" // I'm releasing the turn / done speaking
)

// Address identifies a sender or recipient. An empty Agent with a Topic set
// means a broadcast to everyone subscribed to that topic.
type Address struct {
	Agent string `json:"agent,omitempty"` // direct recipient, e.g. "planner"
	Topic string `json:"topic,omitempty"` // broadcast channel, e.g. "project.alpha"
}

// Message is the envelope every agent sends and receives.
//
// Threading is the part that makes this feel like a conversation rather than
// RPC: ConversationID groups a whole exchange, InReplyTo chains individual
// turns, so any agent can reconstruct "what were we talking about" without
// re-reading an entire topic.
type Message struct {
	ID             string         `json:"id"`
	ConversationID string         `json:"conversation_id"`
	InReplyTo      string         `json:"in_reply_to,omitempty"`
	From           Address        `json:"from"`
	To             Address        `json:"to"`
	Intent         Intent         `json:"intent"`
	Body           map[string]any `json:"body,omitempty"`
	Timestamp      time.Time      `json:"timestamp"`

	// Deadline, when set, asks the recipient to ack/respond before this time.
	// A silent coworker is a problem; timeouts let the bus escalate instead of
	// hanging forever. Zero value means no deadline.
	Deadline time.Time `json:"deadline,omitempty"`
}

// New creates a message that starts a fresh conversation.
func New(from, to Address, intent Intent, body map[string]any) Message {
	id := uuid.NewString()
	return Message{
		ID:             id,
		ConversationID: id, // first message: conversation == message id
		From:           from,
		To:             to,
		Intent:         intent,
		Body:           body,
		Timestamp:      time.Now().UTC(),
	}
}

// Reply builds a response that stays in the same conversation and chains to
// the message it answers. This is the primitive that makes turn-taking work.
func (m Message) Reply(from Address, intent Intent, body map[string]any) Message {
	return Message{
		ID:             uuid.NewString(),
		ConversationID: m.ConversationID,
		InReplyTo:      m.ID,
		From:           from,
		To:             m.From, // reply goes back to whoever spoke
		Intent:         intent,
		Body:           body,
		Timestamp:      time.Now().UTC(),
	}
}

// WantsResponse reports whether an intent obliges the recipient to reply.
// inform and the control acks are terminal; the rest expect a turn back.
func (i Intent) WantsResponse() bool {
	switch i {
	case IntentInform, IntentAck, IntentNack, IntentYield, IntentDone, IntentReady:
		return false
	default:
		return true
	}
}
