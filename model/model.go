package model

// UserEvent represents a (surprise!) user event.
//
// It contains the user ID and the event payload.
type UserEvent struct {
	UserID  string
	Payload string
}
