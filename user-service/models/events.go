package models

type UserUpdateEvent struct {
	EventType     string            `json:"eventType"`
	UserID        string            `json:"userID"`
	UpdatedFields map[string]string `json:"updatedFields"`
}
