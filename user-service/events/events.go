package events

import (
	"fmt"
	"json"
	"log"
	"net/http"

	"github.com/O7Oghany/EDM/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Event struct {
	EventType string
	producer  *kafka.Producer
}

func (e *Event) GetEventType() string {
	return e.EventType
}

func (e *Event) Create() {
	http.HandleFunc("/user/create", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("userID")
		userName := r.URL.Query().Get("userName")
		message := []byte(fmt.Sprintf("UserCreated,%s,%s", userID, userName))
		common.sendMessage(e.producer, "UserEvents", message)
	})

}

func (e *Event) UpdateEvent() {
	http.HandleFunc("/user/update", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("userID")

		var updatedFields map[string]string
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&updatedFields)
		if err != nil {
			log.Printf("Failed to decode request body: %v", err)
			return
		}

		event := models.UserUpdateEvent{
			EventType:     "UserUpdated",
			UserID:        userID,
			UpdatedFields: updatedFields,
		}

		message, _ := json.Marshal(event)
		sendMessage(producer, "UserEvents", message)
	})

}
