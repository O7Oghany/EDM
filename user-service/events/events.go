package events

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/O7Oghany/EDM/common"
	"github.com/O7Oghany/EDM/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

type Event struct {
	EventType string
	producer  *kafka.Producer
	Message   *common.Message
}

func (e *Event) GetEventType() string {
	return e.EventType
}

func NewEvent(producer *kafka.Producer) *Event {
	return &Event{
		EventType: "UserEvents",
		producer:  producer,
		Message:   &common.Message{},
	}
}

func (e *Event) InitEventHandlers(r *mux.Router) {
	r.HandleFunc("/api/user", e.createEventHandler).Methods("POST")
	r.HandleFunc("/api/user", e.updateEventHandler).Methods("PATCH")
	r.HandleFunc("/api/user", e.deleteEventHandler).Methods("DELETE")
}

func (e *Event) createEventHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userID")
	userName := r.URL.Query().Get("userName")
	if userID == "" || userName == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("userID && userName are required"))
		return
	}

	message := []byte(fmt.Sprintf("UserCreated,%s,%s", userID, userName))
	e.Message.SendMessage(e.producer, e.EventType, message)
}

func (e *Event) updateEventHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userID")
	if userID == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("userID is required"))
		return
	}
	if r.Body == http.NoBody {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("request body is required"))
		return
	}
	var updatedFields map[string]string
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&updatedFields)
	if err != nil {
		log.Printf("Failed to decode request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid JSON format"))
		return
	}
	if len(updatedFields) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("request body is required"))
		return
	}
	event := models.UserUpdateEvent{
		EventType:     "UserUpdated",
		UserID:        userID,
		UpdatedFields: updatedFields,
	}
	message, _ := json.Marshal(event)
	e.Message.SendMessage(e.producer, e.EventType, message)
}

func (e *Event) deleteEventHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userID")
	if userID == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("userID is required"))
		return
	}
	message := []byte(fmt.Sprintf("UserDeleted,%s", userID))
	e.Message.SendMessage(e.producer, e.EventType, message)
}
