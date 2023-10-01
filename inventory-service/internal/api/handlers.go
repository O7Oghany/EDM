package api

import (
	"encoding/json"
	"net/http"
	"path"
	"time"

	"github.com/O7Oghany/EDM/inventory-service/internal/kafka"
	"github.com/O7Oghany/EDM/inventory-service/internal/router"
	"github.com/O7Oghany/EDM/inventory-service/pkg/consts"
	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/O7Oghany/EDM/inventory-service/pkg/service"
	"github.com/O7Oghany/EDM/inventory-service/pkg/util"
)

type InventoryHandler struct {
	service service.InventoryService
	logger  logger.Logger
	router  router.Router
}

func NewInventoryHandler(s service.InventoryService, l logger.Logger, r router.Router) InventoryHandler {
	return InventoryHandler{
		service: s,
		logger:  l,
		router:  r,
	}
}

func (h *InventoryHandler) RegisterRoutes() {
	h.router.HandleFunc("/inventory", h.AddItem).Methods("POST")
	h.router.HandleFunc("/inventory/{id}", h.UpdateItem).Methods("PUT")
	//h.router.HandleFunc("/inventory/{id}", h.DeleteItem).Methods("DELETE")
	//h.router.HandleFunc("/inventory/{id}", h.GetItem).Methods("GET")
	//h.router.HandleFunc("/inventory", h.ListItems).Methods("GET")
}

func (h *InventoryHandler) AddItem(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	var item models.CPU
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.service.AddItem(ctx, item); err != nil {
		h.logger.Error(ctx, "Failed to add item: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	h.updateAvroSchema(consts.ItemCreatedEvent)
	itemAdded := models.ItemAdded{
		ItemID:    item.ID,
		Name:      item.Name,
		Quantity:  item.InStock, // This assumes InStock is the correct field for Quantity
		Price:     item.Price,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}

	if err := h.service.PublishEvent(ctx, consts.ItemCreatedEvent, itemAdded); err != nil {
		h.logger.Error(ctx, "Failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusCreated)

	w.WriteHeader(http.StatusCreated)
}

func (h *InventoryHandler) UpdateItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	itemID := path.Base(r.URL.Path)
	originalItem, err := h.service.GetItem(ctx, itemID)
	if err != nil {
		h.logger.Error(ctx, "Failed to get item: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var updatedItem models.CPU
	if err := json.NewDecoder(r.Body).Decode(&updatedItem); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	updatedFields, err := util.MapUpdatedFields(originalItem, updatedItem)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	itemUpdated := models.ItemUpdated{
		ItemId:    updatedItem.ID,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}

	// Populate only updated fields in ItemUpdated
	util.PopulateItemUpdated(&itemUpdated, updatedFields)

	if err := h.service.UpdateItem(ctx, updatedItem); err != nil {
		h.logger.Error(ctx, "Failed to update item: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	h.updateAvroSchema(consts.ItemUpdatedEvent)

	if err := h.service.PublishEvent(ctx, consts.ItemUpdatedEvent, itemUpdated); err != nil {
		h.logger.Error(ctx, "Failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusOK)
}
func (h *InventoryHandler) updateAvroSchema(event string) {
	schemaPath := "./schemas/" + event + ".avsc" // or however you determine your schema path

	avroEncoder, err := kafka.NewAvroEncoder(schemaPath)
	if err != nil {
		return
	}
	h.service.UpdateAvroEncoder(avroEncoder)
}
