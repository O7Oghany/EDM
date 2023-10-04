package api

import (
	"encoding/json"
	"net/http"
	"path"

	"github.com/O7Oghany/EDM/inventory-service/internal/router"
	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/O7Oghany/EDM/inventory-service/pkg/service"
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
	h.router.HandleFunc("/inventory/batch", h.AddItemsBatch).Methods("POST")
	h.router.HandleFunc("/inventory/{id}", h.UpdateItem).Methods("PUT")
	h.router.HandleFunc("/inventory/{id}", h.DeleteItem).Methods("DELETE")
	h.router.HandleFunc("/inventory/{id}", h.GetItem).Methods("GET")
	h.router.HandleFunc("/inventory", h.ListItems).Methods("GET")
}

func (h *InventoryHandler) GetItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	itemID := path.Base(r.URL.Path)
	item, err := h.service.GetItem(ctx, itemID)
	if err != nil {
		h.logger.Error(ctx, "Failed to get item", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encodedItem, err := json.Marshal(item)
	if err != nil {
		h.logger.Error(ctx, "Failed to encode item", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(encodedItem)
	if err != nil {
		h.logger.Error(ctx, "Failed to write item", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *InventoryHandler) ListItems(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	items, err := h.service.ListItems(ctx)
	if err != nil {
		h.logger.Error(ctx, "Failed to list items", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	encodedItems, err := json.Marshal(items)
	if err != nil {
		h.logger.Error(ctx, "Failed to encode items", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(encodedItems)
	if err != nil {
		h.logger.Error(ctx, "Failed to write items", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *InventoryHandler) AddItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var item models.CPU
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.service.AddItem(ctx, item); err != nil {
		h.logger.Error(ctx, "Failed to add item", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *InventoryHandler) AddItemsBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var items []models.CPU
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.service.AddItemsBatch(ctx, items); err != nil {
		h.logger.Error(ctx, "Failed to add items batch", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *InventoryHandler) UpdateItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	itemID := path.Base(r.URL.Path)
	originalItem, err := h.service.GetItem(ctx, itemID)
	if err != nil {
		h.logger.Error(ctx, "Failed to get item", "error", err)
		http.Error(w, "Id not found", http.StatusBadRequest)
		return
	}

	var updatedItem models.CPU
	if err := json.NewDecoder(r.Body).Decode(&updatedItem); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.service.UpdateItem(ctx, *originalItem, updatedItem); err != nil {
		h.logger.Error(ctx, "Failed to update item", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *InventoryHandler) DeleteItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	itemID := path.Base(r.URL.Path)

	if _, err := h.service.GetItem(ctx, itemID); err != nil {
		h.logger.Error(ctx, "Failed to get item", "error", err)
		http.Error(w, "Id not found", http.StatusBadRequest)
		return
	}

	if err := h.service.DeleteItem(ctx, itemID); err != nil {
		h.logger.Error(ctx, "Failed to delete item", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
