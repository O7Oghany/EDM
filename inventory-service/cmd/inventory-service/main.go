package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/O7Oghany/EDM/inventory-service/internal/api"
	"github.com/O7Oghany/EDM/inventory-service/internal/db"
	internal "github.com/O7Oghany/EDM/inventory-service/internal/kafka"
	"github.com/O7Oghany/EDM/inventory-service/internal/router"
	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/O7Oghany/EDM/inventory-service/pkg/service"
	"github.com/O7Oghany/EDM/inventory-service/pkg/util"
	"github.com/gorilla/mux"
)

func main() {
	logger, err := logger.NewZapLogger()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	dsn := os.Getenv("DSN")
	if dsn == "" {
		logger.Error(context.Background(), "DSN not set exiting...")
		os.Exit(1)
	}
	gormDB, err := models.NewDatabaseConnection(dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	inventoryDB := db.NewInventoryDB(gormDB)
	producerCfg, _, err := util.LoadConfigs()
	if err != nil {
		log.Fatalf("Error loading configs: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := internal.NewKafkaProducer(ctx, producerCfg, logger)
	if err != nil {
		log.Fatalf("Error starting producer server: %v", err)
	}

	inventoryService := service.NewInventoryService(inventoryDB, logger, producer)

	r := mux.NewRouter()
	wrappedRouter := &muxRouterWrapper{Router: r}
	handler := api.NewInventoryHandler(inventoryService, logger, wrappedRouter)
	handler.RegisterRoutes()

	server := &http.Server{
		Addr:    ":9070",
		Handler: r,
	}
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		<-sigChan
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	logger.Info(ctx, "Starting the server at :9070")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error(ctx, "Failed to run server: ", err)
	}
}

type muxRouteWrapper struct {
	Route *mux.Route
}

func (mrw *muxRouteWrapper) Methods(methods ...string) router.Route {
	mrw.Route.Methods(methods...)
	return mrw
}

type muxRouterWrapper struct {
	Router *mux.Router
}

func (m *muxRouterWrapper) HandleFunc(path string, f func(http.ResponseWriter, *http.Request)) router.Route {
	r := m.Router.HandleFunc(path, f)
	return &muxRouteWrapper{Route: r}
}
