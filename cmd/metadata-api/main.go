package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/httpapi"
	postgresadapter "github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/postgres"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
)

const shutdownTimeout = 10 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("metadata API stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL := os.Getenv("DQ_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DQ_DATABASE_URL is required")
	}
	listenAddress := os.Getenv("DQ_METADATA_LISTEN_ADDRESS")
	if listenAddress == "" {
		listenAddress = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	catalog, err := postgresadapter.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer catalog.Close()

	queues := application.NewQueueService(catalog, application.RandomIDSource{}, application.SystemClock{})
	server := &http.Server{
		Addr:              listenAddress,
		Handler:           httpapi.NewHandler(queues, catalog, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("metadata API listening", "address", listenAddress)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return err
		}
		return nil
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
