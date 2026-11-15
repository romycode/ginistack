package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/romycode/ginistack/internal/sqs"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:4566", "address to listen on")
	flag.Parse()
	server := &http.Server{
		Addr:              *addr,
		Handler:           sqs.NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("Ginistack SQS listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
