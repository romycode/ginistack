package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/romycode/ginistack/internal/awshttp"
	"github.com/romycode/ginistack/internal/sns"
	"github.com/romycode/ginistack/internal/sqs"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:4566", "address to listen on")
	flag.Parse()
	server := &http.Server{
		Addr:              *addr,
		Handler:           newHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("Ginistack listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newHandler() http.Handler {
	sqsHandler := sqs.NewHandler()
	snsHandler := sns.NewHandler(sqsHandler.DeliverNotification)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == sns.SigningCertPath {
			snsHandler.ServeHTTP(w, r)
			return
		}
		if awshttp.IsQuery(r) {
			if !awshttp.AcceptBody(w, r) {
				return
			}
			_ = awshttp.ParseForm(r)
			if sns.MatchesQuery(r) {
				snsHandler.ServeHTTP(w, r)
				return
			}
		}
		sqsHandler.ServeHTTP(w, r)
	})
}
