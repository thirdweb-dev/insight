package middleware

import (
	"fmt"
	"net/http"

	"github.com/ethereum/go-ethereum/log"
	"github.com/thirdweb-dev/indexer/api"
)

var ErrUnauthorized = fmt.Errorf("invalid username or password")

func Authorization(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || !validateCredentials(username, password) {
			log.Error(ErrUnauthorized.Error())
			api.UnauthorizedErrorHandler(w, ErrUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func validateCredentials(username, password string) bool {
	return username == "admin" && password == "admin"
}