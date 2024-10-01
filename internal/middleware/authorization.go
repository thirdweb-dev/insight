package middleware

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/gin-gonic/gin"
	"github.com/thirdweb-dev/indexer/api"
)

var ErrUnauthorized = fmt.Errorf("invalid username or password")

func Authorization(c *gin.Context) {
	username, password, ok := c.Request.BasicAuth()
	if !ok || !validateCredentials(username, password) {
		log.Error(ErrUnauthorized.Error())
		api.UnauthorizedErrorHandler(c, ErrUnauthorized)
		c.Abort()
		return
	}
	c.Next()
}

func validateCredentials(username, password string) bool {
	return username == "admin" && password == "admin"
}
