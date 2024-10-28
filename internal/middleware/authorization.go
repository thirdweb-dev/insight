package middleware

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/gin-gonic/gin"
	"github.com/thirdweb-dev/indexer/api"
	config "github.com/thirdweb-dev/indexer/configs"
)

var ErrUnauthorized = fmt.Errorf("invalid username or password")

func Authorization(c *gin.Context) {
	if !isBasicAuthEnabled() {
		c.Next()
		return
	}

	username, password, ok := c.Request.BasicAuth()
	if !ok || !validateCredentials(username, password) {
		log.Error(ErrUnauthorized.Error())
		api.UnauthorizedErrorHandler(c, ErrUnauthorized)
		c.Abort()
		return
	}
	c.Next()
}

func isBasicAuthEnabled() bool {
	return config.Cfg.API.BasicAuth.Username != "" && config.Cfg.API.BasicAuth.Password != ""
}

func validateCredentials(username, password string) bool {
	return username == config.Cfg.API.BasicAuth.Username && password == config.Cfg.API.BasicAuth.Password
}
