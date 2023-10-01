package router

import (
	"net/http"
)

type Router interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) Route
}

type Route interface {
	Methods(methods ...string) Route
}
