// Package fakegh is an in-memory stand-in for the GitHub REST calls the gate
// makes, for tests. It serves JSON values registered by path.
package fakegh

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
)

// Server is a fake GitHub API.
type Server struct {
	*httptest.Server

	mu   sync.Mutex
	json map[string]any // "GET /repos/o/r/pulls/1" → value
	fail map[string]int // "GET /path" → status code
}

// New starts a fake API. Close it when done.
func New() *Server {
	s := &Server{
		json: map[string]any{},
		fail: map[string]int{},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// JSON serves v at "METHOD /path".
func (s *Server) JSON(route string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.json[route] = v
}

// Fail answers "METHOD /path" with code.
func (s *Server) Fail(route string, code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail[route] = code
}

// Unfail stops answering "METHOD /path" with an injected failure.
func (s *Server) Unfail(route string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.fail, route)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	route := r.Method + " " + r.URL.Path
	if code, ok := s.fail[route]; ok {
		w.WriteHeader(code)
		_, _ = io.WriteString(w, `{"message":"injected failure"}`)
		return
	}
	v, ok := s.json[route]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"message":"Not Found"}`)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
