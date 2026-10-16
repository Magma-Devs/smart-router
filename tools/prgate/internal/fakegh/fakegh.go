// Package fakegh is an in-memory stand-in for the GitHub REST calls the gates
// make, for tests. It serves JSON values registered by path, and keeps the
// commit statuses posted to it.
package fakegh

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// Status is a commit status as the fake stores it.
type Status struct {
	State       string `json:"state"`
	Context     string `json:"context"`
	Description string `json:"description"`
	TargetURL   string `json:"target_url,omitempty"`
}

// Server is a fake GitHub API.
type Server struct {
	*httptest.Server

	mu       sync.Mutex
	json     map[string]any               // "GET /repos/o/r/pulls/1" → value
	fail     map[string]int               // "GET /path" → status code
	statuses map[string]map[string]Status // sha → context → latest
	posts    []Status
}

// New starts a fake API. Close it when done.
func New() *Server {
	s := &Server{
		json:     map[string]any{},
		fail:     map[string]int{},
		statuses: map[string]map[string]Status{},
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

// Posts returns the statuses posted so far, oldest first.
func (s *Server) Posts() []Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Status(nil), s.posts...)
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
	switch {
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/statuses/"):
		var st Status
		if err := json.NewDecoder(r.Body).Decode(&st); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sha := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		if s.statuses[sha] == nil {
			s.statuses[sha] = map[string]Status{}
		}
		s.statuses[sha][st.Context] = st
		s.posts = append(s.posts, st)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "{}")
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status") && strings.Contains(r.URL.Path, "/commits/"):
		sha := strings.TrimSuffix(r.URL.Path[strings.Index(r.URL.Path, "/commits/")+len("/commits/"):], "/status")
		list := []Status{}
		for _, st := range s.statuses[sha] {
			list = append(list, st)
		}
		writeJSON(w, map[string]any{"statuses": list})
	default:
		v, ok := s.json[route]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"message":"Not Found"}`)
			return
		}
		writeJSON(w, v)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
