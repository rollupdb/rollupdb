package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	urlpath "path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rollupdb/rollupdb"
)

const MaxJSONBody = 1 << 20
const MaxIngestBody = 8 << 20
const RequestTimeout = 30 * time.Second

// SnapshotTimeout replaces RequestTimeout and the server's write timeout for
// GET /v1/snapshot, which exports and then sends the whole database. It covers
// both steps together.
const SnapshotTimeout = 30 * time.Minute

type Problem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, code, message string) {
	jsonResponse(w, status, struct {
		Error Problem `json:"error"`
	}{Problem{code, message}})
}
func engineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, rollupdb.ErrStreamNotFound):
		problem(w, 404, "stream_not_found", "stream not found")
	case errors.Is(err, rollupdb.ErrStreamExists):
		problem(w, 409, "stream_exists", "stream already exists")
	case errors.Is(err, rollupdb.ErrPartialBucket):
		problem(w, 422, "partial_bucket", "requested range cuts a retained bucket")
	case errors.Is(err, rollupdb.ErrInvalidRequest):
		problem(w, 400, "invalid_request", "invalid engine request")
	case errors.Is(err, rollupdb.ErrStreamNotActive):
		problem(w, 409, "stream_not_active", "stream is not active at the host clock")
	case errors.Is(err, rollupdb.ErrClockJump):
		problem(w, 409, "clock_jump", "host clock is too far ahead of the stream's stored watermark; nothing was written")
	case errors.Is(err, rollupdb.ErrNumericOverflow):
		problem(w, 422, "numeric_overflow", "combined numerical state is unrepresentable")
	case errors.Is(err, context.DeadlineExceeded):
		problem(w, 504, "deadline_exceeded", "request deadline exceeded; a missing acknowledgment does not prove rollback")
	case errors.Is(err, context.Canceled):
		problem(w, 408, "canceled", "request canceled; a missing acknowledgment does not prove rollback")
	case errors.Is(err, rollupdb.ErrClosed):
		problem(w, 503, "closed", "database is closed")
	default:
		problem(w, 500, "engine_error", "engine operation failed; inspect the database before retrying an unacknowledged write")
	}
}

func decodeRequest(w http.ResponseWriter, r *http.Request, target any, limit int64) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		problem(w, 415, "content_type", "Content-Type must be application/json")
		return false
	}
	if r.Header.Get("Content-Encoding") != "" && r.Header.Get("Content-Encoding") != "identity" {
		problem(w, 415, "content_encoding", "compressed request bodies are unsupported")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	encoded, err := io.ReadAll(r.Body)
	if err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			problem(w, 413, "body_too_large", fmt.Sprintf("body exceeds %d bytes", limit))
		} else {
			problem(w, 400, "body_read", "could not read complete request body")
		}
		return false
	}
	if err = DecodeObject(encoded, target); err != nil {
		problem(w, 400, "invalid_json", err.Error())
		return false
	}
	return true
}

func localHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// NewHandler is intended for a loopback listener, not Internet exposure. Browser
// Origin requests and nonlocal Host values are refused; no CORS is enabled.
// The caller owns db and must keep it open until all handlers have stopped.
func NewHandler(db *rollupdb.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/info", func(w http.ResponseWriter, r *http.Request) {
		value, err := db.Info(r.Context())
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, value)
	})
	mux.HandleFunc("POST /v1/streams", func(w http.ResponseWriter, r *http.Request) {
		var d rollupdb.Definition
		if !decodeRequest(w, r, &d, MaxJSONBody) {
			return
		}
		if err := d.Validate(); err != nil {
			problem(w, 400, "invalid_definition", err.Error())
			return
		}
		if err := db.CreateStream(r.Context(), d); err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 201, struct {
			Name    string `json:"name"`
			Version int    `json:"version"`
		}{d.Name, d.Version})
	})
	mux.HandleFunc("GET /v1/streams", func(w http.ResponseWriter, r *http.Request) {
		names, err := db.Streams(r.Context())
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, struct {
			Streams []string `json:"streams"`
		}{append([]string{}, names...)})
	})
	mux.HandleFunc("POST /v1/advance", func(w http.ResponseWriter, r *http.Request) {
		var empty struct{}
		if !decodeRequest(w, r, &empty, MaxJSONBody) {
			return
		}
		results, err := db.AdvanceAll(r.Context())
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, results)
	})
	mux.HandleFunc("GET /v1/streams/{name}", func(w http.ResponseWriter, r *http.Request) {
		value, err := db.Stream(r.Context(), r.PathValue("name"))
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, value)
	})
	mux.HandleFunc("GET /v1/streams/{name}/stats", func(w http.ResponseWriter, r *http.Request) {
		value, err := db.Status(r.Context(), r.PathValue("name"))
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, value)
	})
	mux.HandleFunc("POST /v1/ingest", func(w http.ResponseWriter, r *http.Request) {
		var batch rollupdb.Batch
		if !decodeRequest(w, r, &batch, MaxIngestBody) {
			return
		}
		value, err := db.Ingest(r.Context(), batch)
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, value)
	})
	mux.HandleFunc("POST /v1/query", func(w http.ResponseWriter, r *http.Request) {
		var q rollupdb.Query
		if !decodeRequest(w, r, &q, MaxJSONBody) {
			return
		}
		value, err := db.Query(r.Context(), q)
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, value)
	})
	mux.HandleFunc("POST /v1/streams/{name}/advance", func(w http.ResponseWriter, r *http.Request) {
		var empty struct{}
		if !decodeRequest(w, r, &empty, MaxJSONBody) {
			return
		}
		value, err := db.Advance(r.Context(), r.PathValue("name"))
		if err != nil {
			engineError(w, err)
			return
		}
		jsonResponse(w, 200, value)
	})
	mux.HandleFunc(snapshotRoute, func(w http.ResponseWriter, r *http.Request) {
		// The server's 60-second write timeout is for ordinary responses; a
		// large export plus its download needs the snapshot budget instead.
		// Writers that can't set deadlines (such as test recorders) don't need it.
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(SnapshotTimeout))
		dir, err := os.MkdirTemp("", "rollupdb-http-snapshot-")
		if err != nil {
			engineError(w, err)
			return
		}
		defer os.RemoveAll(dir)
		path := filepath.Join(dir, "snapshot.rollupdb")
		if err = db.Snapshot(r.Context(), path); err != nil {
			engineError(w, err)
			return
		}
		f, err := os.Open(path)
		if err != nil {
			engineError(w, err)
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			engineError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="snapshot.rollupdb"`)
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		// A failed download is partial output, not a usable backup. No range resume:
		// another request creates another snapshot and may contain newer state.
		_, _ = io.Copy(w, f)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if !localHost(r.Host) || r.Header.Get("Origin") != "" {
			problem(w, 403, "local_client_required", "use a local non-browser client with a loopback Host")
			return
		}
		if r.URL.Path != urlpath.Clean(r.URL.Path) {
			problem(w, 400, "invalid_path", "use a canonical endpoint path")
			return
		}
		if r.URL.RawQuery != "" {
			problem(w, 400, "query_parameters", "query parameters are unsupported")
			return
		}
		// Handle fallback here so unknown routes/methods also use the JSON contract.
		_, pattern := mux.Handler(r)
		if pattern == "" {
			if strings.HasPrefix(r.URL.Path, "/v1/") {
				// ServeMux distinguishes a known path with an unsupported method. Resolve
				// using the registered methods without invoking any mutating handler.
				for _, method := range []string{http.MethodGet, http.MethodPost} {
					probe := r.Clone(r.Context())
					probe.Method = method
					if _, p := mux.Handler(probe); p != "" {
						w.Header().Set("Allow", method)
						problem(w, 405, "method_not_allowed", "method not allowed")
						return
					}
				}
			}
			problem(w, 404, "not_found", "endpoint not found")
			return
		}
		// ServeHTTP sets path wildcard values; Handler alone does not.
		timeout := RequestTimeout
		if pattern == snapshotRoute {
			timeout = SnapshotTimeout
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

const snapshotRoute = "GET /v1/snapshot"

// Listen refuses nonliteral/nonloopback addresses, even if explicitly supplied.
func Listen(address string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, errors.New("listen address must use a literal loopback IP")
	}
	return net.Listen("tcp", address)
}

// Serve waits for graceful shutdown before returning; the caller can then close
// db. Cancellation also cancels active request contexts. It never closes db.
func Serve(ctx context.Context, db *rollupdb.DB, listener net.Listener) error {
	server := &http.Server{Handler: NewHandler(db), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := server.Shutdown(shutdown)
		if err != nil {
			_ = server.Close()
		}
		serveErr := <-done
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.Join(err, serveErr)
		}
		return err
	}
}
