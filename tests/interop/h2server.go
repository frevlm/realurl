// h2server.go — plain-HTTP/2 (h2c) interop peer for realurl's tests.
//
// Go 1.24+ serves unencrypted HTTP/2 natively via http.Server.Protocols.
// This server is intentionally not part of ctest: run it by hand and point
// examples/fetch or client_test at it to cross-check HPACK/Huffman decoding
// against a real implementation.
//
//	go run tests/interop/h2server.go 9090
//	./build/examples/realurl_fetch http://127.0.0.1:9090/hello
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {
	port := "9090"
	if len(os.Args) > 1 {
		if _, err := strconv.Atoi(os.Args[1]); err == nil {
			port = os.Args[1]
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("x-interop", "go")
		w.Header().Set("content-type", "text/plain")
		fmt.Fprintf(w, "method=%s path=%s proto=%s body_len=%d", r.Method, r.URL.Path, r.Proto, len(body))
	})

	server := &http.Server{
		Addr:    "127.0.0.1:" + port,
		Handler: mux,
	}
	server.Protocols = new(http.Protocols)
	server.Protocols.SetUnencryptedHTTP2(true)

	log.Printf("h2c interop server on http://127.0.0.1:%s", port)
	log.Fatal(server.ListenAndServe())
}
