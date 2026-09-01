// Command tollmeshcache runs a single TollMeshCache node, serving the
// HTTP API used by every language SDK.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/toll-mesh/store/api"
	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/store"
)

func main() {
	nodeName := flag.String("node", "node-1", "unique name for this node")
	bindAddr := flag.String("bind-addr", "0.0.0.0", "address to bind gossip coordination on")
	bindPort := flag.Int("bind-port", 7946, "port to bind gossip coordination on")
	httpAddr := flag.String("http-addr", ":8080", "address for the HTTP API to listen on")
	flag.Parse()

	config := &core.ClusterConfig{
		NodeName:      *nodeName,
		BindAddr:      *bindAddr,
		BindPort:      *bindPort,
		AdvertiseAddr: *bindAddr,
		AdvertisePort: *bindPort,
	}

	ms, err := store.NewMeshStore(config)
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	defer ms.Close()

	coordinator := coordination.NewGossipCoordinator(config, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("failed to start coordinator: %v", err)
	}

	httpServer := api.NewHTTPServer(*httpAddr, ms, coordinator)

	go func() {
		log.Printf("tollmeshcache node %q listening on %s (gossip on %s:%d)", *nodeName, *httpAddr, *bindAddr, *bindPort)
		if err := httpServer.Start(); err != nil {
			log.Printf("http server stopped: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	if err := httpServer.Stop(); err != nil {
		log.Printf("error stopping http server: %v", err)
	}
}
