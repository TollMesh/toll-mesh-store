// Command tollmeshcache runs a single TollMeshCache node, serving the
// HTTP API used by every language SDK.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	advertiseAddr := flag.String("advertise-addr", "", "host:port other nodes should use to reach this node's HTTP API for gossip (default: derived from -http-addr with host \"localhost\")")
	join := flag.String("join", "", "comma-separated host:port list of existing nodes' HTTP APIs to join at startup")
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
	coordinator.RegisterStateMerger(ms.MergeState)
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

	if *join != "" {
		selfAddr, selfPort, err := resolveAdvertiseAddr(*advertiseAddr, *httpAddr)
		if err != nil {
			log.Fatalf("cannot determine advertise address for -join: %v (pass -advertise-addr explicitly)", err)
		}
		for _, target := range strings.Split(*join, ",") {
			target = strings.TrimSpace(target)
			if target == "" {
				continue
			}
			if err := joinCluster(target, selfAddr, selfPort, coordinator); err != nil {
				log.Printf("failed to join %s: %v", target, err)
			} else {
				log.Printf("joined cluster via %s", target)
			}
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	if err := httpServer.Stop(); err != nil {
		log.Printf("error stopping http server: %v", err)
	}
}

// resolveAdvertiseAddr determines the host:port this node should tell
// peers to reach it at. If advertiseAddr is set explicitly, it's used
// as-is; otherwise it's derived from httpAddr (e.g. ":8080" -> host
// defaults to "localhost", "0.0.0.0:8080" -> host defaults to "localhost"
// too, since 0.0.0.0 isn't a routable address to hand to a peer).
func resolveAdvertiseAddr(advertiseAddr, httpAddr string) (string, int, error) {
	if advertiseAddr != "" {
		host, portStr, err := splitHostPort(advertiseAddr)
		if err != nil {
			return "", 0, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return "", 0, fmt.Errorf("invalid port in -advertise-addr %q: %w", advertiseAddr, err)
		}
		return host, port, nil
	}

	host, portStr, err := splitHostPort(httpAddr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in -http-addr %q: %w", httpAddr, err)
	}
	if host == "" || host == "0.0.0.0" {
		host = "localhost"
	}
	return host, port, nil
}

func splitHostPort(addr string) (host, port string, err error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("expected host:port, got %q", addr)
	}
	return addr[:idx], addr[idx+1:], nil
}

// joinCluster announces this node to target's /internal/peers/join, adds
// target as a peer, and adds every peer target told us about too, so a
// single -join address is enough to discover (and be discovered by) the
// rest of an already-formed cluster.
func joinCluster(target, selfAddr string, selfPort int, coordinator *coordination.GossipCoordinator) error {
	host, portStr, err := splitHostPort(target)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port in join target %q: %w", target, err)
	}

	body, _ := json.Marshal(api.PeerJoinRequest{Address: selfAddr, Port: selfPort})
	url := fmt.Sprintf("http://%s/internal/peers/join", target)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join request to %s returned status %d", target, resp.StatusCode)
	}

	var result struct {
		Peers []struct {
			ID      string `json:"id"`
			Address string `json:"address"`
			Port    int    `json:"port"`
		} `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding join response from %s: %w", target, err)
	}

	targetID := host + ":" + strconv.Itoa(port)
	if err := coordinator.AddPeer(&core.Node{ID: targetID, Address: host, Port: port}); err != nil {
		return err
	}
	for _, p := range result.Peers {
		if p.ID == targetID {
			continue
		}
		_ = coordinator.AddPeer(&core.Node{ID: p.ID, Address: p.Address, Port: p.Port})
	}

	return nil
}
