// SPDX-License-Identifier: Apache-2.0
// Porcupine linearizability test for mem_etcd Raft cluster.
//
// Test flow:
// 1. Start 3 mem_etcd containers (Docker)
// 2. Wait for leader election
// 3. Run N concurrent clients doing random Put/Get/Delete on a small key space
// 4. At t=15s: kill a random node (failover test)
// 5. After 30s: stop all clients
// 6. Run Porcupine linearizability check on recorded operation history
// 7. Verify data consistency across surviving nodes
// 8. Clean up

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/anishathalye/porcupine"
)

// ── Porcupine Model for etcd KV ──────────────────────────────────────────

type OpType int

const (
	OpPut OpType = iota
	OpGet
	OpDelete
)

type OpInput struct {
	Type  OpType
	Key   string
	Value string
}

type OpOutput struct {
	Ok    bool   `json:"ok"`
	Value string `json:"value,omitempty"`
	Err   string `json:"err,omitempty"`
}

// kvModel defines the linearizability model for etcd KV operations.
// State: map[string]string (key -> value)
// - Put(key, value): state[key] = value
// - Get(key): returns state[key] or "<NIL>" if not exists
// - Delete(key): delete(state, key)
// - Failed operations (ok=false) are always valid, state unchanged.
var kvModel = porcupine.Model{
	Init: func() interface{} {
		return make(map[string]string)
	},
	Step: func(state, in, out interface{}) (bool, interface{}) {
		s := state.(map[string]string)
		input := in.(OpInput)
		output := out.(OpOutput)

		if !output.Ok {
			// Failed operation: valid in any linearization, state unchanged
			return true, s
		}

		switch input.Type {
		case OpPut:
			s[input.Key] = input.Value
			return true, s
		case OpGet:
			expected, exists := s[input.Key]
			if !exists {
				return output.Value == "<NIL>", s
			}
			return output.Value == expected, s
		case OpDelete:
			delete(s, input.Key)
			return true, s
		}
		return false, s
	},
	// Partition by key for faster checking (P-compositionality)
	Partition: func(ops []porcupine.Operation) [][]porcupine.Operation {
		groups := make(map[string][]porcupine.Operation)
		for _, op := range ops {
			input := op.Input.(OpInput)
			groups[input.Key] = append(groups[input.Key], op)
		}
		result := make([][]porcupine.Operation, 0, len(groups))
		for _, ops := range groups {
			result = append(result, ops)
		}
		return result
	},
}

// ── Test Configuration ───────────────────────────────────────────────────

var (
	numKeys       = flag.Int("keys", 10, "Number of keys in the key space")
	numClients    = flag.Int("clients", 8, "Number of concurrent client goroutines")
	testDuration  = flag.Duration("duration", 30*time.Second, "Test duration")
	killAt        = flag.Duration("kill-at", 15*time.Second, "When to kill a node")
	opRate        = flag.Int("rate", 200, "Total operations per second")
	imageName     = flag.String("image", "mem_etcd-raft", "Docker image name")
	keepCluster   = flag.Bool("keep", false, "Keep cluster running after test")
	skipSetup     = flag.Bool("skip-setup", false, "Skip cluster setup (use existing)")
)

var (
	endpoints  = []string{"http://localhost:23791", "http://localhost:23792", "http://localhost:23793"}
	containers = []string{"raft1", "raft2", "raft3"}
)

// ── Leader Tracker ───────────────────────────────────────────────────────

type LeaderTracker struct {
	mu sync.RWMutex
	ep string
}

func (lt *LeaderTracker) Get() string {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.ep
}

func (lt *LeaderTracker) Update() {
	for _, ep := range endpoints {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			DialTimeout: 2 * time.Second,
		})
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = cli.Put(ctx, "__probe__", "1")
		cancel()
		cli.Close()
		if err == nil {
			lt.mu.Lock()
			if lt.ep != ep {
				log.Printf("[leader] Leader changed: %s -> %s", lt.ep, ep)
			}
			lt.ep = ep
			lt.mu.Unlock()
			return
		}
	}
	log.Printf("[leader] No leader found")
	lt.mu.Lock()
	lt.ep = ""
	lt.mu.Unlock()
}

// ── Client Worker ─────────────────────────────────────────────────────────

func clientWorker(
	id int,
	lt *LeaderTracker,
	stop <-chan struct{},
	ticker *time.Ticker,
	ops *[]porcupine.Operation,
	opsMu *sync.Mutex,
	stats *stats,
) {
	rng := rand.New(rand.NewSource(int64(id)*12345 + time.Now().UnixNano()))

	// Create persistent client connected to leader endpoint
	leaderEp := lt.Get()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{leaderEp},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Printf("[client %d] Failed to create client: %v", id, err)
		return
	}
	defer cli.Close()

	keyPrefix := "key"
	opTimeout := 3 * time.Second

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		// Update endpoint if leader changed
		currentLeader := lt.Get()
		if currentLeader != "" {
			eps := cli.Endpoints()
			if len(eps) == 0 || eps[0] != currentLeader {
				cli.SetEndpoints(currentLeader)
			}
		} else {
			// No leader, try to discover
			lt.Update()
			currentLeader = lt.Get()
			if currentLeader == "" {
				// Still no leader, record failed op
				input := OpInput{Type: OpPut, Key: fmt.Sprintf("%s%d", keyPrefix, rng.Intn(*numKeys))}
				output := OpOutput{Ok: false, Err: "no leader"}
				callTime := time.Now().UnixNano()
				opsMu.Lock()
				*ops = append(*ops, porcupine.Operation{
					Input: input, Call: callTime, Output: output, Ret: time.Now().UnixNano(),
				})
				stats.record(false)
				opsMu.Unlock()
				continue
			}
			cli.SetEndpoints(currentLeader)
		}

		// Pick random operation: 55% Put, 35% Get, 10% Delete
		key := fmt.Sprintf("%s%d", keyPrefix, rng.Intn(*numKeys))
		opType := rng.Intn(20)
		input := OpInput{Key: key}
		output := OpOutput{}
		callTime := time.Now().UnixNano()

		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)

		switch {
		case opType < 11: // 55% Put
			input.Type = OpPut
			input.Value = fmt.Sprintf("v%d", rng.Intn(100000))
			_, err := cli.Put(ctx, key, input.Value)
			if err != nil {
				output.Ok = false
				output.Err = truncateErr(err.Error())
				if isNotLeader(err) {
					lt.Update()
				}
			} else {
				output.Ok = true
			}

		case opType < 18: // 35% Get
			input.Type = OpGet
			resp, err := cli.Get(ctx, key)
			if err != nil {
				output.Ok = false
				output.Err = truncateErr(err.Error())
				if isNotLeader(err) {
					lt.Update()
				}
			} else {
				output.Ok = true
				if len(resp.Kvs) == 0 {
					output.Value = "<NIL>"
				} else {
					output.Value = string(resp.Kvs[0].Value)
				}
			}

		default: // 10% Delete
			input.Type = OpDelete
			_, err := cli.Delete(ctx, key)
			if err != nil {
				output.Ok = false
				output.Err = truncateErr(err.Error())
				if isNotLeader(err) {
					lt.Update()
				}
			} else {
				output.Ok = true
			}
		}
		cancel()

		retTime := time.Now().UnixNano()
		opsMu.Lock()
		*ops = append(*ops, porcupine.Operation{
			Input: input, Call: callTime, Output: output, Ret: retTime,
		})
		stats.record(output.Ok)
		opsMu.Unlock()
	}
}

func isNotLeader(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "leadership") ||
		strings.Contains(msg, "Unavailable") ||
		strings.Contains(msg, "connection refused")
}

func truncateErr(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// ── Stats ────────────────────────────────────────────────────────────────

type stats struct {
	mu       sync.Mutex
	success  int
	fail     int
}

func (s *stats) record(ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.success++
	} else {
		s.fail++
	}
}

func (s *stats) snapshot() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.success, s.fail
}

// ── Docker Cluster Management ────────────────────────────────────────────

func setupCluster() error {
	// Clean up any existing cluster
	exec.Command("docker", "rm", "-f", "raft1", "raft2", "raft3").Run()
	exec.Command("docker", "network", "rm", "raft-net").Run()

	// Create network
	if err := exec.Command("docker", "network", "create", "raft-net").Run(); err != nil {
		return fmt.Errorf("create network: %w", err)
	}
	log.Println("[setup] Docker network 'raft-net' created")

	peers := "1@raft1:2379,2@raft2:2379,3@raft3:2379"
	configs := []struct {
		name   string
		port   string
		mport  string
		nodeID string
		init   bool
	}{
		{"raft1", "23791", "9001", "1", true},
		{"raft2", "23792", "9002", "2", false},
		{"raft3", "23793", "9003", "3", false},
	}

	for _, c := range configs {
		args := []string{"run", "-d", "--name", c.name, "--network", "raft-net",
			"-p", c.port + ":2379", "-p", c.mport + ":9000",
			*imageName,
			"--raft-enabled", "--raft-node-id", c.nodeID,
			"--raft-peers", peers}
		if c.init {
			args = append(args, "--raft-init")
		}
		args = append(args, "--port", "2379", "--metrics-port", "9000", "--wal-dir", "/tmp/wal"+c.nodeID)

		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("start %s: %w\n%s", c.name, err, string(out))
		}
		log.Printf("[setup] Started %s (node %s, port %s)", c.name, c.nodeID, c.port)
	}

	return nil
}

func cleanupCluster() {
	for _, name := range containers {
		exec.Command("docker", "rm", "-f", name).Run()
	}
	exec.Command("docker", "network", "rm", "raft-net").Run()
	log.Println("[cleanup] Cluster removed")
}

// ── Data Consistency Check ───────────────────────────────────────────────

func checkConsistency() error {
	type nodeData struct {
		name string
		data map[string]string
	}

	var nodes []nodeData
	for i, name := range containers {
		// Skip dead containers
		if err := exec.Command("docker", "inspect", name).Run(); err != nil {
			log.Printf("[consistency] Skipping %s (not running)", name)
			continue
		}

		ep := endpoints[i]
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			DialTimeout:  2 * time.Second,
		})
		if err != nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := cli.Get(ctx, "key", clientv3.WithPrefix(), clientv3.WithLimit(int64(*numKeys+10)))
		cancel()
		cli.Close()
		if err != nil {
			log.Printf("[consistency] Failed to read from %s: %v", name, err)
			continue
		}

		data := make(map[string]string)
		for _, kv := range resp.Kvs {
			if string(kv.Key) == "__probe__" {
				continue
			}
			data[string(kv.Key)] = string(kv.Value)
		}
		nodes = append(nodes, nodeData{name: name, data: data})
		log.Printf("[consistency] %s: %d keys", name, len(data))
	}

	if len(nodes) < 2 {
		return fmt.Errorf("not enough surviving nodes to compare")
	}

	// Compare all nodes
	base := nodes[0]
	for i := 1; i < len(nodes); i++ {
		if !mapsEqual(base.data, nodes[i].data) {
			log.Printf("[consistency] MISMATCH between %s and %s:", base.name, nodes[i].name)
			for k, v := range base.data {
				if v2, ok := nodes[i].data[k]; !ok || v != v2 {
					log.Printf("  key %s: %s=%q vs %s=%q", k, base.name, v, nodes[i].name, v2)
				}
			}
			for k, v := range nodes[i].data {
				if v2, ok := base.data[k]; !ok || v != v2 {
					log.Printf("  key %s: %s=%q vs %s=%q", k, nodes[i].name, v, base.name, v2)
				}
			}
			return fmt.Errorf("data inconsistency between %s and %s", base.name, nodes[i].name)
		}
	}

	log.Printf("[consistency] All %d surviving nodes have consistent data (%d keys)", len(nodes), len(base.data))
	return nil
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// ── Main ─────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	// 1. Setup cluster
	if !*skipSetup {
		log.Println("=== Setting up 3-node cluster ===")
		if err := setupCluster(); err != nil {
			log.Fatalf("Setup failed: %v", err)
		}
		if !*keepCluster {
			defer cleanupCluster()
		}
	} else {
		log.Println("=== Skipping cluster setup (using existing) ===")
	}

	// 2. Wait for leader election
	log.Println("Waiting for leader election...")
	time.Sleep(8 * time.Second)

	lt := &LeaderTracker{}
	lt.Update()
	if lt.Get() == "" {
		log.Println("No leader found, retrying in 5s...")
		time.Sleep(5 * time.Second)
		lt.Update()
		if lt.Get() == "" {
			log.Fatalf("No leader found after 13s")
		}
	}
	log.Printf("Initial leader: %s", lt.Get())

	// 3. Run test
	ratePerClient := *opRate / *numClients
	tickerInterval := time.Second / time.Duration(ratePerClient)
	if tickerInterval < 5*time.Millisecond {
		tickerInterval = 5 * time.Millisecond
	}
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	var ops []porcupine.Operation
	var opsMu sync.Mutex
	st := &stats{}
	stop := make(chan struct{})

	log.Printf("=== Starting test: %d clients, %v duration, kill at %v, %d ops/s ===",
		*numClients, *testDuration, *killAt, *opRate)

	var wg sync.WaitGroup
	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientWorker(id, lt, stop, ticker, &ops, &opsMu, st)
		}(i)
	}

	// Progress logger
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				opsMu.Lock()
				opCount := len(ops)
				opsMu.Unlock()
				s, f := st.snapshot()
				log.Printf("[progress] %d ops (%d ok, %d fail)", opCount, s+f, f)
			}
		}
	}()

	// Kill the LEADER at killAt (forces real failover test)
	killedNode := -1
	time.AfterFunc(*killAt, func() {
		// Find which container is the leader by matching endpoint to container index
		leaderEp := lt.Get()
		for i, ep := range endpoints {
			if ep == leaderEp {
				killedNode = i
				break
			}
		}
		if killedNode == -1 {
			killedNode = rand.Intn(len(containers))
		}
		log.Printf("[failover] Killing LEADER container: %s (endpoint: %s)", containers[killedNode], leaderEp)
		exec.Command("docker", "stop", containers[killedNode]).Run()
		log.Printf("[failover] Waiting for re-election...")

		// Wait for re-election
		time.Sleep(3 * time.Second)
		lt.Update()
		if lt.Get() != "" {
			log.Printf("[failover] New leader: %s", lt.Get())
		} else {
			log.Printf("[failover] No leader after 3s, waiting 5 more...")
			time.Sleep(5 * time.Second)
			lt.Update()
			if lt.Get() != "" {
				log.Printf("[failover] New leader: %s", lt.Get())
			} else {
				log.Printf("[failover] WARNING: No leader after 8s")
			}
		}
	})

	// Wait for test duration
	time.Sleep(*testDuration)
	close(stop)
	wg.Wait()

	// 4. Stats
	totalOps := len(ops)
	s, f := st.snapshot()
	log.Printf("=== Test complete: %d ops recorded (%d success, %d failed) ===", totalOps, s, f)

	if killedNode >= 0 {
		log.Printf("Killed node: %s (container %s)", containers[killedNode], containers[killedNode])
	}

	// 5. Linearizability check
	log.Println("=== Checking linearizability (timeout: 5min) ===")
	start := time.Now()
	result := porcupine.CheckOperationsTimeout(kvModel, ops, 5*time.Minute)
	elapsed := time.Since(start)

	if result == porcupine.Ok {
		log.Printf("✅ PASS: Linearizability verified! (%d ops checked in %v)", totalOps, elapsed)
	} else if result == porcupine.Unknown {
		log.Printf("⏰ TIMEOUT: Linearizability check timed out (%d ops in %v)", totalOps, elapsed)
	} else {
		log.Printf("❌ FAIL: Linearizability VIOLATED! (%d ops checked in %v)", totalOps, elapsed)

		// Save history for analysis
		historyFile := "/tmp/porcupine_history.json"
		data, _ := json.MarshalIndent(ops, "", "  ")
		os.WriteFile(historyFile, data, 0644)
		log.Printf("Operation history saved to %s", historyFile)

		// Print first few failed operations for quick analysis
		printProblemOps(ops)
	}

	// 6. Data consistency check
	log.Println("=== Checking data consistency ===")
	if err := checkConsistency(); err != nil {
		log.Printf("❌ Data inconsistency: %v", err)
	} else {
		log.Println("✅ Data consistency verified")
	}

	log.Println("=== Done ===")
}

func printProblemOps(ops []porcupine.Operation) {
	// Print successful operations that might be problematic
	log.Println("--- Last 10 successful operations ---")
	count := 0
	for i := len(ops) - 1; i >= 0 && count < 10; i-- {
		op := ops[i]
		out := op.Output.(OpOutput)
		if out.Ok {
			in := op.Input.(OpInput)
			log.Printf("  [%d] %s key=%s val=%q -> ok=%v val=%q",
				op.Call, opTypeName(in.Type), in.Key, in.Value, out.Ok, out.Value)
			count++
		}
	}
}

func opTypeName(t OpType) string {
	switch t {
	case OpPut:
		return "PUT"
	case OpGet:
		return "GET"
	case OpDelete:
		return "DEL"
	default:
		return "???"
	}
}
