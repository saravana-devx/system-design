package main

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func md5hash(s string) uint32 {
	sum := md5.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

func newClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:            addr,
		PoolSize:        20,
		MinIdleConns:    5,
		PoolTimeout:     4 * time.Second,
		DialTimeout:     3 * time.Second,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    2 * time.Second,
		MaxRetries:      3,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 512 * time.Millisecond,
	})
}

type RedisConsistentHash struct {
	rdb     *redis.Client
	vnodes  int
	ringKey string
}

func NewRedisConsistentHash(rdb *redis.Client, vnodes int) *RedisConsistentHash {
	return &RedisConsistentHash{rdb: rdb, vnodes: vnodes, ringKey: "ch:ring"}
}

func (r *RedisConsistentHash) AddNode(ctx context.Context, node string) error {
	members := make([]redis.Z, r.vnodes)
	for i := 0; i < r.vnodes; i++ {
		members[i] = redis.Z{
			Score:  float64(md5hash(fmt.Sprintf("%s#%d", node, i))),
			Member: fmt.Sprintf("%s#%d", node, i),
		}
	}
	return r.rdb.ZAdd(ctx, r.ringKey, members...).Err()
}

func (r *RedisConsistentHash) RemoveNode(ctx context.Context, node string) error {
	all, err := r.rdb.ZRange(ctx, r.ringKey, 0, -1).Result()
	if err != nil {
		return err
	}
	var victims []any
	prefix := node + "#"
	for _, m := range all {
		if strings.HasPrefix(m, prefix) {
			victims = append(victims, m)
		}
	}
	if len(victims) > 0 {
		return r.rdb.ZRem(ctx, r.ringKey, victims...).Err()
	}
	return nil
}

func (r *RedisConsistentHash) GetNode(ctx context.Context, key string) (string, error) {
	h := float64(md5hash(key))

	results, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     r.ringKey,
		Start:   fmt.Sprintf("%.0f", h),
		Stop:    "+inf",
		ByScore: true,
		Count:   1,
	}).Result()
	if err != nil {
		return "", err
	}

	if len(results) == 0 {
		results, err = r.rdb.ZRange(ctx, r.ringKey, 0, 0).Result()
		if err != nil || len(results) == 0 {
			return "", fmt.Errorf("ring is empty")
		}
	}

	parts := strings.SplitN(results[0], "#", 2)
	return parts[0], nil
}

type RedisSlotMap struct {
	rdb     *redis.Client
	slots   int
	hashKey string
}

func NewRedisSlotMap(rdb *redis.Client, slots int) *RedisSlotMap {
	return &RedisSlotMap{rdb: rdb, slots: slots, hashKey: "ch:slots"}
}

func (m *RedisSlotMap) slotFor(key string) int {
	return int(md5hash(key)) % m.slots
}

func (m *RedisSlotMap) AssignAll(ctx context.Context, nodes []string) error {
	fields := make(map[string]any, m.slots)
	for s := 0; s < m.slots; s++ {
		fields[fmt.Sprintf("%d", s)] = nodes[s%len(nodes)]
	}
	return m.rdb.HMSet(ctx, m.hashKey, fields).Err()
}

func (m *RedisSlotMap) GetNode(ctx context.Context, key string) (string, error) {
	return m.rdb.HGet(ctx, m.hashKey, fmt.Sprintf("%d", m.slotFor(key))).Result()
}

type RedisReadWriteRouter struct {
	primary  *redis.Client
	replicas []*redis.Client
	next     int
}

func NewRedisReadWriteRouter(primaryAddr string, replicaAddrs []string) *RedisReadWriteRouter {
	replicas := make([]*redis.Client, len(replicaAddrs))
	for i, addr := range replicaAddrs {
		replicas[i] = newClient(addr)
	}
	return &RedisReadWriteRouter{
		primary:  newClient(primaryAddr),
		replicas: replicas,
	}
}

func (r *RedisReadWriteRouter) Close() {
	r.primary.Close()
	for _, rdb := range r.replicas {
		rdb.Close()
	}
}

func (r *RedisReadWriteRouter) Set(ctx context.Context, key, val string) error {
	fmt.Printf("  [primary ] SET %-20s = %q\n", key, val)
	return r.primary.Set(ctx, key, val, 0).Err()
}

func (r *RedisReadWriteRouter) Get(ctx context.Context, key string) (string, error) {
	idx := r.next % len(r.replicas)
	r.next++
	val, err := r.replicas[idx].Get(ctx, key).Result()
	if err == nil {
		fmt.Printf("  [replica-%d] GET %-20s = %q\n", idx+1, key, val)
	}
	return val, err
}

func RunRedisDemo() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := newClient("localhost:6379")
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Println("\n── Redis demo (skipped) ──────────────────────────────────")
		fmt.Printf("   no Redis at localhost:6379: %v\n", err)
		fmt.Println("   start one:  docker run -d -p 6379:6379 redis:alpine")
		return
	}

	rdb.Del(ctx, "ch:ring", "ch:slots")

	fmt.Println("\n── RedisConsistentHash ───────────────────────────────────────")
	ch := NewRedisConsistentHash(rdb, 150)
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		if err := ch.AddNode(ctx, n); err != nil {
			fmt.Printf("   AddNode %s: %v\n", n, err)
			return
		}
		fmt.Printf("   service-1: added %s\n", n)
	}

	ch2 := NewRedisConsistentHash(rdb, 150)
	node, _ := ch2.GetNode(ctx, "user:42")
	fmt.Printf("\n   service-2: GetNode(\"user:42\") -> %s\n", node)

	counts := map[string]int{}
	for i := 0; i < 1_000; i++ {
		n, _ := ch.GetNode(ctx, fmt.Sprintf("key:%d", i))
		counts[n]++
	}
	fmt.Println("\n   distribution (1 000 keys, 3 nodes, 150 vnodes):")
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		fmt.Printf("     %-8s %d (%.1f%%)\n", n, counts[n], float64(counts[n])/10)
	}

	moved := 0
	for i := 0; i < 1_000; i++ {
		n, _ := ch.GetNode(ctx, fmt.Sprintf("key:%d", i))
		if n == "node-C" {
			moved++
		}
	}
	ch.RemoveNode(ctx, "node-C")
	node2, _ := ch2.GetNode(ctx, "user:42")
	fmt.Printf("\n   service-1: removed node-C (%d keys migrate)\n", moved)
	fmt.Printf("   service-2: GetNode(\"user:42\") -> %s  (updated without restart)\n", node2)

	fmt.Println("\n── RedisSlotMap ──────────────────────────────────────────────")
	sm := NewRedisSlotMap(rdb, 16384)
	sm.AssignAll(ctx, []string{"node-A", "node-B", "node-C"})
	sn, _ := sm.GetNode(ctx, "user:42")
	fmt.Printf("   user:42 -> %s  (slot %d / 16384)\n", sn, sm.slotFor("user:42"))

	sm.AssignAll(ctx, []string{"node-A", "node-B", "node-C", "node-D"})
	sn2, _ := sm.GetNode(ctx, "user:42")
	fmt.Printf("   after adding node-D: user:42 -> %s  (slot %d)\n", sn2, sm.slotFor("user:42"))

	fmt.Println("\n── RedisReadWriteRouter ──────────────────────────────────────")
	router := NewRedisReadWriteRouter(
		"localhost:6379",
		[]string{"localhost:6379", "localhost:6379"},
	)
	defer router.Close()

	router.Set(ctx, "session:abc", "user:42")
	router.Set(ctx, "session:xyz", "user:99")
	router.Get(ctx, "session:abc")
	router.Get(ctx, "session:xyz")
	router.Get(ctx, "session:abc")
}
