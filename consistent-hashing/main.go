package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
)

type vnode struct {
	hash uint32
	node string
}

type ConsistentHash struct {
	vnodes  int
	ring    []vnode
	nodeSet map[string]bool
}

func NewConsistentHash(vnodes int) *ConsistentHash {
	return &ConsistentHash{
		vnodes:  vnodes,
		ring:    []vnode{},
		nodeSet: make(map[string]bool),
	}
}

func (ch *ConsistentHash) hash(s string) uint32 {
	sum := md5.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

func (ch *ConsistentHash) AddNode(node string) {
	if ch.nodeSet[node] {
		return
	}
	ch.nodeSet[node] = true

	for i := 0; i < ch.vnodes; i++ {
		h := ch.hash(fmt.Sprintf("%s#%d", node, i))
		ch.ring = append(ch.ring, vnode{hash: h, node: node})
	}

	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].hash < ch.ring[j].hash
	})
}

func (ch *ConsistentHash) RemoveNode(node string) {
	if !ch.nodeSet[node] {
		return
	}
	delete(ch.nodeSet, node)
	kept := ch.ring[:0]
	for _, v := range ch.ring {
		if v.node != node {
			kept = append(kept, v)
		}
	}
	ch.ring = kept
}

func (ch *ConsistentHash) GetNode(key string) string {
	if len(ch.ring) == 0 {
		return ""
	}
	h := ch.hash(key)
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i].hash >= h
	})
	if idx == len(ch.ring) {
		idx = 0
	}
	return ch.ring[idx].node
}

type SlotMap struct {
	slots int
	owner []string
}

func NewSlotMap(slots int) *SlotMap {
	return &SlotMap{slots: slots, owner: make([]string, slots)}
}

func (m *SlotMap) slot(key string) int {
	sum := md5.Sum([]byte(key))
	return int(binary.BigEndian.Uint32(sum[:4])) % m.slots
}

func (m *SlotMap) AssignAll(nodes []string) {
	for s := 0; s < m.slots; s++ {
		m.owner[s] = nodes[s%len(nodes)]
	}
}

func (m *SlotMap) GetNode(key string) string {
	return m.owner[m.slot(key)]
}

type DB interface {
	Exec(query string) string
}

type FakeDB struct{ Name string }

func (d FakeDB) Exec(query string) string {
	return fmt.Sprintf("[%s] ran: %s", d.Name, query)
}

type ReadWriteRouter struct {
	primary  DB
	replicas []DB
	next     int
}

func NewReadWriteRouter(primary DB, replicas []DB) *ReadWriteRouter {
	return &ReadWriteRouter{primary: primary, replicas: replicas}
}

func (r *ReadWriteRouter) Write(q string) string {
	return r.primary.Exec(q)
}

func (r *ReadWriteRouter) Read(q string) string {
	db := r.replicas[r.next%len(r.replicas)]
	r.next++
	return db.Exec(q)
}

func main() {
	ch := NewConsistentHash(150)
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		ch.AddNode(n)
	}

	fmt.Printf("user:42 -> %s\n", ch.GetNode("user:42"))

	counts := map[string]int{}
	total := 100_000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		counts[ch.GetNode(key)]++
	}
	fmt.Printf("distribution (3 nodes, 150 vnodes)\n")
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		pct := float64(counts[n]) / float64(total) * 100
		fmt.Printf("%s: %d (%.1f%%)\n", n, counts[n], pct)
	}

	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		before := ch.GetNode(key)
		_ = before
		if before == "node-C" {
			moved++
		}
	}
	ch.RemoveNode("node-C")
	fmt.Printf("removed node-C -> moved %d (%.1f%%)\n", moved, float64(moved)/float64(total)*100)

	sm := NewSlotMap(16384)
	sm.AssignAll([]string{"node-A", "node-B", "node-C"})
	fmt.Printf("slotmap user:42 -> %s\n", sm.GetNode("user:42"))

	router := NewReadWriteRouter(
		FakeDB{Name: "primary"},
		[]DB{FakeDB{Name: "replica-1"}, FakeDB{Name: "replica-2"}},
	)
	fmt.Println(router.Write("INSERT ..."))
	fmt.Println(router.Read("SELECT ..."))
	fmt.Println(router.Read("SELECT ..."))

}
