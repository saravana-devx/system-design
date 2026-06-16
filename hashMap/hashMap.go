package main

import (
	"fmt"
	"hash/fnv"
)

type HashTable struct {
	size  int
	table []string
}

func NewHashTable(size int) *HashTable {
	return &HashTable{
		size:  size,
		table: make([]string, size),
	}
}

func (ht *HashTable) hashFunction(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % ht.size
}

func (ht *HashTable) Insert(key string, value string) {
	index := ht.hashFunction(key)
	ht.table[index] = value
}

func (ht *HashTable) Get(key string) string {
	index := ht.hashFunction(key)
	return ht.table[index]
}

func main() {
	ht := NewHashTable(10)

	ht.Insert("key1", "value1")
	ht.Insert("key2", "value2")

	fmt.Println(ht.Get("key1"))
	fmt.Println(ht.Get("key2"))
}
