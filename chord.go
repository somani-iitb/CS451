package main 

import (
	"fmt"
	"time"
	"crypto/sha1"
	"hash"
)

type Data struct {
	Key string
	Value int
}

type Config struct {
	Hostname string // Local host name
	HashFunc func() hash.Hash // Hash function to use
	NumSuccessors int // Number of successors to maintain
	hashBits int // Bit size of the hash function
	StabilizeMin time.Duration // Minimum stabilization time
	StabilizeMax time.Duration // Maximum stabilization time
}

// Returns the default Ring configuration
func DefaultConfig(hostname string) *Config {
	return &Config{
		hostname,
		sha1.New, // SHA1
		3, // 3 successors
		8, // 8 bit hash function
		time.Duration(5 * time.Second),
		time.Duration(10 * time.Second),
	}
}


// Stores the state required for a Chord ring
type Ring struct {
	config *Config
	lnodes []*LocalNode
	shutdown chan bool
}

type SNode struct{
	Id []byte // key ID
	Host string
}

// Represents a local Vnode
type LocalNode struct {
	Id []byte // key ID
	Host string
	Ring *Ring
	Successors []*SNode
	Finger []*SNode
	Predecessor *SNode
	last_finger int
	stabilized time.Time
	timer *time.Timer
	Store map[string]int	
}


// Creates a new Chord ring given the config and transport
func Create(conf *Config) (*Ring, error) {
	// Create and initialize a ring
	ring := &Ring{}
	ring.init(conf)
	fmt.Println("Exited Create function wt ring = ",ring)
	return ring, nil
}


func Join(conf *Config, existing string) (*Ring, error) {
	// Create a ring
	ring := &Ring{}
	ring.init(conf)
	succ:=SNode{}
	fmt.Println("Existing node's address : ",existing)
	err:=RPC_caller(existing,"FindCoordinator",ring.lnodes[0].Id, &succ)
	ring.lnodes[0].Successors[0]=&succ
	err=RPC_caller(succ.Host,"GetAll",ring.lnodes[0].Host, &(ring.lnodes[0].Store))
	checkError(err)
	return ring,nil
}
