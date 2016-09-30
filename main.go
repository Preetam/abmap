package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/Preetam/libab/go/ab"
	"github.com/VividCortex/siesta"
)

var (
	encryptionKey = ""
	cluster       = map[uint64]string{
		10: "127.0.0.1:2010",
		20: "127.0.0.1:2020",
		30: "127.0.0.1:2030",
	}

	clusterHTTP = map[uint64]string{
		10: "http://127.0.0.1:8010",
		20: "http://127.0.0.1:8020",
		30: "http://127.0.0.1:8030",
	}

	id            = uint64(0)
	state         = map[string]string{}
	stateCommit   = uint64(0)
	isLeader      = false
	currentLeader = uint64(0)
	lock          sync.Mutex
)

type Snapshot struct {
	Data   map[string]string `json:"data"`
	Commit uint64            `json:"commit"`
}

type handler struct{}

func (h handler) OnLeaderChange(node *ab.Node, leaderID uint64) {
	fmt.Println("OnLeaderChange", leaderID)
	lock.Lock()
	currentLeader = leaderID
	lock.Unlock()
}

func (h handler) OnAppend(node *ab.Node, round uint64, data string) {
	fmt.Println("append:", data, round)
	lock.Lock()
	defer lock.Unlock()
	json.NewDecoder(strings.NewReader(data)).Decode(&state)
	stateCommit = round
	node.ConfirmAppend(round)
}

func (h handler) LostLeadership(node *ab.Node) {
	fmt.Println("LostLeadership")
	lock.Lock()
	isLeader = false
	currentLeader = 0
	lock.Unlock()
}

func (h handler) GainedLeadership(node *ab.Node) {
	fmt.Println("GainedLeadership")
	lock.Lock()
	isLeader = true
	currentLeader = 0
	lock.Unlock()
	fetchSnapshots()
}

func main() {
	flag.Uint64Var(&id, "id", 0, "node ID")
	flag.StringVar(&encryptionKey, "key", "", "encryption key")
	flag.Parse()

	if _, ok := cluster[id]; !ok {
		log.Fatal("invalid ID")
	}

	var err error
	var node *ab.Node
	node, err = ab.NewNode(id, cluster[id], handler{}, len(cluster))
	if err != nil {
		log.Fatal(err)
	}
	for peerID, peer := range cluster {
		if peerID != id {
			node.AddPeer(peer)
		}
	}
	node.SetKey(encryptionKey)
	fetchSnapshots()
	go func() {
		panic(node.Run())
	}()

	service := siesta.NewService("/")
	service.AddPost(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
	})
	service.Route("GET", "/", "", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		if !isLeader && currentLeader != 0 {
			w.Header().Set("Location", clusterHTTP[currentLeader])
			return
		}
		json.NewEncoder(w).Encode(state)
	})
	service.Route("GET", "/snapshot", "", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		snapshot := Snapshot{
			Data:   state,
			Commit: stateCommit,
		}
		json.NewEncoder(w).Encode(snapshot)
	})
	service.Route("POST", "/", "", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		newState := map[string]string{}
		err := json.NewDecoder(r.Body).Decode(&newState)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		buf := bytes.NewBuffer(nil)
		if err = json.NewEncoder(buf).Encode(newState); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		lock.Lock()
		defer lock.Unlock()

		if !isLeader {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		err = node.Append(buf.String())
		if err = json.NewEncoder(buf).Encode(newState); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})

	panic(http.ListenAndServe(fmt.Sprintf("localhost:%d", id+8000), service))
}

func fetchSnapshots() {
	for nodeID, endpoint := range clusterHTTP {
		if nodeID == id {
			continue
		}

		res, err := http.Get(endpoint + "/snapshot")
		if err != nil {
			continue
		}

		defer res.Body.Close()
		snapshot := Snapshot{}
		err = json.NewDecoder(res.Body).Decode(&snapshot)
		if err != nil {
			continue
		}

		lock.Lock()
		if snapshot.Commit > stateCommit {
			state = snapshot.Data
			stateCommit = snapshot.Commit
		}
		lock.Unlock()
	}
}
