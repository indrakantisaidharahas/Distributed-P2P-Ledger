/* this file contains the gossip engine implementation 
The GossipEngine struct manages the list of peers, seen transactions, and the local transaction store.
It provides methods to select random peers, check if a transaction has been seen, mark transactions as seen,
and handle incoming gossip.
The main method is Gossip, which takes a transaction, checks if it's new, marks it as seen, and forwards it to 
2 random peers.
The sendGossip method performs the actual HTTP POST to a peer, and HandleIncoming processes received transactions.
The gossipHandler is the HTTP handler for incoming gossip, and getTransactionsHandler returns all seen transactions.
The startServer method registers the handlers and starts the HTTP server.
*/

package gossip

import (
	"bytes" // The bytes package in Go provides functions for manipulating byte slices. In this code, it is used to create a new reader from the JSON data when sending gossip messages to peers. Specifically, bytes.NewReader(jsonData) creates a new reader that reads from the byte slice containing the marshaled transaction data, allowing it to be sent in the body of an HTTP POST request. For example, when the sendGossip method marshals a transaction into JSON format, it uses bytes.NewReader to create a reader for that JSON data, which is then passed to the httpClient.Post method to send the gossip message to a peer.
	"encoding/json" // The encoding/json package in Go provides functions for encoding and decoding JSON data. In this code, it is used to marshal a Transaction struct into JSON format before sending it to peers. Specifically, json.Marshal(tx) converts the Transaction struct into a JSON byte slice, which can then be sent in the body of an HTTP POST request when gossiping the transaction to other nodes in the network. For example, in the sendGossip method, the transaction is marshaled into JSON using json.Marshal, and if there is an error during this process, it logs the failure and returns without sending the gossip message.
	"fmt" 
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"p2pledger/internal/models"
	"p2pledger/internal/mempool"
	"p2pledger/internal/blockchain"

	"p2pledger/internal/models"
	"p2pledger/internal/storage"
)

const (
	defaultFanout       = 2
	maxSendRetries      = 3 
	initialRetryBackoff = 200 * time.Millisecond 
)

// GossipEngine is the core structure that manages peer-to-peer gossip.
// It holds the list of peers, a set of seen transaction IDs (to avoid loops),
// a local store of all transactions, and an HTTP client for outgoing requests.
type GossipEngine struct {
	peers      []string
	nodeAddr   string
	seen       map[string]bool
	seenMu     sync.RWMutex
	httpClient *http.Client
	store      storage.Storage
	fanout     int
}

// Transaction represents a piece of data to be gossiped across the network.
// Each transaction has a unique ID, some data (e.g., "Alice pays Bob $10"), and a timestamp.


// type Transaction struct {
// 	ID        string `json:"id"`
// 	Data      string `json:"data"`
// 	Timestamp int64  `json:"timestamp"`
// }
//<--------chaged becuse alreayd defined




// GossipEngine is the core structure that manages peer-to-peer gossip.
// It holds the list of peers, a set of seen transaction IDs (to avoid loops),
// a local store of all transactions, and an HTTP client for outgoing requests.
type GossipEngine struct {
	peers        []string          // List of peer URLs (e.g., "http://localhost:8002")
	nodeAddr     string            // This node's own address (used only for logging)
	seen         map[string]bool   // Tracks already processed transaction IDs
	seenMu       sync.RWMutex      // Mutex for seen map (concurrent read/write)


//------------------------------------------------
	//transactions []models.Transaction     // All transactions this node has ever seen (for GET /Transactions)
    // goint to repalce trasaction wiht mempool 
//-------------------------------------------------



//-------new things that are to be added -----------
    mempool *mempool.Mempool
	 ledger  *ledger.Ledger

//-------------------------------------------------


     // ---------
	//txMu         sync.RWMutex      // Mutex for transactions slice
	// reove this too as we will not use trasaction anymore 

	//---------


	httpClient   *http.Client      // HTTP client with timeout


	// ------------
	//store storage.Storage         //<-----------------creating a storgae attribute 
	//for now i nees to remove this sas this will be handles in block side 
	//------------
}




// NewGossipEngine creates a new gossip engine by reading peers from a file.
// Parameters:
//   - peersFile: path to a text file with one peer URL per line
//   - nodeAddr: this node's address (e.g., "http://localhost:8001") – used for logging
// Returns:
//   - *GossipEngine: initialised engine
//   - error: if file reading fails
//<---------------need to add new attribute for storage 


// func NewGossipEngine(peersFile, nodeAddr string, store storage.Storage) (*GossipEngine, error) {
// 	// Read the entire peers file (fine for small files; for large lists use bufio.Scanner)
// 	data, err := os.ReadFile(peersFile)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read peers file: %w", err)
// 	}

// 	// Split file content into lines and trim whitespace
// 	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
// 	peers := make([]string, 0, len(lines))
// 	for _, line := range lines {
// 		line = strings.TrimSpace(line)
// 		if line == "" {
// 			continue
// 		}
// 		peers = append(peers, line)
// 	}

// 	return &GossipEngine{
// 		peers:        peers,
// 		nodeAddr:     nodeAddr,
// 		seen:         make(map[string]bool),
// 		store:        store,//adding the new store thingy
// 		transactions: []models.Transaction{},
// 		httpClient:   &http.Client{Timeout: 5 * time.Second},
// 	}, nil
// }
func NewGossipEngine(peersFile, nodeAddr string) (*GossipEngine, error) {
	// Read the entire peers file (fine for small files; for large lists use bufio.Scanner)
	data, err := os.ReadFile(peersFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read peers file: %w", err)
	}

	// Seed the random number generator to ensure different random sequences each run.
	rand.Seed(time.Now().UnixNano())

	// Split file content into lines and trim whitespace
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	peers := make([]string, 0, len(lines))
	seenPeers := make(map[string]struct{}, len(lines))

	for _, line := range lines {
		peer := strings.TrimSpace(line)
		if peer == "" || peer == nodeAddr {
			continue
		}
		if _, ok := seenPeers[peer]; ok {
			continue
		}
		seenPeers[peer] = struct{}{}
		peers = append(peers, peer)
	}

	return &GossipEngine{
		peers:        peers,
		nodeAddr:     nodeAddr,
		seen:         make(map[string]bool),
		mempool:  mempool.NewMempool(),
		ledger:   ledger.LoadLedger(),
	
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}, nil
}

// selectRandomPeers picks n distinct random peers from the peer list.
// If n >= number of peers, it returns a shuffled copy of all peers.
// This is used to implement the gossip fan‑out (k=2).
func (g *GossipEngine) selectRandomPeers(n int) []string {
	if n <= 0 || len(g.peers) == 0 {
		return nil
	}
	if n >= len(g.peers) {
		// Return a shuffled copy of all peers
		shuffled := make([]string, len(g.peers))
		copy(shuffled, g.peers)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		return shuffled
	}
	// Pick n distinct random indices using rand.Perm
	indices := rand.Perm(len(g.peers))[:n]
	selected := make([]string, n)
	for i, idx := range indices {
		selected[i] = g.peers[idx]
	}
	return selected
}

// isSeen checks whether a transaction ID has already been processed. 
// It uses a read lock to safely access the 'seen' map, allowing multiple concurrent reads while preventing writes during the check. This method is called before accepting a transaction to determine if it should be processed or ignored.
func (g *GossipEngine) isSeen(txID string) bool {
	g.seenMu.RLock()
	defer g.seenMu.RUnlock()
	return g.seen[txID]
}

// NOTE:
// Removed legacy markSeen() that referenced g.txMu / g.transactions.
// Dedup + persistence is now handled by acceptTransaction() using:
// 1) in-memory seen map
// 2) persistent store.TransactionExists + store.SaveTransaction

// acceptTransaction checks if a transaction is new and accepts it if so.
// It returns true if the transaction was accepted, false if it was already known,
// and an error if there was a problem during the process.

func (g *GossipEngine) GetTransactions() []models.Transaction {
	return g.mempool.GetAll()
}

func (g *GossipEngine) GetChain() []models.Block {
	return g.ledger.Chain
}







// The method first checks the in-memory 'seen' map to quickly determine if the transaction ID has already been processed. If it has, it returns false without further processing. If it's new, it marks the transaction ID as seen in the 'seen' map. Then it checks the persistent store to see if the transaction already exists (in case of a restart or multiple nodes). If it exists, it returns false. If it's truly new, it saves the transaction to the store and returns true. If any error occurs during the existence check or saving process, it removes the transaction ID from the 'seen' map to allow for future retries and returns an error.

func (g *GossipEngine) acceptTransaction(tx models.Transaction) (bool, error) {
	g.seenMu.Lock()
	defer g.seenMu.Unlock()
	//g.txMu.Lock()
	//defer g.txMu.Unlock()
	g.seen[tx.ID] = true
	//g.store.SaveTransaction(tx)//<------------------------------newly added
	//g.transactions = append(g.transactions, tx)
}


// Gossip is the main method that starts the gossip propagation.
// It marks the transaction as seen (if new) and forwards it to 2 random peers.
// This method is called either by the /Transaction endpoint (user‑initiated)
// or by HandleIncoming when a transaction is received from another node.
func (g *GossipEngine) Gossip(tx models.Transaction) {
	accepted, err := g.acceptTransaction(tx)
	if err != nil {
		log.Printf("[%s] tx %s rejected due to error: %v", g.nodeAddr, tx.ID, err)
		return
	}
	g.mempool.Add(tx)
	g.markSeen(tx)
	log.Printf("[%s] Gossiping tx %s: %s", g.nodeAddr, tx.ID, tx.Data)

	for _, peerURL := range g.selectRandomPeers(g.fanout) {
		go g.sendGossip(peerURL, tx)
	}
}
// sendGossip performs the actual HTTP POST request to a single peer.
// It is called asynchronously by Gossip.
func (g *GossipEngine) sendGossip(peerURL string, tx models.Transaction) {
	jsonData, err := json.Marshal(tx)
	if err != nil {
		log.Printf("[%s] Failed to marshal tx %s: %v", g.nodeAddr, tx.ID, err)
		return
	}

	backoff := initialRetryBackoff
	for attempt := 1; attempt <= maxSendRetries; attempt++ {
		resp, err := g.httpClient.Post(peerURL+"/gossip", "application/json", bytes.NewReader(jsonData))
		if err == nil {
			status := resp.StatusCode
			resp.Body.Close()
			if status == http.StatusOK {
				return
			}
			err = fmt.Errorf("status %d", status)
		}

		if attempt == maxSendRetries {
			log.Printf("[%s] Failed to send tx %s to %s after %d attempts: %v", g.nodeAddr, tx.ID, peerURL, attempt, err)
			return
		}

		log.Printf("[%s] Retry %d/%d for tx %s to %s: %v", g.nodeAddr, attempt, maxSendRetries, tx.ID, peerURL, err)
		time.Sleep(backoff)
		backoff *= 2
	}
}

// HandleIncoming processes a transaction received via /gossip.
// If the transaction is new, it marks it as seen and then forwards it further (gossip).
// This implements the "if exists → ignore; else → forward" rule.

func (g *GossipEngine) HandleIncoming(tx models.Transaction) {
	if g.isSeen(tx.ID) {
		log.Printf("[%s] Ignoring duplicate tx %s", g.nodeAddr, tx.ID)
		return
	}
	g.mempool.Add(tx)//new line 
	g.markSeen(tx)

	log.Printf("[%s] Received new tx %s: %s (timestamp %d)", g.nodeAddr, tx.ID, tx.Data, tx.Timestamp)

	// Forward to other peers asynchronously
	go g.Gossip(tx)
}


func (g *GossipEngine) SubmitTransaction(tx models.Transaction) {
	// so thhis is replacing work of handle incoming 
	if g.mempool.Add(tx) {
		log.Println("added tx", tx.ID)
		go g.Gossip(tx)
	}
}
// func NewGossipEngine(peers []string, nodeAddr string) *GossipEngine {
// 	return &GossipEngine{
// 		peers:    peers,
// 		nodeAddr: nodeAddr,
// 		mempool:  mempool.NewMempool(),
// 		ledger:   blockchain.LoadLedger(),
// 	}
// }



























// i am skippinig these for now ;like manking changes in mine 


// gossipHandler is the HTTP handler for POST /gossip.
// It reads the transaction JSON, then calls HandleIncoming in a goroutine.



func (g *GossipEngine) gossipHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	var tx models.Transaction
	if err := json.Unmarshal(body, &tx); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	go g.HandleIncoming(tx)
	w.WriteHeader(http.StatusOK)
}

func (g *GossipEngine) CreateBlock() {
	txs := g.mempool.GetAll()
	if len(txs) == 0 {
		return
	}

	last := g.ledger.LastBlock()

	block := models.Block{
		Index:        last.Index + 1,
		Transactions: txs,
		Timestamp:    time.Now().Unix(),
		PrevHash:     last.CurrentHash,
	}

	block.CurrentHash = block.CalculateHash()

	if g.ledger.AppendBlock(block) {
		log.Println("block created", block.Index)
		g.mempool.Clear()
	}
	go g.broadcastBlock(&block)
}

//------------new function added 

//v1

// func (g *GossipEngine) broadcastBlock(block *models.Block) {
// 	log.Println("broadcasting block", block.Index)

// 	for _, peer := range g.peers {
// 		go func(p string) {
// 			jsonData, _ := json.Marshal(block)

// 			resp, err := g.httpClient.Post(
// 				p+"/newblock",
// 				"application/json",
// 				bytes.NewBuffer(jsonData),
// 			)

// 			if err != nil {
// 				log.Println("error sending block to", p, err)
// 				return
// 			}
// 			defer resp.Body.Close()

// 			log.Println("sent block to", p, "status:", resp.StatusCode)

// 		}(peer)
// 	}
// }

func (g *GossipEngine) broadcastBlock(block *models.Block) {
	log.Println("broadcasting block", block.Index)

	peers := g.peers
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	fanout := 2
	if len(peers) < fanout {
		fanout = len(peers)
	}

	for i := 0; i < fanout; i++ {
		p := peers[i]

		go func(p string) {
			jsonData, _ := json.Marshal(block)

			resp, err := g.httpClient.Post(
				p+"/newblock",
				"application/json",
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("error sending block to", p, err)
				return
			}
			defer resp.Body.Close()

			log.Println("sent block to", p, "status:", resp.StatusCode)
		}(p)
	}
}



//----------new function added 
func (g *GossipEngine) HandleIncomingBlock(block models.Block) {
	last := g.ledger.LastBlock()

	if block.PrevHash == last.CurrentHash &&
		block.Index == last.Index+1 {

		if g.ledger.AppendBlock(block) {
			log.Println("block added from peer", block.Index)
			g.mempool.Clear()
			return
		}
		
	}
	log.Println(
	"rejected block",
	"blockIndex:", block.Index,
	"lastIndex:", last.Index,
	"expectedIndex:", last.Index+1,
	"prevMatch:", block.PrevHash == last.CurrentHash,
)
	go g.SyncWithPeer()	
}

func (g *GossipEngine) SyncWithPeer() {
	if len(g.peers) == 0 {
		return
	}

	peer := g.peers[rand.Intn(len(g.peers))]

	resp, err := g.httpClient.Get(peer + "/chain")
	if err != nil {
		log.Println("sync failed:", err)
		return
	}
	defer resp.Body.Close()

	var theirChain []models.Block
	if err := json.NewDecoder(resp.Body).Decode(&theirChain); err != nil {
		return
	}

	if len(theirChain) > len(g.ledger.Chain) {
		g.ledger.ReplaceChain(theirChain)
		log.Println("chain replaced from peer")
	}
}






//commetsn by me: call bakc function for gossip how to move this to /internals/api?
 




// getTransactionsHandler is the HTTP handler for GET /Transactions.
// It returns all transactions this node has seen as a JSON array.
func (g *GossipEngine) getTransactionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	//g.txMu.RLock()
	//defer g.txMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(g.mempool.GetAll())
}

//coomments by sai:conciflcts wwiht mt /transaction ,should i edit in /internals/api?













// startServer registers the HTTP handlers and starts listening.
func (g *GossipEngine) startServer(port string) {
	http.HandleFunc("/gossip", g.gossipHandler)
	http.HandleFunc("/Transactions", g.getTransactionsHandler)
	addr := ":" + port
	log.Printf("[%s] Gossip engine listening on %s", g.nodeAddr, addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("[%s] Server failed: %v", g.nodeAddr, err)
	}
}

// func main() {
// 	// Command-line arguments: node-address, port, [peers-file]
// 	if len(os.Args) < 3 {
// 		fmt.Println("Usage: go run main.go <node-address> <port> [peers-file]")
// 		fmt.Println("Example: go run main.go http://localhost:8001 8001 peers.txt")
// 		os.Exit(1)
// 	}
// 	nodeAddr := os.Args[1]
// 	port := os.Args[2]
// 	peersFile := "peers.txt"
// 	if len(os.Args) >= 4 {
// 		peersFile = os.Args[3]
// 	}

// 	rand.Seed(time.Now().UnixNano()) // Seed random number generator

// 	engine, err := NewGossipEngine(peersFile, nodeAddr)
// 	if err != nil {
// 		log.Fatalf("Failed to create gossip engine: %v", err)
// 	}

// 	// Start the gossip HTTP server in a background goroutine
// 	go engine.startServer(port)

// 	// POST /Transaction endpoint – allows users to submit a new transaction
// 	http.HandleFunc("/Transaction", func(w http.ResponseWriter, r *http.Request) {
// 		if r.Method != http.MethodPost {
// 			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
// 			return
// 		}
// 		var req struct {
// 			Data string `json:"data"`
// 		}
// 		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 			http.Error(w, "Invalid JSON", http.StatusBadRequest)
// 			return
// 		}
// 		tx := Transaction{
// 			ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
// 			Data:      req.Data,
// 			Timestamp: time.Now().Unix(),
// 		}
// 		engine.Gossip(tx) // start the gossip chain
// 		w.WriteHeader(http.StatusAccepted)
// 		fmt.Fprintf(w, "Transaction gossiped with ID %s\n", tx.ID)
// 	})

// 	log.Printf("[%s] Ready. Endpoints: POST /Transaction, POST /gossip, GET /Transactions", nodeAddr)

// 	// Block forever (keep the main goroutine alive)
// 	select {}
// }

