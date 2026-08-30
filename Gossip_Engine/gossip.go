package gossip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	ledger "p2pledger/internal/blockchain"
	"p2pledger/internal/mempool"
	"p2pledger/internal/models"
	"p2pledger/internal/storage"
)

const (
	defaultFanout       = 2
	maxSendRetries      = 3
	initialRetryBackoff = 200 * time.Millisecond
)

type GossipEngine struct {
	peers      []string
	nodeAddr   string
	seen       map[string]bool
	seenMu     sync.RWMutex
	httpClient *http.Client
	store      storage.Storage
	fanout     int

	mempool *mempool.Mempool
	ledger  *ledger.Ledger
}

func NewGossipEngine(peersFile, nodeAddr string, store storage.Storage) (*GossipEngine, error) {
	data, err := os.ReadFile(peersFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read peers file: %w", err)
	}

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

	engine := &GossipEngine{
    peers:      peers,
    nodeAddr:   nodeAddr,
    seen:       make(map[string]bool),
    httpClient: &http.Client{Timeout: 5 * time.Second},
    store:      store,
    fanout:     defaultFanout,
    mempool:    mempool.NewMempool(),
    ledger:     ledger.LoadLedger(),
}

// 🔥 ADD THIS HERE
go func() {
    for {
        time.Sleep(3 * time.Second)
        engine.SyncWithPeer()
    }
}()

return engine, nil
}

func (g *GossipEngine) selectRandomPeers(n int) []string {
	if n <= 0 || len(g.peers) == 0 {
		return nil
	}
	if n >= len(g.peers) {
		shuffled := make([]string, len(g.peers))
		copy(shuffled, g.peers)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		return shuffled
	}

	indices := rand.Perm(len(g.peers))[:n]
	selected := make([]string, n)
	for i, idx := range indices {
		selected[i] = g.peers[idx]
	}
	return selected
}

func (g *GossipEngine) isSeen(txID string) bool {
	g.seenMu.RLock()
	defer g.seenMu.RUnlock()
	return g.seen[txID]
}

func (g *GossipEngine) acceptTransaction(tx models.Transaction) (bool, error) {
	g.seenMu.Lock()
	if g.seen[tx.ID] {
		g.seenMu.Unlock()
		return false, nil
	}
	g.seen[tx.ID] = true
	g.seenMu.Unlock()

	exists, err := g.store.TransactionExists(tx.ID)
	if err != nil {
		g.seenMu.Lock()
		delete(g.seen, tx.ID)
		g.seenMu.Unlock()
		return false, fmt.Errorf("exists check failed: %w", err)
	}
	if exists {
		return false, nil
	}

	if err := g.store.SaveTransaction(tx); err != nil {
		g.seenMu.Lock()
		delete(g.seen, tx.ID)
		g.seenMu.Unlock()
		return false, fmt.Errorf("save failed: %w", err)
	}

	return true, nil
}

func (g *GossipEngine) Gossip(tx models.Transaction) {
	accepted, err := g.acceptTransaction(tx)
	if err != nil {
		log.Printf("[%s] tx %s rejected due to error: %v", g.nodeAddr, tx.ID, err)
		return
	}
	if !accepted {
		log.Printf("[%s] tx %s already known; skipping", g.nodeAddr, tx.ID)
		return
	}

	g.mempool.Add(tx)
	log.Printf("[%s] Gossiping tx %s: %s", g.nodeAddr, tx.ID, tx.Data)

	for _, peerURL := range g.selectRandomPeers(g.fanout) {
		go g.sendGossip(peerURL, tx)
	}
}

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

func (g *GossipEngine) HandleIncoming(tx models.Transaction) {
	g.Gossip(tx)
}

func (g *GossipEngine) SubmitTransaction(tx models.Transaction) {
	g.Gossip(tx)
}

func (g *GossipEngine) GetTransactions() ([]models.Transaction,error) {
	return g.mempool.GetAll(),nil
}

func (g *GossipEngine) GetChain() []models.Block {
	return g.ledger.Chain
}

func (g *GossipEngine) CreateBlock() {
	txs := g.mempool.GetAll()
	if len(txs) == 0 {
		log.Println("empty trasactions")
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

func (g *GossipEngine) broadcastBlock(block *models.Block) {
	log.Println("broadcasting block", block.Index)
	jsonData, err := json.Marshal(block)
	if err != nil {
		log.Println("failed to marshal block for broadcast", err)
		return
	}

	peers := make([]string, len(g.peers))
	copy(peers, g.peers)
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	fanout := defaultFanout
	if len(peers) < fanout {
		fanout = len(peers)
	}

	for i := 0; i < fanout; i++ {
		p := peers[i]
		go func(peer string) {
			resp, err := g.httpClient.Post(peer+"/newblock", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Println("error sending block to", peer, err)
				return
			}
			defer resp.Body.Close()
			log.Println("sent block to", peer, "status:", resp.StatusCode)
		}(p)
	}
}

func (g *GossipEngine) HandleIncomingBlock(block models.Block) {
	last := g.ledger.LastBlock()

	if block.PrevHash == last.CurrentHash && block.Index == last.Index+1 {
		if g.ledger.AppendBlock(block) {
			log.Println("block added from peer", block.Index)
			g.mempool.Clear()
			go g.broadcastBlock(&block)
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
func (g *GossipEngine) AddTransaction(tx models.Transaction) {

    g.mempool.Add(tx)
}

func (g *GossipEngine) SyncWithPeer() {
	if len(g.peers) == 0 {
		return
	}

	for _, peer := range g.peers {
    resp, err := g.httpClient.Get(peer + "/chain")
    if err != nil {
        continue
    }

    var theirChain []models.Block
    if err := json.NewDecoder(resp.Body).Decode(&theirChain); err != nil {
        resp.Body.Close()
        continue
    }
    resp.Body.Close()

    if len(theirChain) > len(g.ledger.Chain) {
        g.ledger.ReplaceChain(theirChain)
        log.Println("chain replaced from peer:", peer)
    }
}
}
