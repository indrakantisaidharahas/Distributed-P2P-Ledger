package ledger

import (
	"encoding/json"
	"os"
	"sync"
	"p2pledger/internal/models"

)

type Ledger struct {
	Chain []models.Block
	mu    sync.RWMutex
}

func LoadLedger() *Ledger {
	l := &Ledger{Chain: []models.Block{}}
	data, err := os.ReadFile("ledger.json")
	if err == nil {
		json.Unmarshal(data, &l.Chain)
		return l
	}
	// genesis := models.NewBlock(0, "Genesis Block", "")
	genesis := models.NewBlock(0, []models.Transaction{}, "")
	l.Chain = append(l.Chain, *genesis)
	l.save()
	return l
}

func (l *Ledger) save() {
	l.mu.RLock()
	defer l.mu.RUnlock()
	data, _ := json.MarshalIndent(l.Chain, "", "  ")
	os.WriteFile("ledger.json", data, 0644)
}

func (l *Ledger) LastBlock() models.Block {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.Chain[len(l.Chain)-1]
}

func (l *Ledger) AppendBlock(b models.Block) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	last := l.Chain[len(l.Chain)-1]
	if b.Index != last.Index+1 || b.PrevHash != last.CurrentHash {
		return false
	}
	if b.CalculateHash() != b.CurrentHash {
		return false
	}
	l.Chain = append(l.Chain, b)
	go l.save()
	return true
}

func (l *Ledger) ReplaceChain(newChain []models.Block) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(newChain) <= len(l.Chain) {
		return false
	}
	// Validate the new chain
	for i := 1; i < len(newChain); i++ {
		if newChain[i].Index != newChain[i-1].Index+1 ||
			newChain[i].PrevHash != newChain[i-1].CurrentHash {
			return false
		}
		if newChain[i].CalculateHash() != newChain[i].CurrentHash {
			return false
		}
	}
	l.Chain = newChain
	go l.save()
	return true
}