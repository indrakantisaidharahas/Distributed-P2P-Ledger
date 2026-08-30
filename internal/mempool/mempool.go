package mempool

// so instead of using a trsaction array in gossip engine we are using this mempool data  structure 
// the prev implmentation only used it to add transactions 
// we ned more operations better to use to use sperate handler like this 
import(
	"p2pledger/internal/models"
	"sync"

)


type Mempool struct {
	txs map[string]models.Transaction
	mu  sync.RWMutex
}

func NewMempool() *Mempool {
	return &Mempool{
		txs: make(map[string]models.Transaction),
	}
}

func (m *Mempool) Add(tx models.Transaction) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.txs[tx.ID]; exists {
		return false
	}

	m.txs[tx.ID] = tx
	return true
}

func (m *Mempool) GetAll() []models.Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var res []models.Transaction
	for _, tx := range m.txs {
		res = append(res, tx)
	}
	return res
}

func (m *Mempool) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs = make(map[string]models.Transaction)
}