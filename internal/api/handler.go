package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"p2pledger/Gossip_Engine"
	"p2pledger/internal/models"
	"p2pledger/internal/storage"
)

type Handler struct {
	Store  storage.Storage
	Gossip *gossip.GossipEngine
}

// Backward-compatible: NewHandler(store) OR NewHandler(store, gossipEngine)
func NewHandler(store storage.Storage, g ...*gossip.GossipEngine) *Handler {
	var ge *gossip.GossipEngine
	if len(g) > 0 {
		ge = g[0]
	}
	return &Handler{Store: store, Gossip: ge}
}

func isValidTransaction(tx models.Transaction) bool {
	return tx.ID != "" && tx.Data != "" && tx.Timestamp > 0
}

func (h *Handler) AddTransaction(c *gin.Context) {
	var tx models.Transaction
	if err := c.ShouldBindJSON(&tx); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if !isValidTransaction(tx) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id, data, timestamp are required"})
		return
	}

	exists, err := h.Store.TransactionExists(tx.ID)
	h.Gossip.AddTransaction(tx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if exists {
		c.JSON(http.StatusConflict, gin.H{"error": "already exists"})
		return
	}

	if h.Gossip != nil {
		h.Gossip.Gossip(tx)
		c.JSON(http.StatusAccepted, gin.H{"status": "gossip_started"})
		return
	}

	if err := h.Store.SaveTransaction(tx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "saved_locally"})
}

func (h *Handler) GetTransactions(c *gin.Context) {
	txs, err := h.Gossip.GetTransactions()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, txs)
}

func (h *Handler) GossipReceive(c *gin.Context) {
	var tx models.Transaction
	if err := c.ShouldBindJSON(&tx); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if !isValidTransaction(tx) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id, data, timestamp are required"})
		return
	}
	if h.Gossip == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gossip engine not configured"})
		return
	}

	h.Gossip.HandleIncoming(tx)
	c.JSON(http.StatusOK, gin.H{"status": "received"})
}

func (h *Handler) MineBlock(c *gin.Context) {
	if h.Gossip == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gossip engine not configured"})
		return
	}
	h.Gossip.CreateBlock()
	c.JSON(http.StatusOK, gin.H{"status": "block created"})
}

func (h *Handler) GetChain(c *gin.Context) {
	if h.Gossip == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gossip engine not configured"})
		return
	}
	c.JSON(http.StatusOK, h.Gossip.GetChain())
}

func (h *Handler) GetMempool(c *gin.Context) {
	if h.Gossip == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gossip engine not configured"})
		return
	}
	txs,err:=h.Gossip.GetTransactions()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK,txs)
}

func (h *Handler) ReceiveBlock(c *gin.Context) {
	if h.Gossip == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gossip engine not configured"})
		return
	}

	var block models.Block
	if err := c.ShouldBindJSON(&block); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	h.Gossip.HandleIncomingBlock(block)
	c.JSON(http.StatusOK, gin.H{"status": "block received"})
}

func (h *Handler) SyncTransactions(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"status": "not_implemented"})
}
