/*
This file contains the API handlers for the P2P ledger application. 
API handler is a function that processes incoming HTTP requests,
interacts with the storage layer to manage transactions, and returns appropriate responses.
It defines a Handler struct that has a reference to the storage layer and 
the gossip engine. 
The Handler struct has methods to handle incoming HTTP requests
for adding transactions, getting transactions, and receiving gossip messages. 
The AddTransaction method processes incoming transaction data, checks if it 
already exists, saves it to storage, and then gossips it to peers. 
The GetTransactions method retrieves all transactions from storage and 
returns them as JSON. 
The GossipReceive method handles incoming gossip messages, checks if the
transaction is new, saves it, and gossips it further if necessary.

*/
package api

import (
	"net/http" 
	"github.com/gin-gonic/gin" // for HTTP routing and handling
	gossip "p2pledger/Gossip_Engine"
	"p2pledger/internal/models" 
	"p2pledger/internal/storage"
)

type Handler struct {
	Store storage.Storage
	Gossip *gossip.GossipEngine   //note newly added  change to integrate 

}

// Backward-compatible: NewHandler(store) OR NewHandler(store, gossipEngine)
func NewHandler(store storage.Storage, g ...*gossip.GossipEngine) *Handler {
	var ge *gossip.GossipEngine
	if len(g) > 0 {
		ge = g[0]
	}
	return &Handler{
		Store:  store,
		Gossip: ge,
	}
}

// isValidTransaction checks if the transaction has all required fields (ID, Data, Timestamp) and that they are valid (non-empty ID and Data, positive Timestamp).
func isValidTransaction(tx models.Transaction) bool {
	if tx.ID == "" || tx.Data == "" || tx.Timestamp <= 0 {
		return false
	}
	return true
}

// POST /transaction
// should replace this s

// func (h *Handler) AddTransaction(c *gin.Context) {
// 	var tx models.Transaction
//      // TODO:
// 	// 1. Bind JSON
// 	// 2. Check exists
// 	// 3. Save
// 	// 4. Return response
// 	if err := c.ShouldBindJSON(&tx); err != nil {
// 		c.JSON(400, gin.H{"error": err.Error()})
// 		return
// 	}

// 	exists, err := h.Store.TransactionExists(tx.ID)
// 	if err != nil {
// 		c.JSON(500, gin.H{"error": err.Error()})
// 		return
// 	}

// 	if exists {
// 		c.JSON(400, gin.H{"error": "already exists"})
// 		return
// 	}

// 	if err := h.Store.SaveTransaction(tx); err != nil {
// 		c.JSON(500, gin.H{"error": err.Error()})
// 		return
// 	}
// 	// after saving
//     h.Gossip.Gossip(tx) //newly added 

// 	c.JSON(200, gin.H{"status": "saved"})

// }


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

	h.Gossip.SubmitTransaction(tx)

	c.JSON(200, gin.H{"status": "added to mempool"})
}


// GET /transactions

// func (h *Handler) GetTransactions(c *gin.Context) {
// 	// TODO:
// 	// 1. Load from storage
// 	// 2. Return JSON
// 	txs, err := h.Store.LoadTransactions()
//     // we never use ti but this is not valid as we are no loger using stroe 
// 	if err != nil {
// 		c.JSON(500, gin.H{"error": err.Error()})
// 		return
// 	}

// 	c.JSON(200, txs)
// }
//replacing new get transations method 

func (h *Handler) GetTransactions(c *gin.Context) {
	txs := h.Gossip.GetTransactions()
	c.JSON(200, txs)
}




// route for /gossip newdly added 

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

	// exists, _ := h.Store.TransactionExists(tx.ID)
	// if exists {
	// 	return
	// }


	//h.Store.SaveTransaction(tx)  
    // need to remove this 
      
    //need t replace with the follwoign 
    h.Gossip.HandleIncoming(tx)  
    // i dont remember ths being there 




	//go h.Gossip.Gossip(tx)

	c.JSON(200, gin.H{"status": "received"})
}







// handler for /mine route 
func (h *Handler) MineBlock(c *gin.Context) {
	h.Gossip.CreateBlock()
	c.JSON(200, gin.H{"status": "block created"})
}



//handler for /chain
func (h *Handler) GetChain(c *gin.Context) {
	c.JSON(200, h.Gossip.GetChain())
}
// handler for /mempool
func (h *Handler) GetMempool(c *gin.Context) {
	c.JSON(200, h.Gossip.GetTransactions())
}

// handler for recive block
func (h *Handler) ReceiveBlock(c *gin.Context) {
	var block models.Block

	if err := c.ShouldBindJSON(&block); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	h.Gossip.HandleIncomingBlock(block)

	c.JSON(200, gin.H{"status": "block received"})
}





// POST /sync (optional for now)
func (h *Handler) SyncTransactions(c *gin.Context) {
	// TODO later
    

}