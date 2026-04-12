package main

import (
	"github.com/gin-gonic/gin"
	"os"

	"p2pledger/internal/api"
	"p2pledger/internal/storage"
	"p2pledger/Gossip_Engine"
)

func main() {
	if len(os.Args) < 3 {
		panic("Usage: go run main.go <port> <peers_file>")
	}

	port := os.Args[1]
	peersFile := os.Args[2]

	nodeAddr := "http://localhost:" + port

	router := gin.Default()

	// storage (optional now)
	store := storage.NewFileStorage("nodeA/ledger_" + port + ".json")

	// gossip engine
	gossipEngine, err := gossip.NewGossipEngine(peersFile, nodeAddr)
	if err != nil {
		panic(err)
	}

	// handler
	handler := api.NewHandler(store, gossipEngine)

	// routes
	router.POST("/transaction", handler.AddTransaction)
	router.GET("/transactions", handler.GetTransactions)
	router.POST("/gossip", handler.GossipReceive)
	router.POST("/mine", handler.MineBlock)
	router.GET("/chain", handler.GetChain)
	router.POST("/newblock", handler.ReceiveBlock)

	router.Run(":" + port)
}