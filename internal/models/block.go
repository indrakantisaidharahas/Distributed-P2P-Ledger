//package block
package models
import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
	"encoding/json"
	
)

// type Transaction struct {
// 	ID        string `json:"id"`
// 	Data      string `json:"data"`
// 	Timestamp int64  `json:"timestamp"`
// }

type Block struct {
	Index       int       `json:"index"`
	Transactions []Transaction    `json:"data"`
	Timestamp   time.Time `json:"timestamp"`
	PrevHash    string    `json:"prev_hash"`
	CurrentHash string    `json:"current_hash"`

}

func (b *Block) CalculateHash() string {

	txBytes,_ := json.Marshal(b.Transactions)

	record := fmt.Sprintf("%d%s%s%s",
		b.Index,
		string(txBytes),
		b.Timestamp.String(),
		b.PrevHash,
	)

	h := sha256.New()
	h.Write([]byte(record))
	return hex.EncodeToString(h.Sum(nil))
}


func NewBlock(index int, txs []Transaction, prevHash string) *Block {
	b:= &Block{
		Index:        index,
		Transactions: txs,
		Timestamp:    time.Now(),
		PrevHash:     prevHash,
	}
	b.CurrentHash = b.CalculateHash()
	return b
}