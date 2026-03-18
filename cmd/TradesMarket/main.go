package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var (
	postgresDSN   = os.Getenv("POSTGRES_DSN")
	pairsOverride = os.Getenv("PAIRS_OVERRIDE")
)

// --- Structs for WebSocket ---

type KlineStreamMsg struct {
	Symbol string `json:"s"`
	Data   struct {
		OpenTime float64 `json:"t"`
		Open     string  `json:"o"`
		High     string  `json:"h"`
		Low      string  `json:"l"`
		Close    string  `json:"c"`
		Volume   string  `json:"v"`
		IsClosed bool    `json:"x"` // Crucial: Only save if true
	} `json:"k"`
}

// BinanceKLine matches the array response from REST API
type BinanceKLine []interface{}

// --- Logic ---

func main() {
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ensureOHLCVTable(db)
	pairs := loadPairs()

	// 1. Start Real-time Stream in background
	go startRealTimeStream(db, pairs)

	// 2. Run Backfill for all pairs (blocking or background)
	var wg sync.WaitGroup
	for _, pair := range pairs {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			backfill(db, p)
		}(pair)
	}
	wg.Wait()

	// Keep main alive
	log.Println("🚀 System fully operational. Backfill done, Streaming active.")
	select {}
}

func ensureOHLCVTable(db *sql.DB) {
	query := `
	CREATE TABLE IF NOT EXISTS ohlcv_15m (
		pair TEXT NOT NULL,
		open_time TIMESTAMPTZ NOT NULL,
		open NUMERIC,
		high NUMERIC,
		low NUMERIC,
		close NUMERIC,
		volume NUMERIC,
		PRIMARY KEY (pair, open_time)
	);`
	db.Exec(query)
}

// --- WebSocket Real-Time Logic ---

func startRealTimeStream(db *sql.DB, pairs []string) {
	var streams []string
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@kline_15m")
	}

	url := "wss://stream.binance.com:9443/ws/" + strings.Join(streams, "/")

	for {
		log.Printf("🔌 Connecting to Stream: %v", streams)
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("❌ WS Dial error: %v. Retrying...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("🔴 WS Read error: %v", err)
				break
			}

			var msg KlineStreamMsg
			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}

			// ONLY save if the candle is finished
			if msg.Data.IsClosed {
				saveCandle(db, msg)
			}
		}
		conn.Close()
		time.Sleep(2 * time.Second)
	}
}

func saveCandle(db *sql.DB, msg KlineStreamMsg) {
	ts := time.UnixMilli(int64(msg.Data.OpenTime))
	_, err := db.Exec(`
		INSERT INTO ohlcv_15m (pair, open_time, open, high, low, close, volume)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (pair, open_time) 
		DO UPDATE SET 
			open = EXCLUDED.open, high = EXCLUDED.high, 
			low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume
	`, msg.Symbol, ts, msg.Data.Open, msg.Data.High, msg.Data.Low, msg.Data.Close, msg.Data.Volume)

	if err != nil {
		log.Printf("[%s] Real-time save error: %v", msg.Symbol, err)
	} else {
		log.Printf("📥 [%s] Saved closed candle for %v", msg.Symbol, ts)
	}
}

// --- Historical Backfill Logic (same as before, simplified) ---

func backfill(db *sql.DB, pair string) {
	targetStart := time.Now().AddDate(-1, 0, 0).UnixMilli()
	
	var lastTime int64
	db.QueryRow("SELECT EXTRACT(EPOCH FROM MAX(open_time))*1000 FROM ohlcv_15m WHERE pair=$1", pair).Scan(&lastTime)
	if lastTime > targetStart {
		targetStart = lastTime + 1
	}

	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=15m&startTime=%d&limit=1000", pair, targetStart)
		resp, err := http.Get(url)
		if err != nil || resp.StatusCode != 200 {
			break
		}
		
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var klines []BinanceKLine
		json.Unmarshal(body, &klines)

		if len(klines) <= 1 { // If we only get the currently open candle, we are done
			break
		}

		// Batch insert... (Implementation omitted for brevity, use same logic as previous response)
		
		lastCandleTime := int64(klines[len(klines)-1][0].(float64))
		targetStart = lastCandleTime + 1
		
		if len(klines) < 1000 { break }
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("[%s] Backfill complete.", pair)
}

func loadPairs() []string {
	if pairsOverride != "" {
		return strings.Split(strings.ToUpper(pairsOverride), ",")
	}
	return []string{"BTCUSDT", "ETHUSDT"}
}