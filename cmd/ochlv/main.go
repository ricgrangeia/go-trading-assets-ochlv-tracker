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
	targetTable   = "ohlcv_15m" // Defined here so logs stay consistent
)

type KlineStreamMsg struct {
	Symbol string `json:"s"`
	Data   struct {
		OpenTime float64 `json:"t"`
		Open     string  `json:"o"`
		High     string  `json:"h"`
		Low      string  `json:"l"`
		Close    string  `json:"c"`
		Volume   string  `json:"v"`
		IsClosed bool    `json:"x"`
	} `json:"k"`
}

type BinanceKLine []interface{}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("🛠️ Starting Binance Ingestor...")

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal("❌ Database connection failed: ", err)
	}
	defer db.Close()

	ensureOHLCVTable(db)
	pairs := loadPairs()

	// 1. Start Real-time Stream
	go startRealTimeStream(db, pairs)

	// 2. Run Backfill
	var wg sync.WaitGroup
	for _, pair := range pairs {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			backfill(db, p)
		}(pair)
	}
	wg.Wait()

	log.Println("🚀 BACKFILL COMPLETE. System now running in live-sync mode.")
	select {}
}

func ensureOHLCVTable(db *sql.DB) {
	log.Printf("📋 Verifying table [%s] exists...", targetTable)
	query := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        pair TEXT NOT NULL,
        open_time TIMESTAMPTZ NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume NUMERIC,
        PRIMARY KEY (pair, open_time)
    );`, targetTable)
	
	if _, err := db.Exec(query); err != nil {
		log.Fatal("❌ Table creation failed: ", err)
	}
	log.Printf("✅ Table [%s] is ready.", targetTable)
}

func startRealTimeStream(db *sql.DB, pairs []string) {
	var streams []string
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@kline_15m")
	}

	url := "wss://stream.binance.com:9443/ws/" + strings.Join(streams, "/")

	for {
		log.Printf("🔌 Connecting to WebSocket: %s", url)
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("⚠️ WS Dial error: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("🟢 WebSocket Connected.")

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("🔴 WS Read error: %v", err)
				break
			}

			var msg KlineStreamMsg
			if err := json.Unmarshal(message, &msg); err != nil {
				log.Printf("⚠️ JSON Parse error: %v", err)
				continue
			}

			// Heartbeat log: let us know data is arriving even if candles aren't closed
			if !msg.Data.IsClosed {
				// We don't log every second to avoid spam, but you could add a counter here
				continue 
			}

			log.Printf("📦 [WS] Received CLOSED candle for %s", msg.Symbol)
			saveCandle(db, msg)
		}
		conn.Close()
		log.Println("🔁 WebSocket disconnected. Reconnecting...")
		time.Sleep(2 * time.Second)
	}
}

func saveCandle(db *sql.DB, msg KlineStreamMsg) {
	ts := time.UnixMilli(int64(msg.Data.OpenTime))
	
	query := fmt.Sprintf(`
        INSERT INTO %s (pair, open_time, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (pair, open_time) 
        DO UPDATE SET 
            open = EXCLUDED.open, high = EXCLUDED.high, 
            low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume
    `, targetTable)

	_, err := db.Exec(query, msg.Symbol, ts, msg.Data.Open, msg.Data.High, msg.Data.Low, msg.Data.Close, msg.Data.Volume)

	if err != nil {
		log.Printf("❌ [%s] DB Save Error: %v", msg.Symbol, err)
	} else {
		log.Printf("💾 [%s] Saved to %s | TS: %s | Price: %s", msg.Symbol, targetTable, ts.Format("2006-01-02 15:04"), msg.Data.Close)
	}
}

func backfill(db *sql.DB, pair string) {
	targetStart := time.Now().AddDate(-1, 0, 0).UnixMilli()
	
	var lastTime int64
	queryLast := fmt.Sprintf("SELECT COALESCE(EXTRACT(EPOCH FROM MAX(open_time))*1000, 0) FROM %s WHERE pair=$1", targetTable)
	db.QueryRow(queryLast, pair).Scan(&lastTime)
	
	if lastTime > targetStart {
		targetStart = lastTime + 1
	}

	log.Printf("⏳ [%s] Starting Backfill. From: %v", pair, time.UnixMilli(targetStart).Format("2006-01-02"))

	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=15m&startTime=%d&limit=1000", pair, targetStart)
		
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("❌ [%s] HTTP Fetch Error: %v", pair, err)
			break
		}
		
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var klines []BinanceKLine
		if err := json.Unmarshal(body, &klines); err != nil {
			log.Printf("❌ [%s] JSON Decode Error: %v", pair, err)
			break
		}

		if len(klines) <= 1 {
			log.Printf("🏁 [%s] Backfill finished. Reached the present.", pair)
			break
		}

		// Insert batch
		count := 0
		for _, k := range klines {
			// Small internal loop to save. For 1 year, batching this with a transaction 
			// would be better, but for logging we show progress.
			ts := time.UnixMilli(int64(k[0].(float64)))
			_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pair, open_time, open, high, low, close, volume) 
				VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, targetTable),
				pair, ts, k[1].(string), k[2].(string), k[3].(string), k[4].(string), k[5].(string))
			if err == nil { count++ }
		}

		lastCandleTime := int64(klines[len(klines)-1][0].(float64))
		targetStart = lastCandleTime + 1
		
		log.Printf("📑 [%s] Ingested %d candles. Current marker: %v", pair, count, time.UnixMilli(lastCandleTime).Format("2006-01-02 15:04"))

		if len(klines) < 1000 { 
			log.Printf("🏁 [%s] Backfill caught up.", pair)
			break 
		}
		time.Sleep(250 * time.Millisecond) // Respect rate limits
	}
}

func loadPairs() []string {
	var out []string
	if pairsOverride != "" {
		out = strings.Split(strings.ToUpper(pairsOverride), ",")
	} else {
		out = []string{"BTCUSDT", "ETHUSDT"}
	}
	log.Printf("🔍 Monitoring %d pairs: %v", len(out), out)
	return out
}