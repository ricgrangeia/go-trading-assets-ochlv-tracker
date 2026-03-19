package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

// --- Configuration ---

var (
	postgresDSN   = os.Getenv("POSTGRES_DSN")
	pairsOverride = os.Getenv("PAIRS_OVERRIDE")
	targetTable   = "ohlcv_15m" // Future-proof: can be changed to ohlcv_1m, etc.
)

// --- Types & Custom Unmarshaling ---

// FlexString handles Binance API inconsistency where numbers sometimes arrive as strings and vice-versa.
type FlexString string

func (fs *FlexString) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		return json.Unmarshal(b, (*string)(fs))
	}
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	*fs = FlexString(fmt.Sprintf("%v", f))
	return nil
}

type KlineStreamMsg struct {
	Symbol string `json:"s"`
	Data   struct {
		OpenTime float64    `json:"t"`
		Open     FlexString `json:"o"`
		High     FlexString `json:"h"`
		Low      FlexString `json:"l"`
		Close    FlexString `json:"c"`
		Volume   FlexString `json:"v"`
		IsClosed bool       `json:"x"`
	} `json:"k"`
}

type ticker24h struct {
	Symbol      string `json:"symbol"`
	QuoteVolume string `json:"quoteVolume"`
}

type BinanceKLine []interface{}

// --- Main Entry Point ---

func main() {
	// High-precision logging for monitoring
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("🛠️ Initializing Binance OHLCV Ingestor (v2.0 - 2026 Edition)")

	if postgresDSN == "" {
		log.Fatal("❌ POSTGRES_DSN environment variable is required")
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal("❌ Database connection error: ", err)
	}
	defer db.Close()

	// 1. Database Setup
	ensureOHLCVTable(db)

	// 2. Load Pairs
	pairs := loadPairs()

	// 3. Start Real-time WebSocket (Background)
	go startRealTimeStream(db, pairs)

	// 4. Start Historical Backfill (Parallel)
	var wg sync.WaitGroup
	for _, pair := range pairs {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			backfill(db, p)
		}(pair)
	}

	// Wait for backfills to finish to confirm the initial "Gap" is closed
	wg.Wait()

	log.Println("🚀 ALL BACKFILLS COMPLETED. System is fully synced.")

	// Keep main alive for the WebSocket goroutine
	select {}
}

// --- Logic Functions ---

func ensureOHLCVTable(db *sql.DB) {
	log.Printf("📋 Verifying table [%s]...", targetTable)
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
    );
    CREATE INDEX IF NOT EXISTS idx_%s_pair_time ON %s (pair, open_time DESC);`, 
	targetTable, targetTable, targetTable)

	if _, err := db.Exec(query); err != nil {
		log.Fatal("❌ Table schema setup failed: ", err)
	}
	log.Printf("✅ Table [%s] is ready and indexed.", targetTable)
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
			log.Printf("⚠️ WS Dial error: %v. Retrying in 10s...", err)
			time.Sleep(10 * time.Second)
			continue
		}

		log.Println("🟢 WebSocket Connected. Listening for closed candles...")

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("🔴 WS Read error: %v", err)
				break
			}

			var msg KlineStreamMsg
			if err := json.Unmarshal(message, &msg); err != nil {
				// This shouldn't happen anymore with FlexString!
				log.Printf("⚠️ JSON Unmarshal error: %v | Raw: %s", err, string(message))
				continue
			}

			if msg.Data.IsClosed {
				log.Printf("📦 [WS] %s candle closed. Writing to DB...", msg.Symbol)
				saveCandle(db, msg)
			}
		}
		conn.Close()
		log.Println("🔁 WebSocket disconnected. Reconnecting in 5s...")
		time.Sleep(5 * time.Second)
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

	_, err := db.Exec(query, 
		msg.Symbol, 
		ts, 
		string(msg.Data.Open), 
		string(msg.Data.High), 
		string(msg.Data.Low), 
		string(msg.Data.Close), 
		string(msg.Data.Volume),
	)

	if err != nil {
		log.Printf("❌ [%s] DB Write Error: %v", msg.Symbol, err)
	} else {
		log.Printf("💾 [%s] Saved Live Sync | TS: %s | Close: %s", msg.Symbol, ts.Format("2006-01-02 15:04"), msg.Data.Close)
	}
}

func backfill(db *sql.DB, pair string) {
	// Calculate 1 year ago
	targetStart := time.Now().AddDate(-1, 0, 0).UnixMilli()
	
	// Resume logic: find where we last left off
	var lastTime int64
	queryLast := fmt.Sprintf("SELECT COALESCE(EXTRACT(EPOCH FROM MAX(open_time))*1000, 0) FROM %s WHERE pair=$1", targetTable)
	db.QueryRow(queryLast, pair).Scan(&lastTime)
	
	if lastTime > targetStart {
		targetStart = lastTime + 1
		log.Printf("⏩ [%s] Resuming backfill from last DB entry: %v", pair, time.UnixMilli(targetStart).Format("2006-01-02"))
	} else {
		log.Printf("⏳ [%s] Starting full 1-year backfill from: %v", pair, time.UnixMilli(targetStart).Format("2006-01-02"))
	}

	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=15m&startTime=%d&limit=1000", pair, targetStart)
		
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("❌ [%s] Network error: %v. Retrying in 5s...", pair, err)
			time.Sleep(5 * time.Second)
			continue
		}
		
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			log.Printf("❌ [%s] Binance API error (%d): %s", pair, resp.StatusCode, string(body))
			break
		}

		var klines []BinanceKLine
		if err := json.Unmarshal(body, &klines); err != nil {
			log.Printf("❌ [%s] Backfill JSON error: %v", pair, err)
			break
		}

		// If we only get 1 or 0 candles, it's either the current unclosed one or we're done
		if len(klines) <= 1 {
			log.Printf("🏁 [%s] Backfill complete for current timeframe.", pair)
			break
		}

		// Transactional Batch Insert
		tx, _ := db.Begin()
		count := 0
		for _, k := range klines {
			ts := time.UnixMilli(int64(k[0].(float64)))
			// Skip the very last candle if it's the one currently being formed (now)
			if ts.After(time.Now().Add(-15 * time.Minute)) { continue }

			_, err = tx.Exec(fmt.Sprintf(`
				INSERT INTO %s (pair, open_time, open, high, low, close, volume) 
				VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (pair, open_time) DO NOTHING`, targetTable),
				pair, ts, k[1].(string), k[2].(string), k[3].(string), k[4].(string), k[5].(string))
			if err == nil { count++ }
		}
		tx.Commit()

		lastMarker := int64(klines[len(klines)-1][0].(float64))
		targetStart = lastMarker + 1
		
		log.Printf("📑 [%s] Batch saved: %d candles. Current date: %v", pair, count, time.UnixMilli(lastMarker).Format("2006-01-02 15:04"))

		if len(klines) < 1000 { 
			log.Printf("🏁 [%s] Caught up to latest history.", pair)
			break 
		}
		
		// Rate limit cushion (Binance weight is 1200/min)
		time.Sleep(300 * time.Millisecond)
	}
}

func loadPairs() []string {
	// If the user provided a manual list, use that
	if pairsOverride != "" {
		return strings.Split(strings.ToUpper(pairsOverride), ",")
	}

	log.Println("🔍 Fetching top 100 USDC pairs by volume from Binance...")

	// 1. Get 24h ticker data for all symbols
	resp, err := http.Get("https://api.binance.com/api/v3/ticker/24hr")
	if err != nil {
		log.Printf("❌ Failed to fetch tickers: %v. Falling back to BTC/ETH.", err)
		return []string{"BTCUSDC", "ETHUSDC"}
	}
	defer resp.Body.Close()

	var allTickers []ticker24h
	json.NewDecoder(resp.Body).Decode(&allTickers)

	// 2. Filter for USDC pairs
	var usdcPairs []ticker24h
	for _, t := range allTickers {
		if strings.HasSuffix(t.Symbol, "USDC") {
			usdcPairs = append(usdcPairs)
		}
	}

	// 3. Sort by QuoteVolume (Descending)
	// Note: We use string comparison or convert to float for accuracy
	sort.Slice(usdcPairs, func(i, j int) bool {
		valI, _ := strconv.ParseFloat(usdcPairs[i].QuoteVolume, 64)
		valJ, _ := strconv.ParseFloat(usdcPairs[j].QuoteVolume, 64)
		return valI > valJ
	})

	// 4. Extract top 100
	limit := 100
	if len(usdcPairs) < limit {
		limit = len(usdcPairs)
	}

	var top100 []string
	for i := 0; i < limit; i++ {
		top100 = append(top100, usdcPairs[i].Symbol)
	}

	log.Printf("✅ Loaded %d pairs. Top 3: %v", len(top100), top100[:3])
	return top100
}



