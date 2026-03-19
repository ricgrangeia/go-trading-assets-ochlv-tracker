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
	targetTable   = "ohlcv_15m"
)

// --- Types & Custom Unmarshaling ---

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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("🛠️ Initializing Binance Perpetual Ingestor (v2.2 - 2026)")

	if postgresDSN == "" {
		log.Fatal("❌ POSTGRES_DSN environment variable is required")
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal("❌ Database connection error: ", err)
	}
	db.SetMaxOpenConns(25)
	defer db.Close()

	// 1. Ensure DB Schema
	ensureOHLCVTable(db)

	// 2. Initial Pair Load
	pairs := loadPairs()

	// 3. Start Real-time WebSocket (Background Goroutine)
	// This stays open forever and handles live closes.
	go startRealTimeStream(db, pairs)

	// 4. Historical Sync Loop
	// We run a full backfill immediately, then every 4 hours.
	ticker := time.NewTicker(4 * time.Hour)
	defer ticker.Stop()

	// Define the worker pool logic as a reusable function
	syncCycle := func(currentPairs []string) {
		log.Printf("⏳ Starting sync cycle for %d pairs...", len(currentPairs))
		jobs := make(chan string, len(currentPairs))
		var wg sync.WaitGroup

		// 10 concurrent workers to avoid Binance IP bans
		for w := 1; w <= 10; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for p := range jobs {
					backfill(db, p)
				}
			}()
		}

		for _, p := range currentPairs {
			jobs <- p
		}
		close(jobs)
		wg.Wait()
		log.Println("🚀 SYNC CYCLE COMPLETE. All gaps filled.")
	}

	// First Run
	syncCycle(pairs)

	// Perpetual Loop
	for {
		select {
		case <-ticker.C:
			log.Println("🔄 Periodic Refresh: Re-scanning Binance Top 100...")
			updatedPairs := loadPairs()
			syncCycle(updatedPairs)
		}
	}
}

// --- Database Logic ---

func ensureOHLCVTable(db *sql.DB) {
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
		log.Fatal("❌ Table setup failed: ", err)
	}
	log.Printf("✅ Table [%s] verified.", targetTable)
}

func saveCandle(db *sql.DB, msg KlineStreamMsg) {
	ts := time.UnixMilli(int64(msg.Data.OpenTime))
	query := fmt.Sprintf(`
        INSERT INTO %s (pair, open_time, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (pair, open_time) DO UPDATE SET 
            open = EXCLUDED.open, high = EXCLUDED.high, 
            low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume
    `, targetTable)

	_, err := db.Exec(query, msg.Symbol, ts, string(msg.Data.Open), string(msg.Data.High), string(msg.Data.Low), string(msg.Data.Close), string(msg.Data.Volume))
	if err != nil {
		log.Printf("❌ [%s] Save Error: %v", msg.Symbol, err)
	}
}

// --- Networking Logic ---

func loadPairs() []string {
	if pairsOverride != "" {
		return strings.Split(strings.ToUpper(pairsOverride), ",")
	}

	resp, err := http.Get("https://api.binance.com/api/v3/ticker/24hr")
	if err != nil {
		log.Printf("⚠️ Ticker fetch failed: %v", err)
		return []string{"BTCUSDC", "ETHUSDC"}
	}
	defer resp.Body.Close()

	var all []ticker24h
	json.NewDecoder(resp.Body).Decode(&all)

	var filtered []ticker24h
	for _, t := range all {
		if strings.HasSuffix(t.Symbol, "USDC") {
			vol, _ := strconv.ParseFloat(t.QuoteVolume, 64)
			if vol > 100000 { // Only coins with > 100k daily volume
				filtered = append(filtered, t)
			}
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		vi, _ := strconv.ParseFloat(filtered[i].QuoteVolume, 64)
		vj, _ := strconv.ParseFloat(filtered[j].QuoteVolume, 64)
		return vi > vj
	})

	limit := 100
	if len(filtered) < limit {
		limit = len(filtered)
	}

	var final []string
	for i := 0; i < limit; i++ {
		final = append(final, filtered[i].Symbol)
	}

	log.Printf("📊 Found %d USDC pairs. Tracking Top %d.", len(filtered), len(final))
	return final
}

func startRealTimeStream(db *sql.DB, pairs []string) {
	var streams []string
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@kline_15m")
	}

	url := "wss://stream.binance.com:9443/ws/" + strings.Join(streams, "/")

	for {
		log.Println("🔌 Connecting WebSocket...")
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var msg KlineStreamMsg
			json.Unmarshal(message, &msg)

			if msg.Data.IsClosed {
				saveCandle(db, msg)
			}
		}
		conn.Close()
		log.Println("🔁 WS Disconnected. Reconnecting...")
		time.Sleep(5 * time.Second)
	}
}

func backfill(db *sql.DB, pair string) {
	// Look back 1 year
	startTime := time.Now().AddDate(-1, 0, 0).UnixMilli()

	// Resume from last record
	var lastDB int64
	query := fmt.Sprintf("SELECT COALESCE(EXTRACT(EPOCH FROM MAX(open_time))*1000, 0) FROM %s WHERE pair=$1", targetTable)
	db.QueryRow(query, pair).Scan(&lastDB)

	if lastDB > startTime {
		startTime = lastDB + 1
	}

	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=15m&startTime=%d&limit=1000", pair, startTime)
		resp, err := http.Get(url)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var klines []BinanceKLine
		if err := json.Unmarshal(body, &klines); err != nil || len(klines) <= 1 {
			break
		}

		tx, _ := db.Begin()
		for _, k := range klines {
			// k[0] = open_time, k[1..5] = OHLCV
			ts := time.UnixMilli(int64(k[0].(float64)))
			if ts.After(time.Now().Add(-15 * time.Minute)) {
				continue // Don't backfill the currently open candle
			}
			tx.Exec(fmt.Sprintf("INSERT INTO %s (pair, open_time, open, high, low, close, volume) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING", targetTable),
				pair, ts, k[1].(string), k[2].(string), k[3].(string), k[4].(string), k[5].(string))
		}
		tx.Commit()

		lastMarker := int64(klines[len(klines)-1][0].(float64))
		startTime = lastMarker + 1

		if len(klines) < 1000 {
			break
		}
		time.Sleep(100 * time.Millisecond) // Weight safety
	}
	log.Printf("🏁 [%s] Backfill Done.", pair)
}