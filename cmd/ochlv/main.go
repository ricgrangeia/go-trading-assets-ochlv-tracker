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
	log.Println("🛠️ Initializing Binance OHLCV Ingestor (v2.1 - 2026 Edition)")

	if postgresDSN == "" {
		log.Fatal("❌ POSTGRES_DSN environment variable is required")
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal("❌ Database connection error: ", err)
	}
	db.SetMaxOpenConns(25) // Prevent "too many clients" error
	defer db.Close()

	ensureOHLCVTable(db)
	pairs := loadPairs()

	// 1. Start Real-time WebSocket
	go startRealTimeStream(db, pairs)

	// 2. Start Historical Backfill with a Worker Pool (Concurrency of 10)
	log.Println("⏳ Starting throttled backfill for 100 pairs...")
	jobs := make(chan string, len(pairs))
	var wg sync.WaitGroup

	// Start 10 workers
	for w := 1; w <= 10; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				backfill(db, p)
			}
		}()
	}

	// Feed jobs
	for _, pair := range pairs {
		jobs <- pair
	}
	close(jobs)

	wg.Wait()
	log.Println("🚀 ALL BACKFILLS COMPLETED. System is fully synced.")

	select {}
}

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
		log.Fatal("❌ Table schema setup failed: ", err)
	}
}

func startRealTimeStream(db *sql.DB, pairs []string) {
	var streams []string
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@kline_15m")
	}

	url := "wss://stream.binance.com:9443/ws/" + strings.Join(streams, "/")

	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("⚠️ WS Dial error: %v. Retrying in 10s...", err)
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
		time.Sleep(5 * time.Second)
	}
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

	db.Exec(query, msg.Symbol, ts, string(msg.Data.Open), string(msg.Data.High), string(msg.Data.Low), string(msg.Data.Close), string(msg.Data.Volume))
}

func backfill(db *sql.DB, pair string) {
	targetStart := time.Now().AddDate(-1, 0, 0).UnixMilli()
	var lastTime int64
	queryLast := fmt.Sprintf("SELECT COALESCE(EXTRACT(EPOCH FROM MAX(open_time))*1000, 0) FROM %s WHERE pair=$1", targetTable)
	db.QueryRow(queryLast, pair).Scan(&lastTime)

	if lastTime > targetStart {
		targetStart = lastTime + 1
	}

	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=15m&startTime=%d&limit=1000", pair, targetStart)
		resp, err := http.Get(url)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var klines []BinanceKLine
		json.Unmarshal(body, &klines)

		if len(klines) <= 1 {
			break
		}

		tx, _ := db.Begin()
		for _, k := range klines {
			ts := time.UnixMilli(int64(k[0].(float64)))
			if ts.After(time.Now().Add(-15 * time.Minute)) {
				continue
			}
			tx.Exec(fmt.Sprintf(`INSERT INTO %s (pair, open_time, open, high, low, close, volume) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, targetTable),
				pair, ts, k[1].(string), k[2].(string), k[3].(string), k[4].(string), k[5].(string))
		}
		tx.Commit()

		lastMarker := int64(klines[len(klines)-1][0].(float64))
		targetStart = lastMarker + 1

		if len(klines) < 1000 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("🏁 [%s] Sync Complete.", pair)
}

func loadPairs() []string {
	if pairsOverride != "" {
		return strings.Split(strings.ToUpper(pairsOverride), ",")
	}

	resp, err := http.Get("https://api.binance.com/api/v3/ticker/24hr")
	if err != nil {
		return []string{"BTCUSDC", "ETHUSDC"}
	}
	defer resp.Body.Close()

	var allTickers []ticker24h
	json.NewDecoder(resp.Body).Decode(&allTickers)

	var usdcPairs []ticker24h
	for _, t := range allTickers {
		// Ensure it ends with USDC and has some volume
		if strings.HasSuffix(t.Symbol, "USDC") {
			v, _ := strconv.ParseFloat(t.QuoteVolume, 64)
			if v > 0 {
				usdcPairs = append(usdcPairs, t)
			}
		}
	}

	// DEBUG PRINT: Check your logs for this line!
	log.Printf("📊 Binance found %d total USDC pairs with volume > 0", len(usdcPairs))

	sort.Slice(usdcPairs, func(i, j int) bool {
		valI, _ := strconv.ParseFloat(usdcPairs[i].QuoteVolume, 64)
		valJ, _ := strconv.ParseFloat(usdcPairs[j].QuoteVolume, 64)
		return valI > valJ
	})

	limit := 100
	if len(usdcPairs) < limit {
		limit = len(usdcPairs)
	}

	var top100 []string
	for i := 0; i < limit; i++ {
		top100 = append(top100, usdcPairs[i].Symbol)
	}

	return top100
}