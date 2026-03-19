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

// --- Types ---

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

// --- Global State for WebSocket Management ---
var (
	wsStopChan chan struct{}
	wsWg       sync.WaitGroup
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("🛠️ Initializing Binance Perpetual Ingestor (v2.3 - 2026)")

	if postgresDSN == "" {
		log.Fatal("❌ POSTGRES_DSN environment variable is required")
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal("❌ Database connection error: ", err)
	}
	db.SetMaxOpenConns(50)
	defer db.Close()

	ensureOHLCVTable(db)

	// ticker for refreshing the top 100 list and re-running backfills
	refreshTicker := time.NewTicker(4 * time.Hour)
	defer refreshTicker.Stop()

	// Initial Run
	runFullCycle(db)

	for {
		select {
		case <-refreshTicker.C:
			log.Println("🔄 4-Hour Trigger: Refreshing Pair List and Syncing...")
			runFullCycle(db)
		}
	}
}

// runFullCycle stops the current WS, finds new pairs, starts a new WS, and runs backfill
func runFullCycle(db *sql.DB) {
	// 1. Stop existing WebSocket if running
	if wsStopChan != nil {
		log.Println("🛑 Stopping current WebSocket to update pair list...")
		close(wsStopChan)
		wsWg.Wait()
	}

	// 2. Load the freshest Top 100 USDC pairs
	pairs := loadPairs()

	// 3. Start new WebSocket for the new pairs
	wsStopChan = make(chan struct{})
	wsWg.Add(1)
	go startRealTimeStream(db, pairs, wsStopChan, &wsWg)

	// 4. Run historical backfill in parallel workers
	log.Printf("⏳ Starting backfill for %d pairs...", len(pairs))
	jobs := make(chan string, len(pairs))
	var backfillWg sync.WaitGroup

	for w := 1; w <= 10; w++ {
		backfillWg.Add(1)
		go func() {
			defer backfillWg.Done()
			for p := range jobs {
				backfill(db, p)
			}
		}()
	}

	for _, p := range pairs {
		jobs <- p
	}
	close(jobs)
	backfillWg.Wait()
	log.Println("🚀 SYNC CYCLE COMPLETE.")
}

// --- Logic Modules ---

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
	db.Exec(query)
	log.Printf("✅ Table [%s] ready.", targetTable)
}

func loadPairs() []string {
	if pairsOverride != "" {
		return strings.Split(strings.ToUpper(pairsOverride), ",")
	}

	resp, err := http.Get("https://api.binance.com/api/v3/ticker/24hr")
	if err != nil {
		return []string{"BTCUSDC", "ETHUSDC", "SOLUSDC"}
	}
	defer resp.Body.Close()

	var all []ticker24h
	json.NewDecoder(resp.Body).Decode(&all)

	var usdcPairs []ticker24h
	for _, t := range all {
		// Strictly filter for USDC as the quote currency
		if strings.HasSuffix(t.Symbol, "USDC") {
			vol, _ := strconv.ParseFloat(t.QuoteVolume, 64)
			if vol > 0 {
				usdcPairs = append(usdcPairs, t)
			}
		}
	}

	// Sort by 24h Volume (QuoteVolume is in USDC)
	sort.Slice(usdcPairs, func(i, j int) bool {
		vi, _ := strconv.ParseFloat(usdcPairs[i].QuoteVolume, 64)
		vj, _ := strconv.ParseFloat(usdcPairs[j].QuoteVolume, 64)
		return vi > vj
	})

	limit := 100
	if len(usdcPairs) < limit {
		limit = len(usdcPairs)
	}

	var final []string
	for i := 0; i < limit; i++ {
		final = append(final, usdcPairs[i].Symbol)
	}

	log.Printf("📊 Top 100 USDC Pairs loaded. (Top: %s)", final[0])
	return final
}

func startRealTimeStream(db *sql.DB, pairs []string, stopChan chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var streams []string
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@kline_15m")
	}

	url := "wss://stream.binance.com:9443/ws/" + strings.Join(streams, "/")

	for {
		select {
		case <-stopChan:
			return
		default:
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				time.Sleep(5 * time.Second)
				continue
			}

			// Inner loop for reading
			readErrChan := make(chan error, 1)
			go func() {
				for {
					_, message, err := conn.ReadMessage()
					if err != nil {
						readErrChan <- err
						return
					}
					var msg KlineStreamMsg
					json.Unmarshal(message, &msg)
					if msg.Data.IsClosed {
						saveCandle(db, msg)
					}
				}
			}()

			select {
			case <-stopChan:
				conn.Close()
				return
			case <-readErrChan:
				conn.Close()
				time.Sleep(5 * time.Second)
			}
		}
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
	startTime := time.Now().AddDate(-1, 0, 0).UnixMilli()
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
			ts := time.UnixMilli(int64(k[0].(float64)))
			if ts.After(time.Now().Add(-15 * time.Minute)) {
				continue
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
		time.Sleep(150 * time.Millisecond)
	}
}