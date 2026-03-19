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
	log.Println("🛠️ Initializing Binance Perpetual Ingestor (v2.5 - 2026)")

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

func runFullCycle(db *sql.DB) {
	if wsStopChan != nil {
		log.Println("🛑 Stopping current WebSocket to update pair list...")
		close(wsStopChan)
		wsWg.Wait()
	}

	pairs := loadPairs()
	totalPairs := len(pairs)

	wsStopChan = make(chan struct{})
	wsWg.Add(1)
	go startRealTimeStream(db, pairs, wsStopChan, &wsWg)

	log.Printf("⏳ Starting backfill for %d pairs using 10 workers...", totalPairs)
	
	jobs := make(chan string, totalPairs)
	var backfillWg sync.WaitGroup
	
	var processedCount int32
	var mu sync.Mutex

	for w := 1; w <= 10; w++ {
		backfillWg.Add(1)
		go func(id int) {
			defer backfillWg.Done()
			for p := range jobs {
				backfill(db, p, id)
				mu.Lock()
				processedCount++
				log.Printf("📈 Overall Progress: [%d/%d] pairs finished.", processedCount, totalPairs)
				mu.Unlock()
			}
		}(w)
	}

	for _, p := range pairs {
		jobs <- p
	}
	close(jobs)
	backfillWg.Wait()
	log.Println("🚀 SYNC CYCLE COMPLETE. All systems nominal.")
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
		log.Fatal("❌ Failed to create table:", err)
	}
	log.Printf("✅ Table [%s] verified.", targetTable)
}

func loadPairs() []string {
	if pairsOverride != "" {
		return strings.Split(strings.ToUpper(pairsOverride), ",")
	}

	log.Println("🔍 Scanning Binance for Top 50 USDC Pairs...")
	resp, err := http.Get("https://api.binance.com/api/v3/ticker/24hr")
	if err != nil {
		return []string{"BTCUSDC", "ETHUSDC", "SOLUSDC"}
	}
	defer resp.Body.Close()

	var all []ticker24h
	json.NewDecoder(resp.Body).Decode(&all)

	var usdcPairs []ticker24h
	for _, t := range all {
		// Strictly USDC quoted, excluding other stables like USDTUSDC
		if strings.HasSuffix(t.Symbol, "USDC") && !strings.Contains(t.Symbol, "USDT") {
			vol, _ := strconv.ParseFloat(t.QuoteVolume, 64)
			if vol > 0 {
				usdcPairs = append(usdcPairs, t)
			}
		}
	}

	sort.Slice(usdcPairs, func(i, j int) bool {
		vi, _ := strconv.ParseFloat(usdcPairs[i].QuoteVolume, 64)
		vj, _ := strconv.ParseFloat(usdcPairs[j].QuoteVolume, 64)
		return vi > vj
	})

	limit := 50
	if len(usdcPairs) < limit {
		limit = len(usdcPairs)
	}

	var final []string
	for i := 0; i < limit; i++ {
		final = append(final, usdcPairs[i].Symbol)
	}

	log.Printf("📊 Top %d USDC pairs found. Primary: %s (Volume: %s)", len(final), final[0], usdcPairs[0].QuoteVolume)
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
			log.Println("🔌 Dialing WebSocket...")
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				time.Sleep(5 * time.Second)
				continue
			}

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
			case err := <-readErrChan:
				log.Printf("⚠️ WS Error: %v. Reconnecting...", err)
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
	
	_, err := db.Exec(query, msg.Symbol, ts, string(msg.Data.Open), string(msg.Data.High), string(msg.Data.Low), string(msg.Data.Close), string(msg.Data.Volume))
	if err == nil {
		log.Printf("✅ [%s] Live Candle Saved: %s", msg.Symbol, ts.Format("2006-01-02 15:04"))
	}
}

func backfill(db *sql.DB, pair string, workerID int) {
	startTime := time.Now().AddDate(-1, 0, 0).UnixMilli()
	var lastDB int64
	query := fmt.Sprintf("SELECT COALESCE(EXTRACT(EPOCH FROM MAX(open_time))*1000, 0) FROM %s WHERE pair=$1", targetTable)
	db.QueryRow(query, pair).Scan(&lastDB)

	if lastDB > startTime {
		startTime = lastDB + 1
	}

	log.Printf("👷 [Worker %d] Syncing %s from %s", workerID, pair, time.UnixMilli(startTime).Format("2006-01-02"))

	totalInserted := 0
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
		batchCount := 0
		for _, k := range klines {
			ts := time.UnixMilli(int64(k[0].(float64)))
			if ts.After(time.Now().Add(-15 * time.Minute)) {
				continue
			}
			_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (pair, open_time, open, high, low, close, volume) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING", targetTable),
				pair, ts, k[1].(string), k[2].(string), k[3].(string), k[4].(string), k[5].(string))
			if err == nil {
				batchCount++
			}
		}
		tx.Commit()

		totalInserted += batchCount
		lastMarker := int64(klines[len(klines)-1][0].(float64))
		startTime = lastMarker + 1

		if len(klines) < 1000 {
			break
		}
		time.Sleep(200 * time.Millisecond) // Safety delay
	}

	if totalInserted > 0 {
		log.Printf("🏁 [%s] Finished. Added %d candles.", pair, totalInserted)
	} else {
		log.Printf("🏁 [%s] Up-to-date.", pair)
	}
}