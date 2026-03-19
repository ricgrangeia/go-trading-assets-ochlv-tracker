import os
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from litellm import completion

# 1. Setup high-visibility logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_top_100_usdc_pairs():
    """Fetches the top 100 USDC pairs by 24h quote volume from Binance."""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        
        # Filter for USDC pairs
        df = df[df['symbol'].str.endswith('USDC')]
        
        # Sort by quoteVolume (Total USDC traded)
        df['quoteVolume'] = pd.to_numeric(df['quoteVolume'])
        top_100 = df.sort_values(by='quoteVolume', ascending=False).head(100)
        
        return top_100['symbol'].tolist()
    except Exception as e:
        logging.error(f"⚠️ Failed to fetch top pairs from Binance: {e}")
        return ["BTCUSDC", "ETHUSDC", "SOLUSDC"] # Fallback essentials

def run_analysis_cycle():
    # --- Configuration from Environment ---
    db_url = os.getenv("POSTGRES_DSN")
    if db_url and db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    model_name = os.getenv("VLLM_MODEL") 
    vllm_url = os.getenv("VLLM_URL")      
    vllm_key = os.getenv("VLLM_API_KEY", "vllm-local-key")
    
    if not model_name or not model_name.startswith("openai/"):
        logging.error("❌ VLLM_MODEL must start with 'openai/'")
        return

    # 1. Get the current Top 100 USDC Pairs
    logging.info("🔍 Fetching current Top 100 USDC pairs by volume...")
    pairs_to_analyze = get_top_100_usdc_pairs()

    # 2. Initialize Database Engine
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        for pair in pairs_to_analyze:
            logging.info(f"📊 Processing {pair}...")
            
            # Query the last 10 days of 15m candles
            query = """
                SELECT open_time, open, high, low, close, volume 
                FROM ohlcv_15m 
                WHERE pair = :pair AND open_time > NOW() - INTERVAL '10 days' 
                ORDER BY open_time ASC
            """
            
            try:
                with engine.connect() as conn:
                    df = pd.read_sql(text(query), conn, params={"pair": pair})

                if df.empty:
                    # Skip silently if we don't have data for this specific top-100 coin yet
                    continue

                # --- DATA COMPRESSION (Crucial for Token Limits) ---
                # Rounding numbers drastically reduces token count
                df = df.round({
                    'open': 4, 
                    'high': 4, 
                    'low': 4, 
                    'close': 4, 
                    'volume': 2
                })

                # Ensure we don't exceed model context: 400 candles is ~4 days of 15m data
                if len(df) > 400:
                    df = df.tail(400)

                csv_payload = df.to_csv(index=False)
                
                # 3. Prepare AI Prompt
                messages = [
                    {
                        "role": "system", 
                        "content": "You are a professional crypto quant. Analyze OHLCV data. Be extremely concise and objective."
                    },
                    {
                        "role": "user", 
                        "content": (
                            f"Analyze these 15m candles for {pair}:\n\n{csv_payload}\n\n"
                            "Format:\n1. TREND: (Bullish/Bearish/Neutral)\n2. LEVELS: Support/Resistance\n3. SIGNAL: (BUY/SELL/WAIT)\n4. RATIONALE: (1 sentence)"
                        )
                    }
                ]

                # 4. Call vLLM via LiteLLM
                response = completion(
                    model=model_name,
                    messages=messages,
                    api_base=vllm_url,
                    api_key=vllm_key,
                    temperature=0.1,
                    max_tokens=300 # Shorter output saves time
                )
                
                analysis = response.choices[0].message.content
                print(f"\n📈 {pair} ANALYSIS:\n{analysis}\n{'-'*30}", flush=True)
                
            except Exception as item_err:
                logging.error(f"❌ Error processing {pair}: {item_err}")

    except Exception as e:
        logging.error(f"❌ Critical Cycle Error: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    interval_min = int(os.getenv("ANALYSIS_INTERVAL_MINUTES", 15))
    logging.info(f"🤖 AI Analyzer active. Scanning Top 100 USDC pairs every {interval_min}m.")
    
    while True:
        run_analysis_cycle()
        logging.info(f"😴 Cycle complete. Waiting {interval_min}m...")
        time.sleep(interval_min * 60)