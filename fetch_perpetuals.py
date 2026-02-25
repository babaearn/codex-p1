import requests
import json
import time
import csv


def load_secrets():
    try:
        with open("secrets.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print("secrets.json not found. Please create it with your Mudrex API credentials.")
        return {}

def fetch_mudrex_perpetuals(secrets):
    print("Fetching Mudrex Futures...")
    api_key = secrets.get("mudrex_api_key")
    # Base URL from secrets or default to what we found
    base_url = secrets.get("mudrex_base_url", "https://trade.mudrex.com/fapi/v1/")
    
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print("Skipping Mudrex: API key not configured in secrets.json")
        return []

    # Validated endpoint: /futures with pagination
    url = f"{base_url.rstrip('/')}/futures"
    
    api_secret = secrets.get("mudrex_api_secret")
    headers = {
        "X-Authentication": api_secret, 
        "Content-Type": "application/json"
    }

    active_pairs = []
    offset = 0
    limit = 100 # Using a reasonable limit

    try:
        while True:
            params = {"offset": offset, "limit": limit}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Handle potential response structures
            results = data if isinstance(data, list) else data.get('data', [])
            
            if not results:
                break
                
            current_batch_count = 0
            for item in results:
                symbol = item.get('symbol') or item.get('name')
                if symbol:
                    active_pairs.append(symbol)
                    current_batch_count += 1
            
            if current_batch_count < limit:
                break
                
            offset += limit
            time.sleep(0.1) # Rate limit politeness
        
        return sorted(active_pairs)

    except Exception as e:
        print(f"Error fetching Mudrex data: {e}")
        return []


def fetch_binance_perpetuals():
    print("Fetching Binance USDT Futures...")
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        active_pairs = []
        for symbol in data['symbols']:
            if symbol['status'] == 'TRADING' and symbol['contractType'] == 'PERPETUAL':
                active_pairs.append(symbol['symbol'])
        
        return sorted(active_pairs)
    except Exception as e:
        print(f"Error fetching Binance data: {e}")
        return []

def fetch_bybit_perpetuals():
    print("Fetching Bybit Linear Contracts...")
    try:
        # category=linear covers USDT and USDC perpetuals
        # limit=1000 to ensure we get all (default is often small)
        url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
        
        active_pairs = []
        cursor = ""
        
        while True:
            params = ""
            if cursor:
               request_url = f"{url}&cursor={cursor}"
            else:
               request_url = url
               
            response = requests.get(request_url)
            response.raise_for_status()
            data = response.json()
            
            if data['retCode'] != 0:
                print(f"Bybit API Error: {data['retMsg']}")
                break
                
            for item in data['result']['list']:
                # Filter specifically for LinearPerpetual to avoid futures with expiration dates
                # Also double check symbol ends with USDT as requested
                is_usdt_perp = (
                    item['status'] == 'Trading' and 
                    item.get('contractType') == 'LinearPerpetual' and 
                    item['symbol'].endswith('USDT')
                )
                if is_usdt_perp:
                    active_pairs.append(item['symbol'])
            
            cursor = data['result'].get('nextPageCursor')
            if not cursor:
                break
            
            # Rate limit politeness
            time.sleep(0.1)
            
        return sorted(active_pairs)
    except Exception as e:
        print(f"Error fetching Bybit data: {e}")
        return []

def main():
    secrets = load_secrets()
    
    binance_pairs = fetch_binance_perpetuals()
    bybit_pairs = fetch_bybit_perpetuals()
    mudrex_pairs = fetch_mudrex_perpetuals(secrets)
    
    print(f"\n--- Summary ---")
    print(f"Binance USDT-M Active Pairs: {len(binance_pairs)}")
    print(f"Bybit Linear Active Pairs: {len(bybit_pairs)}")
    
    mudrex_set = set(mudrex_pairs)
    bybit_set = set(bybit_pairs)
    binance_set = set(binance_pairs)
    
    # Pairs in Bybit but NOT in Mudrex
    missing_bybit_in_mudrex = sorted(list(bybit_set - mudrex_set))
    
    # Pairs in Binance but NOT in Mudrex
    missing_binance_in_mudrex = sorted(list(binance_set - mudrex_set))
    
    if mudrex_pairs:
        print(f"Mudrex Active Pairs: {len(mudrex_pairs)}")
        print(f"Pairs on Bybit but MISSING on Mudrex: {len(missing_bybit_in_mudrex)}")
        print(f"Pairs on Binance but MISSING on Mudrex: {len(missing_binance_in_mudrex)}")
    
    # Save to JSON file
    output_file = "active_perpetuals.json"
    result = {
        "binance": binance_pairs,
        "bybit": bybit_pairs,
        "mudrex": mudrex_pairs,
        "missing_bybit_in_mudrex": missing_bybit_in_mudrex,
        "missing_binance_in_mudrex": missing_binance_in_mudrex,
        "last_updated": int(time.time())
    }
    
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)
    
    print(f"\nSaved list to {output_file}")

    # Save to CSV (Combined)
    csv_file = "active_perpetuals.csv"
    with open(csv_file, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Exchange", "Symbol"])
        for pair in binance_pairs:
            writer.writerow(["Binance", pair])
        for pair in bybit_pairs:
            writer.writerow(["Bybit", pair])
        for pair in mudrex_pairs:
            writer.writerow(["Mudrex", pair])    
            
    print(f"Saved CSV to {csv_file}")

    # Save Bybit only CSV
    bybit_csv_file = "bybit_perpetuals.csv"
    with open(bybit_csv_file, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Symbol"])
        for pair in bybit_pairs:
            writer.writerow([pair])
            
    print(f"Saved Bybit-only CSV to {bybit_csv_file}")
    
    # Save Missing Pairs CSV (Bybit - Mudrex)
    if mudrex_pairs:
        missing_bybit_csv = "mudrex_missing_bybit_pairs.csv"
        with open(missing_bybit_csv, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Symbol (On Bybit but not Mudrex)"])
            for pair in missing_bybit_in_mudrex:
                writer.writerow([pair])
        print(f"Saved Missing Bybit Pairs CSV to {missing_bybit_csv}")

        # Save Missing Pairs CSV (Binance - Mudrex)
        missing_binance_csv = "mudrex_missing_binance_pairs.csv"
        with open(missing_binance_csv, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Symbol (On Binance but not Mudrex)"])
            for pair in missing_binance_in_mudrex:
                writer.writerow([pair])
        print(f"Saved Missing Binance Pairs CSV to {missing_binance_csv}")
    
    # Also print a preview
    print("\nFirst 10 Binance Pairs:")
    print(", ".join(binance_pairs[:10]))
    print("\nFirst 10 Bybit Pairs:")
    print(", ".join(bybit_pairs[:10]))
    if mudrex_pairs:
        print("\nFirst 10 Mudrex Pairs:")
        print(", ".join(mudrex_pairs[:10]))
        print("\nFirst 10 Missing in Mudrex (from Bybit):")
        print(", ".join(missing_bybit_in_mudrex[:10]))
        print("\nFirst 10 Missing in Mudrex (from Binance):")
        print(", ".join(missing_binance_in_mudrex[:10]))

if __name__ == "__main__":
    main()
