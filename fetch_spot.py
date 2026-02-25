import requests
import json
import time
import csv

def fetch_binance_spot():
    print("Fetching Binance Spot Pairs...")
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        active_assets = set()
        for symbol in data['symbols']:
            if symbol['status'] == 'TRADING' and symbol['quoteAsset'] == 'USDT':
                active_assets.add(symbol['baseAsset'])
        
        return sorted(list(active_assets))
    except Exception as e:
        print(f"Error fetching Binance data: {e}")
        return []

def fetch_bybit_spot():
    print("Fetching Bybit Spot Pairs...")
    try:
        url = "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000"
        
        active_assets = set()
        cursor = ""
        
        while True:
            request_url = f"{url}&cursor={cursor}" if cursor else url
            response = requests.get(request_url)
            response.raise_for_status()
            data = response.json()
            
            if data['retCode'] != 0:
                print(f"Bybit API Error: {data['retMsg']}")
                break
                
            for item in data['result']['list']:
                if item['status'] == 'Trading' and item['quoteCoin'] == 'USDT':
                    active_assets.add(item['baseCoin'])
            
            cursor = data['result'].get('nextPageCursor')
            if not cursor:
                break
            
            time.sleep(0.1)
            
        return sorted(list(active_assets))
    except Exception as e:
        print(f"Error fetching Bybit data: {e}")
        return []

def fetch_mudrex_coins():
    print("Fetching Mudrex Spot Coins (from /api/v1/coins)...")
    try:
        url = "https://mudrex.com/api/v1/coins"
        coins = set()
        offset = 0
        limit = 500 # Validated that 500 works or we can use smaller if needed.
        
        while True:
            params = {"limit": limit, "offset": offset}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Response is a list of objects based on curl output: [{"id":..., "symbol": "AFC", ...}, ...]
            # Or wrapped in data? The curl output showed:
            # `[{"id":"...","symbol":"AFC"...` (It seemed to start with a list bracket or was it truncated?)
            # Wait, curl output step 463 started with `e_strength_index"`. 
            # Let's verify structure safely.
            
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Maybe {"success": true, "data": [...]} ?
                items = data.get('data', [])
            
            if not items:
                print("DEBUG: No items found in response.")
                break
                
            for item in items:
                symbol = item.get('symbol')
                if symbol:
                    coins.add(symbol.upper())
            
            count = len(items)
            print(f"DEBUG: Fetched {count} items in this batch. Total unique coins: {len(coins)}")
            
            if count == 0:
                print("DEBUG: Reached end of list (0 items returned).")
                break
                
            # If we requested limit but got fewer items, it *might* be the end, 
            # OR the server enforces a lower max limit.
            # To be safe, we continue unless it's 0, but we must update offset by actual count.
            offset += count
            
            # Safety break if we got significantly fewer than expected default page (e.g. 1 or 2 items might be end?)
            # But 25 looks like a page size. if we get 25, we continue.
            # If we get < 25 AND we suspect 25 is the max page size, it's the end.
            # But let's just rely on 0 items to stop, or if count < 25 (assuming 25 is the forced limit).
            if count < 25: 
                 # If server forces 25, getting less means end. 
                 # If server respects limit=500, getting less means end.
                 print(f"DEBUG: Reached end of list (batch size {count} < 25).")
                 break
                 
            time.sleep(0.1)
        
        return sorted(list(coins))

    except Exception as e:
        print(f"Error fetching Mudrex data: {e}")
        return []
        
        return sorted(list(set(coins)))

    except Exception as e:
        print(f"Error fetching Mudrex data: {e}")
        return []

def main():
    binance_assets = fetch_binance_spot()
    bybit_assets = fetch_bybit_spot()
    mudrex_coins = fetch_mudrex_coins()
    
    print(f"\n--- Summary (Spot Market) ---")
    print(f"Binance USDT Spot Assets: {len(binance_assets)}")
    print(f"Bybit USDT Spot Assets: {len(bybit_assets)}")
    print(f"Mudrex Supported Coins: {len(mudrex_coins)}")
    
    mudrex_set = set(mudrex_coins)
    bybit_set = set(bybit_assets)
    binance_set = set(binance_assets)
    
    # Missing on Mudrex
    missing_bybit = sorted(list(bybit_set - mudrex_set))
    missing_binance = sorted(list(binance_set - mudrex_set))
    
    print(f"Assets on Bybit Spot but MISSING on Mudrex: {len(missing_bybit)}")
    print(f"Assets on Binance Spot but MISSING on Mudrex: {len(missing_binance)}")
    
    # Output CSVs
    if missing_bybit:
        filename = "mudrex_missing_spot_coins_from_bybit.csv"
        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Asset (On Bybit Spot but not Mudrex)"])
            for asset in missing_bybit:
                writer.writerow([asset])
        print(f"Saved {filename}")

    if missing_binance:
        filename = "mudrex_missing_spot_coins_from_binance.csv"
        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Asset (On Binance Spot but not Mudrex)"])
            for asset in missing_binance:
                writer.writerow([asset])
        print(f"Saved {filename}")
        
    # Preview
    print("\nFirst 10 Mudrex Coins:")
    print(", ".join(mudrex_coins[:10]))
    print("\nFirst 10 Missing (from Bybit):")
    print(", ".join(missing_bybit[:10]))

if __name__ == "__main__":
    main()
