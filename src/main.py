import asyncio
import websockets
import json
import os
from pybit.unified_trading import HTTP
from pump_monitor import PumpMonitor, PumpState
from order_manager import FuturesOrders
from aiogram_bot import send_notification, format_position_close_notification, format_position_open_notification

API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
cl = HTTP(api_key=API_KEY, api_secret=SECRET_KEY, demo=True)

BASE_POSITION = 1000
MAX_POSITIONS = 3

tickers_mapping = {
    "btcusdt": 1,
    "ethusdt": 2,
    "solusdt": 3,
    "xrpusdt": 4,
    "adausdt": 5,
    "dogeusdt": 6,
    "mntusdt": 7
}

tickers = list(tickers_mapping.keys())
streams = "/".join([f"{t}@kline_1m" for t in tickers])
url = f"wss://stream.binance.com:9443/stream?streams={streams}"

monitoring = {
    s: PumpMonitor(s, pump_started_condition=0.01) for s in tickers
}
orders = {
    s: FuturesOrders(cl, s.upper()) for s in tickers
}

async def listen():
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            raw = json.loads(msg)
            data = raw["data"]["k"]
            
            if not data["x"]:
                continue
                
            # Convert Binance WebSocket format to our kline format
            kline = {
                "s": data["s"].lower(),  # Symbol in lowercase
                "o": data["o"],          # Open price
                "c": data["c"],          # Close price
                "t": data["t"],          # Open time
                "T": data["T"],          # Close time
                "v": data["v"]           # Volume
            }
            
            print("Внешний принт свечи:")
            print(kline)
            print("Внутренний принт обработки:")
            
            symbol = kline["s"]
            monitoring[symbol].process_kline(kline)

            state = monitoring[symbol].state 
            n_positions = len(cl.get_positions(category="linear", settleCoin="USDT")["result"]["list"])
            pos_im = float(cl.get_positions(category="linear", symbol=symbol.upper())["result"]["list"][0]["positionValue"] or 0)

            # Trading logic
            if state in [PumpState.COOLING_OFF, PumpState.DUMPED, PumpState.RETESTED] and pos_im > 0:
                position_details = orders[symbol].get_position()
                orders[symbol].close_position()
                
                message = format_position_close_notification(symbol, position_details)
                send_notification(message)
                
            elif n_positions < MAX_POSITIONS or pos_im > 0:
                print('-'*50, pos_im, '-'*50)
                if state == PumpState.STARTED and pos_im < BASE_POSITION/2 * 0.9:
                    orders[symbol].place_market_order_by_quote(BASE_POSITION/2, side="buy")
                    
                    position_details = orders[symbol].get_position()
                    message = format_position_open_notification(
                        symbol, position_details, "STARTED", BASE_POSITION/2
                    )
                    send_notification(message)
                        
                elif state == PumpState.CONFIRMED and pos_im < BASE_POSITION * 0.9:
                    orders[symbol].place_market_order_by_quote(BASE_POSITION/2, side="buy")
                    
                    position_details = orders[symbol].get_position()
                    message = format_position_open_notification(
                        symbol, position_details, "CONFIRMED", BASE_POSITION/2
                    )
                    send_notification(message)
                        
                elif state == PumpState.STABILIZED and pos_im < BASE_POSITION * 0.9: 
                    orders[symbol].place_market_order_by_quote(BASE_POSITION, side="buy")
                    
                    position_details = orders[symbol].get_position()
                    message = format_position_open_notification(
                        symbol, position_details, "STABILIZED", BASE_POSITION
                    )
                    send_notification(message)

if __name__ == "__main__":
    asyncio.run(listen())