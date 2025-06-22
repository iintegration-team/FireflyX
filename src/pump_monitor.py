import time
import pandas as pd
import os

BASE_REMAINING = 120
START_REMAINING = 2
PUPM_STARTED_CONDITION = 0.01
GREEN_COUNT_CONDITION = 3
PUMP_STABILIZED_CONDITION = 0.25 
PUMP_COOLING_OFF_CONDITION = 0.1
PUMP_DUMPED_CONDITION = 0.55

tickers_mapping = {
    "btcusdt": 1,
    "ethusdt": 2,
    "solusdt": 3,
    "xrpusdt": 4,
    "adausdt": 5
}


class PumpState:
    BASE = "BASE"
    STARTED = "STARTED"
    CONFIRMED = "CONFIRMED"
    COOLING_OFF = "COOLING_OFF"
    STABILIZED = "STABILIZED"
    DUMPED = "DUMPED"
    RETESTED = "RETESTED"

class PumpMonitor:
    def __init__(self, symbol, pump_started_condition=PUPM_STARTED_CONDITION):
        self.symbol = symbol.lower()
        self.pump_started_condition = pump_started_condition
        
        self.remaining = 0
        self.start_price = None
        self.max_price = None
        self.state = PumpState.BASE
        self.green_count = 0
        
        self.data_rows = []

    def print_state(self, kline):
        self.remaining -= 1
        print('-'*100)
        print(self.pump_id )
        print(
            f"{self.symbol}: o={kline.get('o')}, c={kline.get('c')}, {kline.get('datetime', '')} | "
            f"start={self.start_price}, max={self.max_price}, +{self.pct_start_to_max}%, {self.pct_curr_to_max}% from max | remaining={self.remaining}"
        )
        print(f"[{self.symbol}] Current state: {self.state}")
        
        self.data_rows.append({
            'pump_id': self.pump_id,
            'symbol': self.symbol,
            'o': kline.get('o'),
            'c': kline.get('c'),
            'datetime': kline.get('T', ''),
            'start_price': self.start_price,
            'max_price': self.max_price,
            'pct_start_to_max': self.pct_start_to_max,
            'pct_curr_to_max': self.pct_curr_to_max,
            'remaining': self.remaining,
            'state': self.state
        })

    def process_kline(self, kline):
        current = float(kline["c"])
        o = float(kline["o"])

        if self.state == PumpState.BASE:
            if (current - o) / o > self.pump_started_condition:
                self.remaining = START_REMAINING
                self.start_price = o
                self.max_price = current
                self.state = PumpState.STARTED
                self.green_count = 0
                try:
                    time_id = str(kline["T"])
                except:
                    time_id = str(time.time())

                self.pump_id = f"{self.symbol}_{time_id}"
                print(f"Pump started on symbol: {self.symbol}")
            else:
                if self.remaining > 0:
                    self.print_state(kline)
                return
        
        self.max_price = max(current, self.max_price)
        self.pct_start_to_max = (self.max_price - self.start_price) / self.start_price * 100
        self.pct_curr_to_max = (current - self.max_price) / self.max_price * 100
        candle_is_green = current > o

        if self.state == PumpState.STARTED:
            if candle_is_green:
                self.green_count += 1
                if self.green_count >= GREEN_COUNT_CONDITION:
                    self.state = PumpState.CONFIRMED
                    self.remaining = BASE_REMAINING
                    print(f"Pump confirmed: {self.symbol}")
            else:
                print(f"DEBUG: not green candle, pump not confirmed, go to BASE")
                self.start_price = None
                self.max_price = None
                self.state = PumpState.BASE
                self.green_count = 0

        elif self.state == PumpState.CONFIRMED:
            if self.pct_start_to_max * PUMP_COOLING_OFF_CONDITION <= abs(self.pct_curr_to_max):
                self.state = PumpState.COOLING_OFF
                self.remaining = BASE_REMAINING
                print(f"Pump is cooling off: {self.symbol}")

        elif self.state == PumpState.COOLING_OFF:
            if self.pct_start_to_max * PUMP_STABILIZED_CONDITION <= abs(self.pct_curr_to_max):
                self.state = PumpState.STABILIZED
                self.remaining = BASE_REMAINING
                print(f"Price rolled back by a quarter of the growth: {self.symbol}")

        elif self.state == PumpState.STABILIZED:
            if current >= self.max_price:
                self.state = PumpState.RETESTED
                self.remaining = BASE_REMAINING
                print(f"Price reached the pump peak: {self.symbol}")
            elif self.pct_start_to_max * PUMP_DUMPED_CONDITION <= abs(self.pct_curr_to_max):
                self.state = PumpState.DUMPED
                self.remaining = BASE_REMAINING
                print(f"Price rolled back by more than half of the growth: {self.symbol}")
                
        elif self.state == PumpState.DUMPED:
            self.state = PumpState.BASE


        if self.remaining == 0:
            if self.data_rows:
                df = pd.DataFrame(self.data_rows)
                os.makedirs('/home/koshkidadanet/My Files/FireflyX/pump_data', exist_ok=True)
                parquet_path = f"/home/koshkidadanet/My Files/FireflyX/pump_data/{self.pump_id}.parquet"
                df.to_parquet(parquet_path)
                print(f"Saved pump data to {parquet_path}")
                self.data_rows = []
            
            self.state = PumpState.BASE
            self.start_price = None
            self.max_price = None
            self.green_count = 0
        else:
            self.print_state(kline)