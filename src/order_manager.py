from pump_monitor import PumpMonitor, PumpState

import decimal
import inspect
import time
from typing import Optional

from pybit import exceptions

class FuturesOrders:
    def __init__(self, cl, symbol):
        """
        Конструктор класса и инициализация
        - клиента pybit
        - получение параметров и фильтров Инструмента
        """
        self.cl = cl
        self.symbol = symbol
        self.category = "linear"
        self.price_decimals, self.qty_decimals, self.min_qty=self.get_filters()

    def get_filters(self):
        """
        Фильтры заданного инструмента
        - макс колво знаков в аргументах цены,
        - мин размер ордера в Базовой Валюте,
        - макс размер ордера в БВ
        """
        r = self.cl.get_instruments_info(symbol=self.symbol, category=self.category)
        c = r.get('result', {}).get('list', [])[0]
        # print(c)
        min_qty = c.get('lotSizeFilter', {}).get('minOrderQty', '0.0')
        qty_decimals = abs(decimal.Decimal(min_qty).as_tuple().exponent)
        price_decimals = int(c.get('priceScale', '4'))
        min_qty = float(min_qty)

        self.log(price_decimals, qty_decimals, min_qty)
        return price_decimals, qty_decimals, min_qty

    def get_price(self):
        """
        Один из способов получения текущей цены
        """
        r = float(self.cl.get_tickers(category=self.category, symbol=self.symbol).get('result').get('list')[0].get('ask1Price'))
        self.log(r)
        return r

    def get_position(self, key : Optional[str] = None):
        """
        Получаю текущую позицию
        :param key:
        :return:
        """
        r = self.cl.get_positions(category=self.category, symbol=self.symbol)
        p = r.get('result', {}).get('list', [])[0]
        qty = float(p.get('size', '0.0'))
        if qty <= 0.0: raise Exception("empty position")

        ret = dict(
            avg_price=float(p.get('avgPrice', '0.0')),
            side=p.get('side'),
            unrel_pnl=float(p.get('unrealisedPnl', '0.0')),
            qty=qty
        )
        ret['rev_side'] = ("Sell", "Buy")[ret['side'] == 'Sell']
        self.log(ret)

        return ret.get(key) if key else ret

    def place_limit_order_by_percent(
            self,
            qty : float=0.00001,
            side : str="Sell",
            distance_perc : int=2,
            order_link_id : Optional[str]=None
    ):
        """
        Установка лимитного ордера по инструменту определенному в конструкторе класса
        в процентах от текущей цены
        в зависимости от направления лимитного ордера цена стопа расчитывается в разные стороны
        """
        curr_price =self.get_price()
        order_price = self.calculate_limit_price_perc(curr_price, side, distance_perc)
        if not order_link_id: order_link_id = f"FireflyX_{self.symbol}_{time.time()}"

        args = dict(
            category=self.category,
            symbol=self.symbol,
            side=side.capitalize(),
            orderType="Limit",
            qty=self.floor_qty(qty),
            price=self.floor_price(order_price),
            orderLinkId=order_link_id
        )
        self.log("args", args)

        r = self.cl.place_order(**args)
        self.log("result", r)

        return r

    def place_market_order_by_base(self, qty : float=0.00001, side : str="Sell"):
        """
        Размещение рыночного ордера с указанием размера ордера в Базовой Валюте (BTC, XRP, etc)
        :param qty:
        :param side:
        :return:
        """
        args = dict(
            category=self.category,
            symbol=self.symbol,
            side=side.capitalize(),
            orderType="Market",
            qty=self.floor_qty(qty),
            orderLinkId=f"FireflyX__{self.symbol}_{time.time()}"
        )
        self.log("args", args)

        r = self.cl.place_order(**args)
        self.log("result", r)

        return r

    def place_market_order_by_quote(self, quote: float=5.0, side: str="Sell"):
        """
        Отправка ордера с размером позиции в Котируемой Валюте (USDT напр)
        имеет смысл только для контрактов
        (для спота есть аргумент marketUnit, см. https://youtu.be/e7Np2ICYBzg )
        """
        curr_price = self.get_price()
        qty = self.floor_qty(quote / curr_price)
        if qty < self.min_qty: raise Exception(f"{qty} is to small")

        self.place_market_order_by_base(qty, side)


    def cancel_open_order_by_order_link_id(self, order_link_id):
        """
        Отменяю открытый ордер лимитка/алго
        по кастомному идентификатору
        """
        r = self.cl.cancel_order(category=self.category, symbol=self.symbol, orderLinkId=order_link_id)
        self.log(r)

        return r

    def cancel_all_open_orders(self):
        """
        Отмена всех открытых ордеров (лимитки и алго)
        по секции + инструмент
        """
        r = self.cl.cancel_all_orders(category=self.category, symbol=self.symbol)
        print("* cancel_all_open_orders", r)

    def close_position(self):
        """
        Полное закрытие текущей позиции
        """
        args = dict(
            category=self.category,
            symbol=self.symbol,
            side=self.get_position("rev_side"),
            orderType="Market",
            qty=0.0,
            orderLinkId=f"FireflyX__{self.symbol}_{time.time()}",
            reduceOnly=True,
            closeOnTrigger=True,
        )
        self.log("args", args)

        r = self.cl.place_order(**args)
        self.log("result", r)

    def log(self, *args):
        """
        Для удобного вывода из методов класса
        """
        caller = inspect.stack()[1].function
        print(f"* {caller}", self.symbol, "\n\t", args, "\n")

    def _floor(self, value, decimals):
        """
        Для аргументов цены нужно отбросить (округлить вниз)
        до колва знаков заданных в фильтрах цены
        """
        factor = 1 / (10 ** decimals)
        return round((value // factor) * factor, decimals)

    def floor_price(self, value):
        return self._floor(value, self.price_decimals)

    def floor_qty(self, value):
        return self._floor(value, self.qty_decimals)

    def calculate_limit_price_perc(self, price, side : str="Sell", distance_perc : int=2):
        """
        Расчет цен для постановки лимитного/алго ордера
        в процентах от заданной цены
        и в зависимости от направления
        :param price: Цена инструмента
        :param side: Sell/Buy
        :param distance_perc: колво процентов, мб отрицательным
        :return:
        """
        return price * (100 + ((-1, 1)[side.lower() == "sell"] * distance_perc)) / 100