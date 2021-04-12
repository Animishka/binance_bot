import websocket, json, numpy
from talib import STOCH

from multiprocessing.dummy import Pool
from threading import Thread

from binance.client import Client
from binance.enums import *


#STOCH_1h задаём параметры для индикатора, таймфрейм 1час
FASTK_PERIOD_1h = 5
SLOWK_PERIOD_1h = 3
SLOWK_MATYPE_1h = 0
SLOWD_PERIOD_1h = 4
SLOWD_MATYPE_1h = 0

#STOCH_15m задаём параметры для индикатора, таймфрейм 15минут
FASTK_PERIOD_15m = 5
SLOWK_PERIOD_15m = 3
SLOWK_MATYPE_15m = 0
SLOWD_PERIOD_15m = 4
SLOWD_MATYPE_15m = 0

TRADE_SYMBOL = 'BTCUSDT'
TRADE_QUANTITY = 0.00025

#ключи из личного кабинета биржи
API_KEY = ""
API_SECRET = ""

"""определяем списки, в которые будут попадать данные из стрима биржи"""
openes_1h = []
highes_1h = []
lowes_1h = []
closes_1h = []

closes_15m = []
openes_15m = []
highes_15m = []
lowes_15m = []

"""чтобы не было постоянных попыток сделать ордер при выполнении определённых условий,
   введем переменную in_position"""
in_position = False

client = Client(API_KEY, API_SECRET, tld='com')

#задаём функцию, которая размещает ордер по рыночной цене
def order(side, quantity, symbol, order_type=ORDER_TYPE_MARKET):
    try:
        print("sending order")
        make_order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)
        print(make_order)
    except Exception as e:
        print("an exception occured - {}".format(e))
        return False

    return True


def on_open(ws):
    print('opened connection')


def on_close(ws):
    print('closed connection')


def on_message(ws, message):
    #указываем на глобальные переменные, в большей степени это необходимо для in_position
    global closes_15m, openes_15m, highes_15m, lowes_15m, closes_1h, openes_1h, highes_1h, lowes_1h, in_position
    #получаем данные из стрима биржи в формате json
    json_message = json.loads(message)
    candle = json_message['k']
    is_candle_closed = candle['x']
    close = candle['c']
    open = candle['o']
    high = candle['h']
    low = candle['l']
    interval = candle['i']
    #если свеча закрылась, то в зависимости от выбранного интервала, добавляем в конец списков значения цены
    if is_candle_closed:
        if interval == '15m':
            closes_15m.append(float(close))
            openes_15m.append(float(open))
            highes_15m.append(float(high))
            lowes_15m.append(float(low))
        if interval == '1h':
            closes_1h.append(float(close))
            openes_1h.append(float(open))
            highes_1h.append(float(high))
            lowes_1h.append(float(low))
    # если количества данных по закрытым свечам достаточно для запуска работы индикатора, то определяем массивы
    if len(closes_1h) > FASTK_PERIOD_1h:
        np_closes_1h = numpy.array(closes_1h)
        np_highes_1h = numpy.array(highes_1h)
        np_lowes_1h = numpy.array(lowes_1h)

        np_closes_15m = numpy.array(closes_15m)
        np_highes_15m = numpy.array(highes_15m)
        np_lowes_15m = numpy.array(lowes_15m)

        # применяем индикатор из библиотеки talib
        slowk_1h, slowd_1h = STOCH(np_highes_1h, np_lowes_1h, np_closes_1h, FASTK_PERIOD_1h, SLOWK_PERIOD_1h, SLOWK_MATYPE_1h,
                                   SLOWD_PERIOD_1h, SLOWD_MATYPE_1h)

        slowk_15m, slowd_15m = STOCH(np_highes_15m, np_lowes_15m, np_closes_15m, FASTK_PERIOD_15m, SLOWK_PERIOD_15m,
                                     SLOWK_MATYPE_15m, SLOWD_PERIOD_15m, SLOWD_MATYPE_15m)
        # используем последние значения из массива данных
        last_slowk_1h = slowk_1h[-1]
        last_slowd_1h = slowd_1h[-1]

        last_slowk_15m = slowk_15m[-1]
        last_slowd_15m = slowd_15m[-1]
        prelast_slowk_15m = slowk_15m[-2]
        prelast_slowd_15m = slowd_15m[-2]
        stkd_2 = float(prelast_slowk_15m) - float(prelast_slowd_15m)
        stkd_1 = float(last_slowk_15m) - float(last_slowd_15m)

        # определяем условия, при которых будет размещаться ордер на продажу
        if last_slowd_15m > 50 and stkd_2 > stkd_1 or last_slowk_15m <= last_slowd_15m:
            if in_position:
                print("Продаем!")
                order_succeeded = order(SIDE_SELL, TRADE_QUANTITY, TRADE_SYMBOL)
                if order_succeeded:
                    in_position = False
                    print("Продано!")
            else:
                print("Можно продавать, но еще в сделке.")

        # определяем условия, при которых будет размещаться ордер на покупку
        if last_slowk_1h > last_slowd_1h:
            if last_slowd_15m < 50 and prelast_slowd_15m < last_slowd_15m and last_slowk_15m >= last_slowd_15m:
                if in_position:
                    print("Можно покупать, но еще в сделке.")
                else:
                    print("Покупаем!")
                    order_succeeded = order(SIDE_BUY, TRADE_QUANTITY, TRADE_SYMBOL)
                    if order_succeeded:
                        in_position = True
                        print("Куплено!")

# определяем функцию, которая будет ежесекундно обрабатывать данные с биржи и размещать ордера на покупку/продажу
def run(socket):
    ws = websocket.WebSocketApp(socket, on_open=on_open, on_close=on_close, on_message=on_message)
    ws.run_forever()


""" два варианта выполнения кода для многопоточности, используя multiprocessing.dummy(для процессов) и threading
   Это позволяет обрабатывать сразу два потока с двумя объемами данных по таймфрейму 15 минут и 1 час """
def main():
    with Pool(5) as p:
        p.map(run, ["wss://stream.binance.com:9443/ws/btcusdt@kline_15m",
                   "wss://stream.binance.com:9443/ws/btcusdt@kline_1h"])

    """
    for socket in ["wss://stream.binance.com:9443/ws/btcusdt@kline_15m",
                   "wss://stream.binance.com:9443/ws/btcusdt@kline_1h"]:
        thread = Thread(target=run, args=(socket,))
        thread.start()
    """


if __name__== '__main__':
    main()