bitmex_orderbook collector

How to use: git clone https://github.com/damonchen6886/bitmex_orderbook.git, then run

A sample Bitmex BTC/USD orderbook collector that will save and update data to Redis and MongoDB as well as display real time BTC/USD order book on terminal.

eg:
           price  |  size
----------------------------------------
ask 5 :    57744  |  803
ask 4 :  57740.5  |  4000
ask 3 :    57740  |  2341
ask 2 :    57738  |  65
ask 1 :    57737  |  136019
****************************************
bid 1 :  57736.5  |  3370530
bid 2 :    57736  |  135715
bid 3 :  57735.5  |  153419
bid 4 :  57733.5  |  93068
bid 5 :  57729.5  |  75000


Additional feature: save real time data to the redis and MongoDB and consistently update.
Simply paste your cloud dabaseinfo and run
