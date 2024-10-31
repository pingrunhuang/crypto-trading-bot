# crypto-trading-bot



## setup

```
python -m pip install venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

once setup virtual environment, we can run follows:
`python main.py --funcname upsert_symbols` this is update all the symbols for all exchanges specified in the config file

`python main.py --funcname grab_klines`

## design 
the `connections` module is where I store all the connections to different exchanges
I define a base connection class which has all the common functions that one connection is supposed to have.
For example, 
each connection will have a symbol manager which allows it to query all the symbols' info into memory
each connection should have a union way to write data into databases

### different functionalities
- bar data grabbing
- trades grabbing
- securities data grabbing
- minnor operations
each functionality will have one corresponding config file to that



## how does it work?
define the configs file as shown in the example configurations



## matrix of measuring a strategy
- sharpe ratio
- win rate
- number of trades

## configurations of strategies
- win lost ratio



2 issues need to be solved 
1. make sure async running code work
2. make sure websocket client code work