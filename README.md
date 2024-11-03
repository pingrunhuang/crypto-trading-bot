# crypto-trading-bot

This is a module used for receiving market data using websocket and based on signal to generate trading signal in realtime. 

## setup
```
python -m pip install venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
1. clone another repository `trading-meta` 
2. save the path of the previous step and add it into `python.analysis.extraPaths` in the .vscode/settings.json file.

once setup virtual environment, we can run follows:
`python main.py --config_file grid_trading.yaml`
you can supply the config_file along with correspondong bot implementation.

## design 
the `bots` module is where I store all the connections to different exchanges
each bot will manage 2 threads. One is used for receiving market data called `socket`. The other is used for checking condition to generate trading signal. Data transformation between this 2 threads is now using queue. Diagram shown below:
![diagram](https://github.com/pingrunhuang/crypto-trading-bot/blob/master/diagram.png)
I define one base class for each `socket` which integrate all common functionalities that I can imagine that each `socket` should have.


## how does it work?
define the configs file as shown in the example configurations


## matrix of measuring a strategy
- sharpe ratio
- win rate
- number of trades

## configurations of strategies
- win lost ratio

## Ideas to improve
for scaling purpose and separating services, can use a redis queue to pass through market data to feed into our bot