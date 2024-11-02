import websocket
from loggers import LOGGER
import json

class BaseWebsocket:

    URL = ""
    EXCHANGE = ""
    CHANNELS = []

    def __init__(
        self,
        **kwargs
    ) -> None:
        websocket.enableTrace(True)
        channels = kwargs.pop("channels")
        assert channels is not None
        self.CHANNELS = channels
        exchange = kwargs.pop("exchange")
        assert exchange is not None
        self.EXCHANGE = exchange
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.ws_client = websocket.WebSocketApp(
            self.URL,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
            on_ping=self.on_ping,
            on_pong=self.on_pong
        )

    def on_message(self, ws, msg):
        raise NotImplementedError(msg)
    
    def on_error(self, ws, msg):
        LOGGER.error(msg)

    def on_open(self, ws):
        """
        subscribe to certain channel when open socket
        """
        LOGGER.info(f"Subscribing all data feed...")
        for channel in self.CHANNELS:
            self.subscribe(channel)
    
    def on_close(self, ws, close_status_code, close_msg):
        LOGGER.info(f">>>>>>Closing socket {close_status_code}:{close_msg}...")
        self.ws_client.close()

    def on_ping(self, ws, msg):
        LOGGER.warning(f"ping {msg}")

    def on_pong(self, ws, msg):
        LOGGER.warning(f"pong {msg}")
    
    def get_request_params(self, t):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()
    
    def subscribe(self, channel):
        params = self.get_request_params(channel)
        LOGGER.info(f"Sending {params}")
        self.ws_client.send(params)

class BaseBot:
    def __init__(self, *args, **kwargs) -> None:
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def generate_signal(self, *args, **kwargs)->bool:
        pass
    
    def place_order(self, *args, **kwargs):
        pass

