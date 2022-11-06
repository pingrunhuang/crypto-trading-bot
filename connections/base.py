from abc import abstractmethod

class ABCConnection:
    def __init__(self, root_url:str):
        self.root_url = root_url
    
    @abstractmethod
    def ohlcv(self, end_point:str):
        pass



