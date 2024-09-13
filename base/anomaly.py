from abc import ABC, abstractmethod

import polars as pl


class Anomaly(ABC):
    @abstractmethod
    def calculate(self):
        pass
