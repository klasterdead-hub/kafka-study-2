
import faust


# Конфигурация Faust-приложения


class _Init_Faust:
    def __init__(self,name:str,brokers:str):
        self.app = faust.App(
            name,
            broker=brokers,
        )