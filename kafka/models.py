import faust
from datetime import datetime



# Базовые модели данных

class UserInfo(faust.Record):
    dst: str
    msg: str
    timestamp: datetime

class BL_USER(faust.Record):
    user_src: str = ""
    user_dst: str = ""
    user_bad: bool = False
    my_block: dict = []

class BL_MSG(faust.Record):
    user_src: str = ""
    user_dst: str = ""
    user_msg: dict = {}
    my_bad_words: list = []