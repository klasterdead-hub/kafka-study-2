from models import (UserInfo,
                    BL_USER,
                    BL_MSG)




#Подключаем и связываем топики с базовыми моделями 

class _Init_Topics:
    def __init__(self,app):
        self.app=app
        self.input_topic = app.topic("messages", key_type=str, value_type=UserInfo)
        self.bl_stats_topic = app.topic('blocked_users',key_type=str, value_type=BL_USER)
        self.filtered_topic = app.topic("filtered_messages", key_type=str, value_type=BL_MSG)