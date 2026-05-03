from models import (BL_USER,
                    BL_MSG)




#Подключаем и связываем таблицы с базовыми моделями

class _Init_Tables:
    def __init__(self,app):
        self.app=app
        
        self.bl_user = self.app.Table(
        'blocked_users',
        default=BL_USER,
        partitions=3,
        help='Таблица заблокированных пользователей',
        use_partitioner=True
        )

        self.bl_msg = self.app.Table(
        'filtered_messages',
        default=BL_MSG,
        partitions=3,
        help='Таблица блокировок сообщений',
        use_partitioner=True
        )