import faust
import re
from datetime import datetime, timedelta


# Конфигурация Faust-приложения
app = faust.App(
    "simple-faust-app",
    broker="192.168.0.122:9092",
)


"""example
{"dst":"user3",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

"""

class UserInfo(faust.Record):
    dst: str
    msg: str
    timestamp: datetime

# Определение топика для входных данных
input_topic = app.topic("kafka-topic-in", key_type=str, value_type=UserInfo)

# Определение топика для выходных данных
output_topic = app.topic("kafka-topic-out", key_type=str, value_type=UserInfo)



class ForUserBlackList(faust.Record):
   for_user_acl: dict = {"user1":["user3","users4"],
                        "user2":["user3","user4"],
                        "user3":[],
                        "user4":[]}

class ForMessageBlackList(faust.Record):
   bad_words: list = ["fuck","asshole"]


class BLstats(faust.Record):
    user_src: str = ""
    user_dst: str = ""
    user_bad: bool = False
    msg_bad: dict = {}


bl_stats = app.Table(
   'stats_bl',
   default=BLstats,
   partitions=3,
   help='Статистика по блокировкам'
)


# how run  python.exe -m kafka-stream worker -l info
# Функция, реализующая потоковую обработку данных
@app.agent(input_topic)
async def process(stream):
    async for key,value in stream.items():
        # Обновляем общую статистику
        print(1)
        stats = bl_stats[key]
        if stats is None:
            stats = BLstats()
        print(2)
        stats.user_src=key
        stats.user_dst=value.dst
        print(stats)
        # Обработка данных users black-list
        users_bl=ForUserBlackList()
        msg_bl=ForMessageBlackList()
        
        #Find src user
        if key in list(users_bl.for_user_acl.keys()):
            stats.user_bad=True
            #Check dst user
            if value.dst in users_bl.for_user_acl[key]:
                await output_topic.send(value=f"Block src: {key} - dst:{value.dst}")
            #Replace bad word on ###
            old_msg=value.msg
            for i in msg_bl.bad_words:
                value.msg=re.sub(i,"###",value.msg)
            if old_msg != value.msg:
                stats.msg_bad={"old":old_msg,"new":value.msg}
            else:
                stats.msg_bad={"old":old_msg,"new":""}
            print(value)
            print(stats)
            bl_stats[key] = stats

            

if __name__ == '__main__':
    app.main()
