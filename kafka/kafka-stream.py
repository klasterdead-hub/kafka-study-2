import faust
import re
from datetime import datetime
from models import (BL_MSG,
                    BL_USER,
                    UserInfo)



"""example
{"dst":"user3",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

"""




# Конфигурация Faust-приложения
app = faust.App(
    "simple-faust-app",
    broker="192.168.0.122:9092",
)


# Определение топика для входных данных
input_topic = app.topic("messages", key_type=str, value_type=UserInfo)
bl_stats_topic = app.topic('blocked_users',key_type=str, value_type=BL_USER)
filtered_topic = app.topic("filtered_messages", key_type=str, value_type=BL_MSG)

bl_user = app.Table(
   'blocked_users',
   default=BL_USER,
   partitions=3,
   help='Таблица заблокированных пользователей'
)

bl_msg = app.Table(
   'filtered_messages',
   default=BL_MSG,
   partitions=3,
   help='Таблица блокировок сообщений'
)


# how run  python.exe -m kafka-stream worker -l info
# Функция, реализующая потоковую обработку данных
@app.agent(input_topic)
async def process(stream):
    async for key,value in stream.items():

        
        # Обновляем общую статистику
        bl_user_info = bl_user[key]
        if bl_user_info is None:
            bl_user_info = BL_USER()


        bl_msg_info = bl_msg[key]
        if bl_msg_info is None:
            bl_msg_info = BL_MSG()


        bl_user_info.user_src=key
        bl_user_info.user_dst=value.dst
        bl_user_info.user_bad = False
        
        bl_msg_info.user_src=key
        bl_msg_info.user_dst=value.dst

        #Check dst user
        if value.dst in bl_user_info.my_block:
            bl_user_info.user_bad=True
            await bl_stats_topic.send(key=key,value=bl_user_info)

        #Replace bad word on ###
        old_msg=value.msg
        for i in bl_msg_info.my_bad_words:
            value.msg=re.sub(i,"###",value.msg)
        #Check if changes
        if old_msg != value.msg:
            bl_msg_info.user_msg={"old":old_msg,"new":value.msg}
        else:
            bl_msg_info.user_msg={"old":old_msg,"new":""}
        
        #Send msg when user not blocked
        if bl_user_info.user_bad == False:
            await filtered_topic.send(key=key,value=bl_msg_info)
        
        #ChangeLog
        bl_user[key] = bl_user_info
        bl_msg[key] = bl_msg_info

@app.page('/add_user_bl/{user_id}/{user_block}')
async def add_user_bl(web, request, user_id,user_block):
   try:
        bl_user_info=bl_user[user_id]
        if bl_user_info is None:
            bl_user_info = BL_USER()
        if user_block not in bl_user_info.my_block:
            bl_user_info.my_block.append(user_block)
        return web.json({
           'bl_list': bl_user_info.my_block,
           'user_id': user_id
       }, status=200)
   except Exception as e:
       return web.json({
           'error': str(e),
           'user_id': user_id
       }, status=500)

@app.page('/del_user_bl/{user_id}/{user_block}')
async def del_user_bl(web, request, user_id,user_block):
   try:
        
        bl_user_info=bl_user[user_id]
        if bl_user_info is None:
           bl_user_info = BL_USER()

        if user_block in bl_user_info.my_block:
            pos=bl_user_info.my_block.index(user_block)
            del bl_user_info.my_block[pos]
        return web.json({
           'bl_list': bl_user_info.my_block,
           'user_id': user_id
       }, status=200)
   except Exception as e:
       return web.json({
           'error': str(e),
           'user_id': user_id
       }, status=500)



@app.page('/add_bad_word/{user_id}/{word}')
async def add_bad_word(web,request, user_id, word):
   try:       
        bl_msg_info = bl_msg[user_id]
        if bl_msg_info is None:
            print(1)
            bl_msg_info = BL_MSG()

        if word not in bl_msg_info.my_bad_words:
           bl_msg_info.my_bad_words.append(word)
        return web.json({
           'bad_word_list': bl_msg_info.my_bad_words,
           "user_id":user_id,
       }, status=200)
   except Exception as e:
       return web.json({
           'error': str(e)
       }, status=500)

@app.page('/del_bad_word/{user_id}/{word}')
async def del_bad_word(web,request,user_id, word):
   try:
        bl_msg_info = bl_msg[user_id]
        if bl_msg_info is None:
            bl_msg_info = BL_MSG()

        if word in bl_msg_info.my_bad_words:
            pos=bl_msg_info.my_bad_words.index(word)
            del bl_msg_info.my_bad_words[pos]
        return web.json({
           'bad_word_list': bl_msg_info.my_bad_words
       }, status=200)
   except Exception as e:
       return web.json({
           'error': str(e)
       }, status=500)




if __name__ == '__main__':
    app.main()
