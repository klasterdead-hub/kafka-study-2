import init_conf
from app_faust import _Init_Faust
import re
import tables,topics
from models import (BL_USER,
                    BL_MSG)

#Init faust stream
app=_Init_Faust(init_conf.faust_app_name,init_conf.faust_app_broker).app

#Add-init topics and tables
tb=tables._Init_Tables(app)
tp=topics._Init_Topics(app)

@app.agent(tp.input_topic)
async def process(stream):
    async for key,value in stream.items():

        # Обновляем общую статистику
        bl_user_info = tb.bl_user[key]
        if bl_user_info is None:
            bl_user_info = BL_USER()


        bl_msg_info = tb.bl_msg[key]
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
            await tp.bl_stats_topic.send(key=key,value=bl_user_info)

        #Replace bad word on ###
        old_msg=value.msg
        for i in bl_msg_info.my_bad_words:
            tb.bl_msg[key] = bl_msg_info
            value.msg=re.sub(i,"###",value.msg)
            
        #Check if changes MSG
        if old_msg != value.msg:
            bl_msg_info.user_msg={"old":old_msg,"new":value.msg}
        else:
            bl_msg_info.user_msg={"old":old_msg,"new":""}
        
        #Send msg when user not blocked
        if bl_user_info.user_bad == False:
            tb.bl_user[key] = bl_user_info
            await tp.filtered_topic.send(key=key,value=bl_msg_info)
            
        
        #ChangeLog
        #tb.bl_user[key] = bl_user_info
        #tb.bl_msg[key] = bl_msg_info


@app.page('/add_user_bl/{user_id}/{user_block}')
@app.table_route(table=tb.bl_user, match_info='user_id')
async def add_user_bl(web, request, user_id,user_block):
   try:
        #Проверка и формировка данных для ключа
        if user_id in tb.bl_user:
            bl_user_info=tb.bl_user[user_id]
        else:
            bl_user_info = BL_USER()
            bl_user_info.my_block=[]

        #Есть ли пользователь, которого хотим заблокировать
        if user_block not in bl_user_info.my_block:
            bl_user_info.my_block.append(user_block)

        #ChangeLog in Broker
        tb.bl_user[user_id] = bl_user_info
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
        #Проверка и формировка данных для ключа
        if user_id in tb.bl_user:
            bl_user_info=tb.bl_user[user_id]
        else:
            bl_user_info = BL_USER()
            bl_user_info.my_block=[]

        #Есть ли пользователь, которого хотим разблокировать
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
        #Проверка и формировка данных для ключа
        if user_id in tb.bl_msg:
            bl_msg_info=tb.bl_msg[user_id]
        else:
            bl_msg_info = BL_MSG()
            bl_msg_info.my_bad_words=[]  
        
        #Есть ли слово, которое хотим заблокировать
        if word not in bl_msg_info.my_bad_words:
           bl_msg_info.my_bad_words.append(word)
        tb.bl_msg[user_id]=bl_msg_info
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
        #Проверка и формировка данных для ключа
        if user_id in tb.bl_msg:
            bl_msg_info=tb.bl_msg[user_id]
        else:
            bl_msg_info = BL_MSG()
            bl_msg_info.my_bad_words=[]  

        #Есть ли слово, которое хотим разблокировать
        if word in bl_msg_info.my_bad_words:
            pos=bl_msg_info.my_bad_words.index(word)
            del bl_msg_info.my_bad_words[pos]
        tb.bl_msg[user_id]=bl_msg_info
        return web.json({
           'bad_word_list': bl_msg_info.my_bad_words
       }, status=200)
   except Exception as e:
       return web.json({
           'error': str(e)
       }, status=500)

if __name__ == '__main__':
    app.main()