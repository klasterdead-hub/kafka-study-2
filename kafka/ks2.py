import init_conf

from app_faust import _Init_Faust
import re
import tables,topics
from models import (BL_USER,
                    BL_MSG)


app=_Init_Faust(init_conf.faust_app_name,init_conf.faust_app_broker).app

tb=tables._Init_Tables(app)
tp=topics._Init_Topics(app)

@app.agent(tp.input_topic)
async def process(stream):
    async for key,value in stream.items():

        
        # Обновляем общую статистику
        bl_user_info = tb.bl_user[f"{key}_{value.dst}"]
        if bl_user_info is None:
            bl_user_info = BL_USER()


        bl_msg_info = tb.bl_msg[f"{key}_{value.dst}"]
        if bl_msg_info is None:
            bl_msg_info = BL_MSG()


        bl_user_info.user_src=key
        bl_user_info.user_dst=value.dst

        bl_msg_info.user_src=key
        bl_msg_info.user_dst=value.dst

        #Check dst user
        if value.dst in bl_user_info.my_block:
            bl_user_info.user_bad=True
            await tp.bl_stats_topic.send(key=key,value=bl_user_info)

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
            await tp.filtered_topic.send(key=key,value=bl_msg_info)
        
        #ChangeLog
        tb.bl_user[f"{key}_{value.dst}"] = bl_user_info
        tb.bl_msg[f"{key}_{value.dst}"] = bl_msg_info




if __name__ == '__main__':
    app.main()