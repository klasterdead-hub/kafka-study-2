# kafka-study-2


[INFO]

Файлы:
* init_conf.py          - настройки для запуска приложения
* models.py             - хранение базовых моделей     
* tables.py             - хранение таблиц
* topics.py             - подключение к топикам
* app_faust.py          - инициализация faust-streaming
* start_faust.py        - запуск приложения, обработка и изменение блокировочных листов по пользователям и по сообщениям


init_conf.py
Основные настройки для запуска start_faust.py


app_faust.py
class _Init_Faust   - инициализация FaustStreaming


models.py
class UserInfo      - модель для входящих сообщений
class BL_USER       - модель для заблокированных пользователей
class BL_MSG        - модель для заблокированных слов


tables.py
class _Init_Tables  - инициализация двух таблиц bl_user и bl_msg, которые в свою очередь опираются по дефолту на модели (models.py) BL_USER и BL_MSG


topics.py
class _Init_Topics  - инициализация 3-х топиков (messages,blocked_users,filtered_messages), а топики value_type берут из models.py


start_faust.py
Сначала инициализирует запуск faust-streaming, потом "подключает" таблицы и топики
Есть следующие функции:
* async def process         - 1)принимает поток данных из топика messages
                            - 2)выполняет проверку на заблокированность пользователя назначения + отправки в топик blocked_users 
                            - 3)если пользователь назначения не заблокирован, то редактирует-проверяет сообщения на "плохие" слова и заменяет эти слова на ### и отправляет в топик filtered_messages

* async def add_user_bl    - добавляет в список блокировок по пользователям, опираясь и взаимодейуствуя с таблицей bl_user

* async def del_user_bl    - удаляет пользователя из списка блокировки, опираясь и взаимодейуствуя с таблицей bl_user

* async def add_bad_word   - добавление плохих слов на замену-редактуру для пользователя, опираясь и взаимодейуствуя с таблицей bl_msg

* async def del_bad_word   - удаление плохих слов на замену-редактуру для пользователя, опираясь и взаимодейуствуя с таблицей bl_msg




[HOW RUN]

1) Создать 3 топика (создал через kafka-ui messages,blocked_users,filtered_messages)
2) Выполнить команды из how_build.sh
3) Поднимется приложение faust
4) Для приложения faust посмотреть логи docker logs -f


[CHECK]

___Проверка на слова___

1. Добавление на замену слов

Через графический интерфейс kafka-ui
key = user1
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

Добавляем плохое слово asshole для пользователя user1 (если key в kafka-ui равен user1), а для user2 - ass

http://192.168.0.122:6066/add_bad_word/user1/asshole
http://192.168.0.122:6066/add_bad_word/user2/ass


Теперь при формировании следующих сообщений из топика messages:
-
key = user1
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}
-
key = user2
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

В топике filtered_messages увидим следующий результат:

Для 1
{
	"user_src": "user1",
	"user_dst": "user1",
	"user_msg": {
		"old": "ABD123asshole",
		"new": "ABD123###"
	},
	"my_bad_words": [
		"asshole"
	],
	"__faust": {
		"ns": "models.BL_MSG"
	}
}

Для 2
{
	"user_src": "user2",
	"user_dst": "user1",
	"user_msg": {
		"old": "ABD123asshole",
		"new": "ABD123###hole"
	},
	"my_bad_words": [
		"ass"
	],
	"__faust": {
		"ns": "models.BL_MSG"
	}
}


2. Удаление слов из списка на замену

Теперь удалим эти слова 

http://192.168.0.122:6066/del_bad_word/user1/asshole
http://192.168.0.122:6066/del_bad_word/user2/ass


Теперь при формировании следующих сообщений из топика messages:
-
key = user1
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}
-
key = user2
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

В топике filtered_messages увидим следующий результат:

Для 1
{
	"user_src": "user1",
	"user_dst": "user1",
	"user_msg": {
		"old": "ABD123asshole",
		"new": ""
	},
	"my_bad_words": [],
	"__faust": {
		"ns": "models.BL_MSG"
	}
}

Для 2
{
	"user_src": "user2",
	"user_dst": "user1",
	"user_msg": {
		"old": "ABD123asshole",
		"new": ""
	},
	"my_bad_words": [],
	"__faust": {
		"ns": "models.BL_MSG"
	}
}





___Проверка на блокировки___

1. Добавление на блокировку пользователей
Блокируем user3 для пользователя user1 (если key в kafka-ui равен user1), а для user2 блокируем user1

http://192.168.0.122:6066/add_user_bl/user1/user3

http://192.168.0.122:6066/add_user_bl/user2/user1


Теперь при формировании следующих сообщений из топика messages:
-
key = user1
Value = {"dst":"user3",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}
-
key = user2
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

В топике blocked_users (в топике filtered_messages сообщений не будет так как отправка сообщений пользователю в поле dst заблокирована) увидим следующий результат:
Для 1
{
	"user_src": "user1",
	"user_dst": "user3",
	"user_bad": true,
	"my_block": [
		"user3"
	],
	"__faust": {
		"ns": "models.BL_USER"
	}
}

Для 2

{
	"user_src": "user2",
	"user_dst": "user1",
	"user_bad": true,
	"my_block": [
		"user1"
	],
	"__faust": {
		"ns": "models.BL_USER"
	}
}


2. Удаление пользователей из листа блокировки

Разблокируем user3 для пользователя user1 (если key в kafka-ui равен user1), а для user2 разблокируем user1

http://192.168.0.122:6066/del_user_bl/user1/user3

http://192.168.0.122:6066/del_user_bl/user2/user1


Теперь при формировании следующих сообщений из топика messages:
-
key = user1
Value = {"dst":"user3",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}
-
key = user2
Value = {"dst":"user1",
"msg":"ABD123asshole",
"timestamp":"2026-05-02T16:00:00.123456"}

В топике filtered_messages увидим следующий результат:

Для 1
{
	"user_src": "user1",
	"user_dst": "user3",
	"user_msg": {
		"old": "ABD123asshole",
		"new": ""
	},
	"my_bad_words": [],
	"__faust": {
		"ns": "models.BL_MSG"
	}
}

Для 2
{
	"user_src": "user2",
	"user_dst": "user1",
	"user_msg": {
		"old": "ABD123asshole",
		"new": ""
	},
	"my_bad_words": [],
	"__faust": {
		"ns": "models.BL_MSG"
	}
}



P.S.

IP адреса оставил, так как на домашнем макете бардак с DNS + NGINX + IPtables, а разрулить это в кратчайшие сроки не успел