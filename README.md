# kafka-study-2


[INFO]

Файлы:

























































[Check]

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
