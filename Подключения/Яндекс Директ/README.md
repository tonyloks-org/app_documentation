# Создание подключения к аккаунту Яндекс Директ

## Название подключения
**Название подключения** - Название, которое будет использоваться как техническое поле в ваших таблицах. 
На скриншоте вы можете увидеть поле ***AccountName***, которое отличается от логина аккаунта. 
Это есть поле названия подключения, которое будет использоваться в таблицах.

![пример](2024-11-27_15-35-09.png)

---
## Логин аккаунта
**Логин аккаунта** - Логин аккаунта, который будет использоваться для авторизации в Яндекс Директ без @yandex.ru.
Если вы работаете под управляющим/агентским аккаунтом - указывайте логин конечного аккаунта, откуда необходимо выгрузить статистику.


## API-токен доступа
**Токен доступа** - Токен доступа к аккаунту. Этот токен нужен для авторизации в Яндекс Директ.
Получить токен можно 2-мя способами:
- Получите токен через мое (любое другое) приложение API Яндекс Директа, просто переходите по [ссылке](https://oauth.yandex.ru/authorize?response_type=token&client_id=db0084b785964e89908f2b32e246f1de).
- Заведите свое приложение для выдачи токенов в разделе Инструменты - API - Мои заявки - Новая заявка. 
![раздел с заявкой на новое приложение](2024-11-27_15-41-30.png)
Укажите цель приложения и подождите около 2-х суток модерацию. 
Часто модерация отказывает в принятии приложения, поэтому старайтесь описать его как можно подробнее.


Если вы работаете под управляющим/агентским аккаунтом - укажите токен управляющего/агентского аккаунта из под которого вы имеете доступ к конечному.

## ID целей
**ID целей** - список ID целей, по которым вам нужна информация в статистике. 

Список всех доступных целей можно получить в разделе "Цели" в Яндекс Метрике:
![раздел цели в Яндекс Метрике](2024-11-27_15-47-47.png)

Вам нужно только идентификатор такого типа: ***123321312***

***Внимание!*** 
Вы можете добавить максимум 10 целей в одном подключении. Это связано с ограничениями API Яндекс Директа.