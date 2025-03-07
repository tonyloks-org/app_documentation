# Создание подключения к таблице Google Sheets


## Название подключения
**Название подключения** - Название, которое будет использоваться как техническое поле в ваших таблицах. 
На скриншоте вы можете увидеть поле ***AccountName***, которое отличается от логина аккаунта. 
Это есть поле названия подключения, которое будет использоваться в таблицах.

![пример](../../Подключения/Яндекс Метрика/2024-11-27_15-35-09.png)

---
## Ссылка на таблицу
**Ссылка на таблицу** - Ссылка на таблицу Google Sheets, которую вы хотите использовать в качестве источника данных.

Пример ссылки: https://docs.google.com/spreadsheets/d/1v0mbywWGhiu1fbYASsdlwiewjii3IMA/edit?usp=sharing

**Важно!**

Убедитесь, что у вас выставлены права на чтение данных в таблице "Все у кого есть ссылка".


# Типы данных полей

**Типы данных полей** – это настройки, определяющие, как данные из таблицы Google Sheets будут интерпретированы и обработаны при выгрузке. Эти типы задаются один раз при создании подключения.  
**Важно:** Если в таблице Google Sheets изменились поля, необходимо обновить подключение, нажав кнопку **"Обновить подключение"**.

---

## Доступные типы данных

- **Строка**  
  Используется для текстовых значений и параметров.  
  *Примеры:* Название кампании, Сотрудник


- **Число**  
  Применяется для целочисленных значений без остатка.  
  *Примеры:* Показы, Клики, Конверсии


- **Число с плавающей точкой**  
  Предназначен для числовых значений с дробной частью.  
  *Примеры:* Расход, Цена, Прибыль


- **Дата**  
  Используется для представления дат.  
  *Примеры:* Дата начала, Дата окончания


- **Дата и время**  
  Применяется для полей, содержащих как дату, так и время.  
  *Примеры:* Дата начала, Дата окончания, Время начала, Время окончания


- **Большое число**  
  Используется для хранения идентификаторов (ID).  
  *Примеры:* ID кампании, ID сотрудника


- **Булево значение**  
  Применяется для логических значений (true/false).  
  *Примеры:* Новый юзер, Активен, Онлайн
