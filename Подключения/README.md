# Подключения

## Что такое подключение?

Подключение — это настройка доступа приложения к стороннему сервису для получения или передачи данных.  
Оно позволяет интегрировать данные из различных источников в единое пространство для анализа и отчетности.

## Доступные подключения

На данный момент приложение поддерживает следующие подключения:

1. **Яндекс Директ** — API для доступа к данным рекламных кампаний.
2. **Яндекс Метрика** — система аналитики для мониторинга данных о посетителях сайта.
3. **Google Sheets** — работа с таблицами Google для выгрузки и загрузки данных.

---

## Настройка подключения для каждого источника

### 1. Яндекс Директ

Для подключения к API Яндекс Директа выполните следующие шаги:

1. **Введите название подключения**  
   Укажите уникальное название, например, «Реклама Директ Аккаунт 1».

2. **Добавьте логин и токен**  
   - Логин — ваш логин в Яндекс Директе.  
   - Токен — ключ доступа к API.  
     - Получить токен можно через [страницу авторизации Яндекс](https://oauth.yandex.ru).  

3. **Сохраните настройки**  
   - Нажмите «Подключить».  
   - Приложение проверит токен и данные.  
   - При успешной проверке подключение будет добавлено в список.

### 2. Яндекс Метрика

Для подключения к Яндекс Метрике:

1. **Введите название подключения**  
   Например, «Метрика Аналитика».

2. **Добавьте токен доступа**  
   - Токен можно получить через API Яндекс Метрики на [странице OAuth авторизации](https://oauth.yandex.ru).  

3. **Сохраните настройки**  
   - Приложение проверит данные и добавит подключение.

### 3. Google Sheets

Для подключения к Google Sheets:

1. **Введите название подключения**  
   Например, «Таблицы для аналитики».

2. **Добавьте учетные данные**  
   - Необходимо предоставить Google OAuth токен для доступа к таблицам.

3. **Сохраните настройки**  
   - После успешной валидации подключение будет добавлено.

---

## Советы по настройке

- **Корректные токены**: Убедитесь, что токены актуальны и выданные для нужных сервисов.  
- **Уникальные названия**: Используйте удобные и описательные названия для каждого подключения.  
- **Техническая документация**: Ознакомьтесь с документацией API каждого источника для более подробной настройки.
