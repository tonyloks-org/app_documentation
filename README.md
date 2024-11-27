# Документация приложения

## Приложение "Поток"

### Цель
**Выгрузка данных из рекламных систем для автоматизации рутинных аналитических действий и доступа к продвинутым технологиям аналитики.**

---

### Основные преимущества

#### 1. **Безлимитная выгрузка из рекламных систем**
- Вы можете подключить любое количество рекламных аккаунтов и таблиц Google Sheets (GS).

#### 2. **Выгрузка данных из разных аккаунтов одной системы в единую таблицу**
- Повышает удобство при построении визуализаций в DataLens.
- Упрощает процесс аналитики и мониторинга аккаунтов.

#### 3. **Автоматическое создание баз данных и таблиц в СУБД**
- Все основные сущности для работы с СУБД создаются автоматически.
- Позволяет использовать технологии быстрой обработки данных без необходимости глубоких знаний о них.

---

### Принцип работы
- **Автономность**: Приложение работает в формате Enterprise, обозначающем, что оно создано для внутреннего использования. Я и никто другой не имеет доступа к данным внутри вашего приложения, в том числе логам и информации об ошибках.

---


---

### Используемые технологии
- **Микросервисы**: Python, Vue.js и Nginx.
- **Контейнеризация**: Хранение приложения в Docker.

---

### FAQ

#### ❓ Вопрос: **Как мне обновить приложение?**

✅ Ответ: Чтобы обновить приложение, выполните следующие действия:

1. Перейдите в папку с приложением и убедитесь, что в терминале путь отображается корректно:
    ```bash
    cd aoyad_app/
    ```
2. Остановите старое приложение:
    ```bash
    docker-compose down
    ```
3. Скачайте новые контейнеры:
    ```bash
    docker-compose pull
    ```
4. Соберите новое приложение:
    ```bash
    docker-compose build
    ```
5. Запустите приложение:
    ```bash
    docker-compose up -d
    ```
    *Флаг `-d` позволяет запустить приложение в фоновом режиме.*

---

#### ❓ Вопрос: **У меня отсутствуют данные за N-ый период, что делать?**

✅ Ответ: Чтобы устранить разницу между данными в рекламном аккаунте и базе, выполните следующие шаги:

1. Сделайте запрос в СУБД к вашей таблице, который сгруппирует данные по дате и, например, по кликам:
    ```sql
    SELECT date, SUM(clicks)
    FROM your_table
    GROUP BY date;
    ```
2. Убедитесь, что данные действительно отличаются.

3. Если пропуск в данных найден, возможные причины:
    - Сервер мог зависнуть в момент выгрузки (например, произошло отключение интернет-соединения).
    - В приложении есть баг.  
      В этом случае пришлите мне в личные сообщения в Telegram логи задачи. Их можно получить, перейдя в конкретную задачу и нажав кнопку «Скачать файл».
