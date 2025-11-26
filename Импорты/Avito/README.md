# Импорты Avito

Поддерживаемые выгрузки:

1. **Статистика объявлений (ads_stats, stats/v2)**

   - Метрики: показы, просмотры, контакты, конверсии, расходы, заказы доставки и др.
   - Группировки: `по объявлениям`
   - Параметры по умолчанию: `metrics` — полный набор, `grouping` — `item`, `limit` — 1000, `offset` — 0.
2. **Баланс и CPA-аванс (account_balance)**

   - Поля: кошелек (real/bonus/total), `cpaAdvance`, `cpaBalance`, `cpaDebt`, `balanceCheckedAt`, `userId`.
3. **Контент объявлений (item_content)**

   - Список объявлений аккаунта и детальная информация по каждому.
   - Статусы по умолчанию: `active,old,removed,blocked,rejected`
