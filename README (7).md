# Bybit Order Flow Bot (Python)

24/7 бот на Python для Bybit фьючерсов. Только PostOnly chase (минимальная комиссия мейкера).

## Возможности

- Сканирует топ монет по объёму или волатильности (или свой список)
- Сигнал по стакану + ленте сделок (order flow score)
- Входы и выходы только через быстрые PostOnly лимитки (chase)
- Take-profit, stop-loss, максимальное время в сделке, портфельный стоп
- Режим «только шорт» и фильтр отката для шортов
- Простой веб-интерфейс для мониторинга и старта/стопа

## Переменные окружения (Railway / VPS)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `BYBIT_API_KEY` | — | API ключ (обязательно) |
| `BYBIT_API_SECRET` | — | API секрет (обязательно) |
| `BYBIT_ENV` | `testnet` | `testnet` или `mainnet` |
| `DEPOSIT` | `100` | Депозит USDT |
| `LEVERAGE` | `3` | Плечо |
| `MAX_TRADES` | `3` | Макс. одновременных сделок |
| `PORTFOLIO_STOP_PCT` | `15` | Стоп по просадке депозита % |
| `TP_PCT` | `0.9` | Take profit % |
| `SL_PCT` | `10` | Stop loss % |
| `MAX_HOLD_MIN` | `20` | Макс. время в сделке, мин |
| `UNIVERSE_MODE` | `volatile` | `auto` / `volatile` / `custom` |
| `TOP_N` | `10` | Сколько монет сканировать |
| `MIN_TURNOVER` | `5000000` | Мин. оборот 24ч USDT |
| `EXCLUDE_MAJORS` | `1` | Исключить BTC/ETH (1/0) |
| `CUSTOM_SYMBOLS` | — | Свой список через запятую |
| `ENTRY_THRESHOLD` | `0.35` | Порог входа по score |
| `EXIT_THRESHOLD` | `0.45` | Порог выхода по развороту |
| `TAPE_WINDOW_SEC` | `60` | Окно ленты, сек |
| `OB_DEPTH` | `20` | Глубина стакана |
| `OB_WEIGHT` | `0.5` | Вес стакана (0–1) |
| `SHORT_ONLY` | `0` | Только шорт (1/0) |
| `CHASE_MAX_ATTEMPTS` | `8` | Попыток chase |
| `CHASE_INTERVAL_MS` | `1500` | Интервал перевыставления, мс |
| `REQUIRE_PULLBACK` | `1` | Шорт только после отката |
| `PULLBACK_PCT` | `0.15` | Размер отката % |
| `PULLBACK_WINDOW_SEC` | `30` | Окно отката, сек |
| `PORT` | `8080` | Порт веб-интерфейса |

## Запуск локально

```bash
pip install -r requirements.txt
export BYBIT_API_KEY=...
export BYBIT_API_SECRET=...
export BYBIT_ENV=testnet
python bot.py
```

Открой http://localhost:8080

## Railway

1. Создай новый проект из этого репозитория.
2. В Variables добавь `BYBIT_API_KEY`, `BYBIT_API_SECRET` и остальные по желанию.
3. Railway сам определит Python и запустит `python bot.py` (или укажи start command).
4. Веб-интерфейс будет доступен по домену Railway.

**Важно:** начинай с Testnet. На Mainnet бот торгует реальными деньгами.
"""