import logging
import time
from typing import List, Dict, Any, Optional

import requests

logger = logging.getLogger(__name__)

# Базовый URL: для mainnet именно такой в доках Lighter
# https://mainnet.zklighter.elliot.ai/api/v1/funding-rates
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ENDPOINT_PATH = "/api/v1/funding-rates"


class LighterFundingError(Exception):
    """Ошибка при запросе или разборе фандингов Lighter."""


def fetch_lighter_funding_raw(
    base_url: str = BASE_URL,
    timeout: int = 10,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Any:
    """
    Делает запрос к эндпоинту /funding-rates и возвращает сырой JSON.

    base_url        – база API (по умолчанию mainnet).
    timeout         – таймаут HTTP-запроса.
    extra_headers   – доп. заголовки, если вдруг понадобится авторизация.

    Пример, если когда-нибудь решат требовать токен:
        raw = fetch_lighter_funding_raw(
            extra_headers={"Authorization": f"Bearer {TOKEN}"}
        )
    """
    url = f"{base_url.rstrip('/')}{ENDPOINT_PATH}"
    headers = {"Accept": "application/json"}

    if extra_headers:
        headers.update(extra_headers)

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.exception("Ошибка HTTP при запросе Lighter funding-rates: %s", e)
        raise LighterFundingError(f"HTTP error: {e}") from e

    try:
        data = resp.json()
    except ValueError as e:
        logger.exception("Не удалось распарсить JSON от Lighter: %s", e)
        raise LighterFundingError(f"Invalid JSON: {e}") from e

    return data


def _extract_items_from_response(data: Any) -> List[Dict[str, Any]]:
    """
    Пробуем аккуратно вытащить список объектов фандинга из разных
    возможных форматов ответа (list, dict с ключом data, и т.д.).
    """
    # 1. Если сразу список
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    items: Optional[List[Dict[str, Any]]] = None

    # 2. Если dict – ищем типичные ключи с массивами
    if isinstance(data, dict):
        for key in ("fundingRates", "fundings", "data", "result", "items"):
            value = data.get(key)
            if isinstance(value, list):
                items = [x for x in value if isinstance(x, dict)]
                break

        # 3. Вариант: dict(symbol -> объект)
        if items is None:
            if data and all(isinstance(v, dict) for v in data.values()):
                tmp: List[Dict[str, Any]] = []
                for symbol, obj in data.items():
                    obj = dict(obj)  # копия
                    obj.setdefault("symbol", symbol)
                    tmp.append(obj)
                if tmp:
                    items = tmp

    if items is None:
        # кидаем понятную ошибку, чтобы в логах видеть реальный ответ
        snippet = repr(data)
        if len(snippet) > 800:
            snippet = snippet[:800] + "...(truncated)"
        raise LighterFundingError(
            f"Неожиданная структура ответа Lighter /funding-rates: {snippet}"
        )

    return items


def normalize_lighter_funding(data: Any) -> List[Dict[str, Any]]:
    """
    Нормализует ответ Lighter к списку записей единого вида:

    {
        "exchange": "lighter",
        "symbol": str,            # тикер / market id
        "fundingRate": float,     # ставка за период (доля, не %)
        "fundingRateAnnual": float | None,  # оценка APR, если можем посчитать
        "timestamp": int,         # ms
        "raw": dict               # исходный объект из API
    }

    Важно: структура ответа Lighter может обновляться,
    поэтому парсер написан максимально "гибко".
    """
    items = _extract_items_from_response(data)
    now_ms = int(time.time() * 1000)

    result: List[Dict[str, Any]] = []

    for raw in items:
        if not isinstance(raw, dict):
            continue

        # --- symbol / market ---
        symbol = (
            raw.get("symbol")
            or raw.get("market")
            or raw.get("marketId")
            or raw.get("market_id")
            or raw.get("ticker")
        )

        # --- funding rate ---
        # пытаемся найти поле со ставкой
        if "fundingRate" in raw:
            fr = raw.get("fundingRate")
        elif "funding_rate" in raw:
            fr = raw.get("funding_rate")
        elif "rate" in raw:
            fr = raw.get("rate")
        else:
            fr = None

        try:
            funding_rate = float(fr) if fr is not None else None
        except (TypeError, ValueError):
            funding_rate = None

        # --- timestamp ---
        ts = (
            raw.get("timestamp")
            or raw.get("ts")
            or raw.get("time")
            or raw.get("blockTimestamp")
            or raw.get("block_timestamp")
        )

        try:
            timestamp_ms = int(ts) if ts is not None else now_ms
        except (TypeError, ValueError):
            timestamp_ms = now_ms

        if symbol is None or funding_rate is None:
            # если не можем вытащить базовые поля — просто пропускаем строку
            continue

        # По материалам про perp DEX вроде Lighter, фандинг у них часовой. :contentReference[oaicite:1]{index=1}
        # Если это действительно ставка за 1 час, можно оценить годовой APR:
        # APR ≈ funding_rate_per_hour * 24 * 365.
        funding_rate_annual: Optional[float]
        try:
            funding_rate_annual = funding_rate * 24 * 365
        except TypeError:
            funding_rate_annual = None

        result.append(
            {
                "exchange": "lighter",
                "symbol": str(symbol),
                "fundingRate": funding_rate,
                "fundingRateAnnual": funding_rate_annual,
                "timestamp": timestamp_ms,
                "raw": raw,
            }
        )

    return result


def fetch_lighter_funding(
    base_url: str = BASE_URL,
    timeout: int = 10,
    extra_headers: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Главная функция: дергает API и возвращает нормализованный список фандингов.

    Пример использования:

        from lighter_funding_parser import fetch_lighter_funding

        rows = fetch_lighter_funding()
        for row in rows:
            print(row["symbol"], row["fundingRate"])

    Если когда-нибудь для /funding-rates введут обязательную авторизацию,
    можно вызвать так:

        rows = fetch_lighter_funding(
            extra_headers={"Authorization": f"Bearer {TOKEN}"}
        )
    """
    raw = fetch_lighter_funding_raw(
        base_url=base_url,
        timeout=timeout,
        extra_headers=extra_headers,
    )
    return normalize_lighter_funding(raw)


if __name__ == "__main__":
    # Простой ручной запуск для проверки
    logging.basicConfig(level=logging.INFO)

    try:
        rows = fetch_lighter_funding()
    except LighterFundingError as e:
        print("Ошибка при получении фандингов Lighter:", e)
    else:
        print(f"Получено записей: {len(rows)}")
        for row in rows[:10]:
            print(
                row["symbol"],
                "fundingRate=",
                row["fundingRate"],
                "fundingRateAnnual=",
                row["fundingRateAnnual"],
                "timestamp=",
                row["timestamp"],
            )
