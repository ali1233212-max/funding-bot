import logging
import asyncio
from datetime import datetime, timezone
import requests
from typing import Any, Dict, List, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Токены (замени на свои реальные)
TELEGRAM_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"
COINGLASS_TOKEN = "2d73a05799f64daab80329868a5264ea"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class LighterFundingAPI:
    """
    Парсер фандингов для биржи Lighter на основе публичных API.

    Использует:
    - Текущие ставки фандинга: https://mainnet.zklighter.elliot.ai/api/v1/funding-rates
    - Список рынков:           https://explorer.elliot.ai/api/markets

    Все эндпоинты публичные, без API-ключей.
    """

    BASE_URL = "https://mainnet.zklighter.elliot.ai/api/v1"
    EXPLORER_URL = "https://explorer.elliot.ai/api"

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self._markets_cache: Optional[Dict[str, Dict[str, Any]]] = None

    # ============ НИЗКОУРОВНЕВЫЕ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ============

    def _request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Универсальный HTTP-запрос с логированием и обработкой ошибок.
        """
        try:
            resp = requests.request(method, url, params=params, timeout=self.timeout)
        except Exception as e:
            logger.error("Lighter API request error %s %s: %s", method, url, e)
            raise

        if not resp.ok:
            logger.error(
                "Lighter API HTTP %s for %s %s: %s",
                resp.status_code,
                method,
                url,
                resp.text[:500],
            )
            resp.raise_for_status()

        try:
            data = resp.json()
        except Exception as e:
            logger.error("Lighter API JSON parse error for %s %s: %s", method, url, e)
            raise

        return data

    # ============ МАРКЕТЫ ============

    def get_markets_raw(self) -> Any:
        """
        Возвращает сырой ответ списка рынков с /api/markets (Explorer API).

        Эндпоинт: GET https://explorer.elliot.ai/api/markets
        """
        url = f"{self.EXPLORER_URL}/markets"
        return self._request("GET", url)

    def get_markets_map(self, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Кэширует и возвращает словарь рынков по market_id.

        Возвращает:
            {
                "BTC-PERP": {
                    "id": "BTC-PERP",
                    "symbol": "BTC-PERP",
                    "raw": {...}
                },
                ...
            }
        """
        if self._markets_cache is not None and not force_refresh:
            return self._markets_cache

        data = self.get_markets_raw()

        markets_map: Dict[str, Dict[str, Any]] = {}

        # Возможные варианты структуры:
        # 1) просто список объектов
        # 2) { "markets": [...] }
        if isinstance(data, dict) and "markets" in data:
            markets_list = data.get("markets") or []
        elif isinstance(data, list):
            markets_list = data
        else:
            logger.warning("Unexpected markets response format from Lighter: %s", type(data))
            markets_list = []

        for item in markets_list:
            if not isinstance(item, dict):
                continue

            market_id = (
                item.get("id")
                or item.get("marketId")
                or item.get("market_id")
            )
            if not market_id:
                # если нет явного id — пропускаем
                continue

            symbol = (
                item.get("symbol")
                or item.get("ticker")
                or item.get("name")
                or str(market_id)
            )

            markets_map[str(market_id)] = {
                "id": str(market_id),
                "symbol": str(symbol),
                "raw": item,
            }

        self._markets_cache = markets_map
        return markets_map

    # ============ ФАНДИНГИ ============

    def get_funding_rates_raw(self, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Сырой вызов текущих ставок фандинга.

        Эндпоинт: GET https://mainnet.zklighter.elliot.ai/api/v1/funding-rates

        params — оставляю на будущее (если понадобится фильтрация по marketId и т.п.).
        Сейчас можно вызывать без параметров для получения всех рынков.
        """
        url = f"{self.BASE_URL}/funding-rates"
        return self._request("GET", url, params=params)

    def _normalize_funding_entry(
        self,
        entry: Dict[str, Any],
        markets_map: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Нормализует одну запись фандинга в единый формат.

        Если ставка фандинга равна 0 — возвращает None (чтобы убрать нулевые фандинги).
        """
        market_id = (
            entry.get("marketId")
            or entry.get("market_id")
            or entry.get("id")
        )
        market_id = str(market_id) if market_id is not None else None

        symbol = None
        if market_id and market_id in markets_map:
            symbol = markets_map[market_id]["symbol"]
        symbol = (
            symbol
            or entry.get("symbol")
            or entry.get("ticker")
            or entry.get("name")
            or market_id
        )

        funding_rate_hourly = (
            entry.get("hourlyFundingRate")
            or entry.get("fundingRateHourly")
            or entry.get("fundingRate")
            or entry.get("funding_rate")
        )

        funding_rate_predicted = (
            entry.get("predictedFundingRate")
            or entry.get("predictedFunding")
            or entry.get("nextFundingRate")
        )

        funding_rate_8h = (
            entry.get("fundingRate8h")
            or entry.get("fundingRatePerPeriod")
            or entry.get("fundingRatePer8h")
        )

        next_funding_time = (
            entry.get("nextFundingTime")
            or entry.get("nextFundingTimestamp")
        )

        if funding_rate_hourly is None and funding_rate_predicted is None and funding_rate_8h is None:
            return None

        base_rate = funding_rate_hourly
        if base_rate is None:
            base_rate = funding_rate_8h
        if base_rate is None:
            base_rate = funding_rate_predicted

        try:
            base_rate_float = float(base_rate)
        except Exception:
            base_rate_float = None

        if base_rate_float is not None and base_rate_float == 0.0:
            return None

        return {
            "market_id": market_id,
            "symbol": symbol,
            "funding_rate_hourly": funding_rate_hourly,
            "funding_rate_8h": funding_rate_8h,
            "funding_rate_predicted": funding_rate_predicted,
            "next_funding_time": next_funding_time,
            "raw": entry,
        }

    def get_all_funding_nonzero(self) -> List[Dict[str, Any]]:
        """
        Возвращает список нормализованных записей фандинга для ВСЕХ рынков Lighter
        БЕЗ НУЛЕВЫХ ставок фандинга.
        """
        raw = self.get_funding_rates_raw()
        markets_map = self.get_markets_map()

        if isinstance(raw, dict) and "data" in raw:
            entries = raw.get("data") or []
        elif isinstance(raw, list):
            entries = raw
        else:
            logger.warning("Unexpected funding-rates response format from Lighter: %s", type(raw))
            entries = []

        result: List[Dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            norm = self._normalize_funding_entry(entry, markets_map)
            if norm is not None:
                result.append(norm)

        def _key(e: Dict[str, Any]) -> float:
            v = e.get("funding_rate_hourly") or e.get("funding_rate_8h") or e.get("funding_rate_predicted") or 0
            try:
                return abs(float(v))
            except Exception:
                return 0.0

        result.sort(key=_key, reverse=True)
        return result

    def get_funding_for_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        symbol_lower = symbol.lower()
        all_items = self.get_all_funding_nonzero()
        return [
            item for item in all_items
            if item.get("symbol", "").lower() == symbol_lower
               or item.get("market_id", "").lower() == symbol_lower
        ]

    def get_top_funding(
        self,
        limit: int = 20,
        min_abs_rate: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        all_items = self.get_all_funding_nonzero()
        if min_abs_rate is not None:
            filtered: List[Dict[str, Any]] = []
            for item in all_items:
                v = item.get("funding_rate_hourly") or item.get("funding_rate_8h") or item.get("funding_rate_predicted")
                try:
                    if abs(float(v)) >= float(min_abs_rate):
                        filtered.append(item)
                except Exception:
                    continue
            all_items = filtered

        return all_items[:limit]


class CoinglassAPI:
    """
    Обёртка над Coinglass API + Hyperliquid + Paradex + EdgeX + Lighter
    """
    def __init__(self):
        self.base_url_v3 = "https://open-api.coinglass.com/api/pro/v1"
        self.base_url_v4 = "https://open-api-v4.coinglass.com/api"
        self.headers_v3 = {
            "accept": "application/json",
            "coinglassSecret": COINGLASS_TOKEN,
        }
        self.headers_v4 = {
            "accept": "application/json",
            "CG-API-KEY": COINGLASS_TOKEN,
        }

        # Публичный REST API Paradex (без ключей)
        self.paradex_base_url = "https://api.prod.paradex.trade/v1"
        self.paradex_headers = {
            "accept": "application/json",
        }

        # Публичный REST API EdgeX (без ключей)
        self.edgex_base_url = "https://pro.edgex.exchange"
        self.edgex_headers = {
            "accept": "application/json",
        }

        # Cooldown для EdgeX и Lighter, чтобы не ловить массу 429 Too Many Requests
        self._edgex_last_attempt = None
        self._edgex_min_interval_seconds = 300  # EdgeX — не чаще 1 раза в 5 минут

        self._lighter_last_attempt = None
        self._lighter_min_interval_seconds = 120  # Lighter — не чаще 1 раза в 2 минуты

    def _normalize_interval(self, val):
        """
        Нормализация интервала фандинга в часы.
        """
        try:
            if val in (None, "", "?"):
                return 8
            hours = float(val)
            if hours <= 0:
                return 8
            if float(hours).is_integer():
                return int(hours)
            return hours
        except Exception:
            return 8

    def get_funding_rates(self):
        """
        Полный запрос всех ставок фандинга с Coinglass + доп. добавление Hyperliquid + Paradex + EdgeX + Lighter
        """
        url = f"{self.base_url_v4}/futures/funding-rate/exchange-list"
        MAX_RETRIES = 3
        TIMEOUT = 60

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=self.headers_v4, timeout=TIMEOUT)
                resp.raise_for_status()
                data = resp.json()

                if data.get("code") != "0":
                    logger.warning("Coinglass v4 funding-rate/exchange-list error: %s", data)
                    return None

                entries = data.get("data", [])
                result = []

                for entry in entries:
                    sym = entry.get("symbol", "")
                    stable_list = entry.get("stablecoin_margin_list") or []
                    token_list = entry.get("token_margin_list") or []

                    # Стейбл-маржа
                    for row in stable_list:
                        try:
                            rate = float(row.get("funding_rate", 0.0))
                        except (TypeError, ValueError):
                            rate = 0.0
                        interval = self._normalize_interval(row.get("funding_rate_interval"))

                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            "rate": rate,  # проценты за интервал
                            "marginType": "STABLE",
                            "interval": interval,
                            "nextFundingTime": row.get("next_funding_time", ""),
                            "stableCoin": "STABLE",
                        }
                        result.append(item)

                    # COIN-маржа
                    for row in token_list:
                        try:
                            rate = float(row.get("funding_rate", 0.0))
                        except (TypeError, ValueError):
                            rate = 0.0
                        interval = self._normalize_interval(row.get("funding_rate_interval"))

                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            "rate": rate,
                            "marginType": "COIN",
                            "interval": interval,
                            "nextFundingTime": row.get("next_funding_time", ""),
                        }
                        result.append(item)

                logger.info("Coinglass v4 funding-rate: получили %d записей", len(result))

                # Лог по биржам из Coinglass
                try:
                    from collections import Counter
                    ex_counter = Counter(
                        row.get("exchangeName", "")
                        for row in result
                        if row.get("exchangeName")
                    )
                    logger.info(
                        "Биржи в данных Coinglass: %s",
                        ", ".join(f"{k}:{v}" for k, v in ex_counter.items())
                    )
                except Exception as log_ex:
                    logger.warning("Не удалось залогировать список бирж: %s", log_ex)

                # Добавляем Hyperliquid из нативного API
                try:
                    hl_items = self._get_hyperliquid_funding()
                    if hl_items:
                        existing_keys = {
                            (str(row.get("symbol")), str(row.get("exchangeName")).lower())
                            for row in result
                        }
                        added = 0
                        for it in hl_items:
                            key = (str(it.get("symbol")), str(it.get("exchangeName")).lower())
                            if key in existing_keys:
                                continue
                            result.append(it)
                            existing_keys.add(key)
                            added += 1
                        logger.info(
                            "Hyperliquid: добавлено %d новых записей в общий кэш фандинга",
                            added,
                        )
                    else:
                        logger.info("Hyperliquid: нативный API вернул 0 записей")
                except Exception as hl_ex:
                    logger.warning("Ошибка при добавлении Hyperliquid: %s", hl_ex)

                # Добавляем Paradex из нативного API
                try:
                    pdx_items = self._get_paradex_funding()
                    if pdx_items:
                        existing_keys = {
                            (str(row.get("symbol")), str(row.get("exchangeName")).lower())
                            for row in result
                        }
                        added = 0
                        for it in pdx_items:
                            key = (str(it.get("symbol")), str(it.get("exchangeName")).lower())
                            if key in existing_keys:
                                continue
                            result.append(it)
                            existing_keys.add(key)
                            added += 1
                        logger.info(
                            "Paradex: добавлено %d новых записей в общий кэш фандинга",
                            added,
                        )
                    else:
                        logger.info("Paradex: нативный API вернул 0 записей")
                except Exception as pdx_ex:
                    logger.warning("Ошибка при добавлении Paradex: %s", pdx_ex)

                # Добавляем EdgeX из нативного API
                try:
                    edgex_items = self._get_edgex_funding()
                    if edgex_items:
                        existing_keys = {
                            (str(row.get("symbol")), str(row.get("exchangeName")).lower())
                            for row in result
                        }
                        added = 0
                        for it in edgex_items:
                            key = (str(it.get("symbol")), str(it.get("exchangeName")).lower())
                            if key in existing_keys:
                                continue
                            result.append(it)
                            existing_keys.add(key)
                            added += 1
                        logger.info(
                            "EdgeX: добавлено %d новых записей в общий кэш фандинга",
                            added,
                        )
                    else:
                        logger.info("EdgeX: нативный API вернул 0 записей")
                except Exception as edx_ex:
                    logger.warning("Ошибка при добавлении EdgeX: %s", edx_ex)

                # Добавляем Lighter из нативного API
                try:
                    lighter_items = self._get_lighter_funding()
                    if lighter_items:
                        existing_keys = {
                            (str(row.get("symbol")), str(row.get("exchangeName")).lower())
                            for row in result
                        }
                        added = 0
                        for it in lighter_items:
                            key = (str(it.get("symbol")), str(it.get("exchangeName")).lower())
                            if key in existing_keys:
                                continue
                            result.append(it)
                            existing_keys.add(key)
                            added += 1
                        logger.info(
                            "Lighter: добавлено %d новых записей в общий кэш фандинга",
                            added,
                        )
                    else:
                        logger.info("Lighter: нативный API вернул 0 записей")
                except Exception as l_ex:
                    logger.warning("Ошибка при добавлении Lighter: %s", l_ex)

                return result

            except requests.exceptions.ReadTimeout:
                logger.warning(
                    "Таймаут при запросе к Coinglass v4 (попытка %d/%d)",
                    attempt,
                    MAX_RETRIES,
                )
                if attempt == MAX_RETRIES:
                    return None
            except requests.exceptions.RequestException as e:
                logger.error("Ошибка сети при запросе к Coinglass: %s", e)
                if attempt == MAX_RETRIES:
                    return None
            except Exception as e:
                logger.exception("Неожиданная ошибка при запросе к Coinglass v4: %s", e)
                return None

    def _get_hyperliquid_funding(self):
        """
        Дополнительная загрузка ставок фандинга с биржи Hyperliquid
        (metaAndAssetCtxs + predictedFundings)
        """
        items = []

        # 1) metaAndAssetCtxs — текущий funding
        try:
            url = "https://api.hyperliquid.xyz/info"
            payload = {"type": "metaAndAssetCtxs"}
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list) and len(data) >= 2:
                meta = data[0] or {}
                ctx_list = data[1] or []
                universe = meta.get("universe", [])

                if isinstance(universe, list) and isinstance(ctx_list, list):
                    n = min(len(universe), len(ctx_list))
                    for i in range(n):
                        u = universe[i] or {}
                        ctx = ctx_list[i] or {}
                        symbol = u.get("name")
                        if not symbol:
                            continue

                        funding_raw = ctx.get("funding")
                        if funding_raw in (None, "", "?"):
                            continue

                        try:
                            funding = float(funding_raw)
                        except (TypeError, ValueError):
                            continue

                        rate_percent = funding * 100.0

                        items.append({
                            "symbol": symbol,
                            "exchangeName": "Hyperliquid",
                            "rate": rate_percent,
                            "marginType": "USDC",
                            "interval": 8,
                            "nextFundingTime": "",
                            "stableCoin": "USDC",
                            "source": "hyperliquid_meta",
                        })

            logger.info("Hyperliquid metaAndAssetCtxs: %d записей", len(items))
        except requests.exceptions.RequestException as e:
            logger.warning("Не удалось получить Hyperliquid metaAndAssetCtxs: %s", e)
        except Exception as e:
            logger.warning("Ошибка при разборе Hyperliquid metaAndAssetCtxs: %s", e)

        # 2) predictedFundings — fallback
        if not items:
            try:
                url = "https://api.hyperliquid.xyz/info"
                payload = {"type": "predictedFundings"}
                resp = requests.post(url, json=payload, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, list):
                    for entry in data:
                        if not (isinstance(entry, list) and len(entry) == 2):
                            continue
                        symbol, venues = entry
                        if not isinstance(symbol, str):
                            continue
                        if not isinstance(venues, list):
                            continue

                        for venue in venues:
                            if not (isinstance(venue, list) and len(venue) == 2):
                                continue
                            venue_name, info = venue
                            if not isinstance(venue_name, str):
                                continue
                            if not isinstance(info, dict):
                                continue

                            if not venue_name.lower().startswith("hl"):
                                continue

                            fr_raw = info.get("fundingRate")
                            if fr_raw in (None, "", "?"):
                                continue

                            try:
                                fr = float(fr_raw)
                            except (TypeError, ValueError):
                                continue

                            rate_percent = fr * 100.0
                            interval_hours = 8

                            items.append({
                                "symbol": symbol,
                                "exchangeName": "Hyperliquid",
                                "rate": rate_percent,
                                "marginType": "USDC",
                                "interval": interval_hours,
                                "nextFundingTime": info.get("nextFundingTime", ""),
                                "stableCoin": "USDC",
                                "source": "hyperliquid_predicted",
                            })

                logger.info("Hyperliquid predictedFundings: %d записей", len(items))
            except requests.exceptions.RequestException as e:
                logger.warning("Не удалось получить Hyperliquid predictedFundings: %s", e)
            except Exception as e:
                logger.warning("Ошибка при разборе Hyperliquid predictedFundings: %s", e)

        if items:
            try:
                syms = sorted({it["symbol"] for it in items if it.get("symbol")})
                logger.info("Hyperliquid symbols в кэше (первые 20): %s", ", ".join(syms[:20]))
            except Exception:
                pass

        return items

    def _get_paradex_funding(self):
        """
        Загрузка ставок фандинга с Paradex через публичный REST API.
        """
        items = []

        markets_meta = {}
        try:
            url_markets = f"{self.paradex_base_url}/markets"
            resp = requests.get(url_markets, headers=self.paradex_headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            markets = data.get("results", []) or []

            for m in markets:
                try:
                    chain_details = m.get("chain_details") or {}
                    symbol = m.get("symbol") or chain_details.get("symbol")
                    if not symbol:
                        continue

                    period_raw = m.get("funding_period_hours", 8)
                    try:
                        period_h = float(period_raw) if period_raw not in (None, "", "?") else 8.0
                    except (TypeError, ValueError):
                        period_h = 8.0
                    if period_h <= 0:
                        period_h = 8.0

                    markets_meta[symbol] = {
                        "asset_kind": m.get("asset_kind"),
                        "funding_period_hours": period_h,
                        "settlement_currency": m.get("settlement_currency", "USDC"),
                    }
                except Exception:
                    continue

            logger.info("Paradex /markets: загружено %d рынков", len(markets_meta))
        except requests.exceptions.RequestException as e:
            logger.warning("Paradex: не удалось получить /markets: %s", e)
        except Exception as e:
            logger.warning("Paradex: ошибка при разборе /markets: %s", e)

        try:
            url_summary = f"{self.paradex_base_url}/markets/summary"
            params = {"market": "ALL"}
            resp = requests.get(url_summary, headers=self.paradex_headers, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("results", []) or []

            for row in rows:
                symbol = row.get("symbol")
                if not symbol:
                    continue

                meta = markets_meta.get(symbol, {})
                asset_kind = meta.get("asset_kind")

                if asset_kind:
                    try:
                        if str(asset_kind).upper() != "PERP":
                            continue
                    except Exception:
                        pass
                else:
                    if "-PERP" not in symbol:
                        continue

                fr_raw = row.get("funding_rate")
                if fr_raw in (None, "", "?"):
                    continue

                try:
                    fr_val = float(fr_raw)
                except (TypeError, ValueError):
                    continue

                rate_percent = fr_val * 100.0

                interval_h = meta.get("funding_period_hours", 8.0)
                try:
                    interval_h = float(interval_h)
                except (TypeError, ValueError):
                    interval_h = 8.0
                if interval_h <= 0:
                    interval_h = 8.0

                settlement = meta.get("settlement_currency", "USDC")

                items.append({
                    "symbol": symbol,
                    "exchangeName": "Paradex",
                    "rate": rate_percent,
                    "marginType": settlement,
                    "interval": interval_h,
                    "nextFundingTime": "",
                    "stableCoin": settlement,
                    "source": "paradex_markets_summary",
                })

            logger.info("Paradex /markets/summary: получено %d записей funding", len(items))
        except requests.exceptions.RequestException as e:
            logger.warning("Не удалось получить Paradex /markets/summary: %s", e)
        except Exception as e:
            logger.warning("Ошибка при обработке Paradex /markets/summary: %s", e)

        if items:
            try:
                syms = sorted({it["symbol"] for it in items if it.get("symbol")})
                logger.info("Paradex symbols в кэше (первые 20): %s", ", ".join(syms[:20]))
            except Exception:
                pass

        return items

    def _get_edgex_funding(self) -> List[Dict[str, Any]]:
        """
        Загрузка ставок фандинга с EdgeX через публичный REST API.
        """
        items: List[Dict[str, Any]] = []

        # простой cooldown, чтобы не долбить EdgeX каждые 30 секунд
        try:
            now = datetime.now(timezone.utc)
        except Exception:
            now = datetime.utcnow()

        if self._edgex_last_attempt is not None:
            try:
                delta = (now - self._edgex_last_attempt).total_seconds()
            except Exception:
                delta = None
            if delta is not None and delta < self._edgex_min_interval_seconds:
                logger.info(
                    "EdgeX: пропускаем запрос фандинга (cooldown ещё %.1f c)",
                    self._edgex_min_interval_seconds - delta,
                )
                return items

        self._edgex_last_attempt = now

        contracts_meta: Dict[str, Dict[str, Any]] = {}
        coin_by_id: Dict[str, Dict[str, Any]] = {}

        # 1) meta
        try:
            url_meta = f"{self.edgex_base_url}/api/v1/public/meta/getMetaData"
            resp = requests.get(url_meta, headers=self.edgex_headers, timeout=10)
            resp.raise_for_status()
            meta_json = resp.json()

            if meta_json.get("code") != "SUCCESS":
                logger.warning("EdgeX meta/getMetaData error: %s", meta_json)
                return items

            data = meta_json.get("data") or {}

            for coin in data.get("coinList", []) or []:
                cid = coin.get("coinId")
                if cid:
                    coin_by_id[cid] = coin

            for c in data.get("contractList", []) or []:
                cid = c.get("contractId")
                if not cid:
                    continue
                if not c.get("enableDisplay", True):
                    continue
                if not c.get("enableTrade", True):
                    continue
                if not c.get("enableOpenPosition", True):
                    continue
                contracts_meta[cid] = c

            logger.info("EdgeX meta/getMetaData: загружено %d активных контрактов", len(contracts_meta))
        except Exception as e:
            logger.warning("EdgeX: ошибка при запросе meta/getMetaData: %s", e)
            return items

        if not contracts_meta:
            return items

        def _safe_float(x: Any) -> Optional[float]:
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        funding_by_id: Dict[str, Dict[str, Any]] = {}

        # 2) bulk getLatestFundingRate
        bulk_429 = False
        try:
            url_funding = f"{self.edgex_base_url}/api/v1/public/funding/getLatestFundingRate"
            resp = requests.get(url_funding, headers=self.edgex_headers, timeout=10)
            resp.raise_for_status()
            f_json = resp.json()
            if f_json.get("code") == "SUCCESS":
                data_list = f_json.get("data") or []
                if isinstance(data_list, list):
                    for fr in data_list:
                        cid = fr.get("contractId")
                        if not cid:
                            continue
                        funding_by_id[cid] = fr
                logger.info("EdgeX getLatestFundingRate (bulk): получено %d записей", len(funding_by_id))
            else:
                logger.warning("EdgeX getLatestFundingRate (bulk) code != SUCCESS: %s", f_json)
        except requests.exceptions.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 429:
                bulk_429 = True
                logger.warning("EdgeX: bulk getLatestFundingRate вернул 429 Too Many Requests, перезапросы отключены")
            else:
                logger.warning("EdgeX: ошибка bulk getLatestFundingRate, fallback per-contract: %s", e)
        except Exception as e:
            logger.warning("EdgeX: ошибка bulk getLatestFundingRate, fallback per-contract: %s", e)

        # 3) per-contract fallback (если bulk не сработал и это не явный 429)
        if (not funding_by_id or len(funding_by_id) < len(contracts_meta)) and not bulk_429:
            for cid in contracts_meta.keys():
                if cid in funding_by_id:
                    continue
                try:
                    url_funding = f"{self.edgex_base_url}/api/v1/public/funding/getLatestFundingRate"
                    resp = requests.get(
                        url_funding,
                        headers=self.edgex_headers,
                        params={"contractId": cid},
                        timeout=10,
                    )
                    resp.raise_for_status()
                    f_json = resp.json()
                    if f_json.get("code") != "SUCCESS":
                        continue
                    data_list = f_json.get("data") or []
                    if not isinstance(data_list, list) or not data_list:
                        continue
                    funding_by_id[cid] = data_list[-1]
                except requests.exceptions.HTTPError as e:
                    status = getattr(getattr(e, "response", None), "status_code", None)
                    if status == 429:
                        logger.warning(
                            "EdgeX: 429 Too Many Requests при запросе контракта %s, прекращаем fallback", cid
                        )
                        break
                    logger.warning(
                        "EdgeX: ошибка getLatestFundingRate для контракта %s: %s",
                        cid,
                        e,
                    )
                except Exception as e:
                    logger.warning(
                        "EdgeX: ошибка getLatestFundingRate для контракта %s: %s",
                        cid,
                        e,
                    )

        # 4) нормализация
        for cid, meta in contracts_meta.items():
            fr = funding_by_id.get(cid)
            if not fr:
                continue

            rate_dec = _safe_float(fr.get("fundingRate"))
            if rate_dec is None:
                continue
            rate_percent = rate_dec * 100.0

            interval_min = _safe_float(fr.get("fundingRateIntervalMin") or meta.get("fundingRateIntervalMin"))
            if interval_min is None or interval_min <= 0:
                interval_min = 240.0
            interval_hours = interval_min / 60.0

            quote_coin_id = meta.get("quoteCoinId")
            quote_coin = coin_by_id.get(quote_coin_id, {})
            quote_name = quote_coin.get("coinName") or "USDT"

            symbol = meta.get("contractName") or cid

            items.append({
                "symbol": symbol,
                "exchangeName": "EdgeX",
                "rate": rate_percent,
                "marginType": quote_name,
                "interval": interval_hours,
                "nextFundingTime": fr.get("fundingTime", ""),
                "stableCoin": quote_name,
                "source": "edgex_funding",
            })

        logger.info("EdgeX: нормализовано %d записей funding", len(items))
        return items

    def _get_lighter_funding(self) -> List[Dict[str, Any]]:
        """
        Загрузка ставок фандинга с Lighter через публичный API.
        Использует LighterFundingAPI и приводит данные к формату бота.
        """
        items: List[Dict[str, Any]] = []

        # cooldown, чтобы не долбить Lighter каждые 30 секунд
        try:
            now = datetime.now(timezone.utc)
        except Exception:
            now = datetime.utcnow()

        if self._lighter_last_attempt is not None:
            try:
                delta = (now - self._lighter_last_attempt).total_seconds()
            except Exception:
                delta = None
            if delta is not None and delta < self._lighter_min_interval_seconds:
                logger.info(
                    "Lighter: пропускаем запрос фандинга (cooldown ещё %.1f c)",
                    self._lighter_min_interval_seconds - delta,
                )
                return items

        self._lighter_last_attempt = now

        try:
            api = LighterFundingAPI(timeout=10)
            raw_items = api.get_all_funding_nonzero()
        except Exception as e:
            logger.warning("Lighter: ошибка при запросе фандинга: %s", e)
            return items

        for entry in raw_items:
            base = (
                entry.get("funding_rate_8h")
                or entry.get("funding_rate_hourly")
                or entry.get("funding_rate_predicted")
            )
            try:
                base_dec = float(base)
            except Exception:
                continue

            if entry.get("funding_rate_8h") is not None:
                interval_hours = 8.0
            else:
                interval_hours = 1.0

            rate_percent = base_dec * 100.0

            symbol = (
                entry.get("symbol")
                or entry.get("market_id")
                or "UNKNOWN"
            )

            items.append({
                "symbol": symbol,
                "exchangeName": "Lighter",
                "rate": rate_percent,
                "marginType": "USDC",
                "interval": interval_hours,
                "nextFundingTime": entry.get("next_funding_time") or "",
                "stableCoin": "USDC",
                "source": "lighter_funding",
            })

        logger.info("Lighter: нормализовано %d записей funding", len(items))
        return items

    def get_arbitrage_opportunities(self):
        """
        Арбитраж по цене через v3 API (доп. функция)
        """
        url = f"{self.base_url_v3}/futures/market"
        params = {"symbol": "BTC"}

        try:
            response = requests.get(
                url, headers=self.headers_v3, params=params, timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return self._calculate_arbitrage(data.get("data", []))
            logger.warning("Coinglass v3 futures/market error: %s", response.text)
            return None
        except Exception as e:
            logger.exception("Ошибка при запросе к Coinglass v3 futures/market: %s", e)
            return None

    def _calculate_arbitrage(self, market_data):
        opportunities = []
        for coin_data in market_data:
            symbol = coin_data.get("symbol", "")
            exchanges = coin_data.get("exchangeName", [])
            prices = coin_data.get("price", [])

            if len(prices) >= 2:
                try:
                    prices_float = [float(p) for p in prices]
                except Exception:
                    continue

                min_price = min(prices_float)
                max_price = max(prices_float)

                if min_price > 0:
                    spread_percent = ((max_price - min_price) / min_price) * 100
                    if spread_percent > 0.5:
                        opportunities.append({
                            "symbol": symbol,
                            "min_price": min_price,
                            "max_price": max_price,
                            "spread_percent": round(spread_percent, 2),
                            "exchanges": exchanges,
                        })

        return sorted(opportunities, key=lambda x: x["spread_percent"], reverse=True)

    def calculate_funding_arbitrage_from_items(self, funding_items, symbol=None, min_spread=0.0005):
        """
        Расчёт арбитража фандинга из загруженных данных.
        min_spread — в тех же единицах, что и rate (проценты за интервал)
        """
        if not funding_items:
            return None

        by_symbol = {}
        for item in funding_items:
            sym = item.get("symbol", "")
            if not sym:
                continue

            if symbol and sym.upper() != symbol.upper():
                continue

            margin_type = item.get("marginType", "USDT")
            if str(margin_type).upper() not in ("USDT", "USDC", "USD", "STABLE"):
                continue

            rate = item.get("rate", 0)
            exchange = item.get("exchangeName", "")
            if not exchange:
                continue

            try:
                r = float(rate)
            except (TypeError, ValueError):
                continue

            by_symbol.setdefault(sym, []).append((exchange, r))

        opportunities = []
        for sym, ex_rates in by_symbol.items():
            if len(ex_rates) < 2:
                continue

            min_ex, min_rate = min(ex_rates, key=lambda x: x[1])
            max_ex, max_rate = max(ex_rates, key=lambda x: x[1])
            spread = max_rate - min_rate

            if abs(spread) < min_spread:
                continue

            opportunities.append({
                "symbol": sym,
                "min_exchange": min_ex,
                "max_exchange": max_ex,
                "min_rate": min_rate,
                "max_rate": max_rate,
                "spread": spread,
            })

        if not opportunities:
            return None

        opportunities.sort(key=lambda x: abs(x["spread"]), reverse=True)
        return opportunities


class CryptoArbBot:
    def __init__(self):
        self.api = CoinglassAPI()
        self.application = Application.builder().token(TELEGRAM_TOKEN).build()
        self.funding_cache = []
        self.funding_cache_updated_at = None
        self.cache_lock = asyncio.Lock()

        self.MIN_ABS_RATE = 1e-6

        self.setup_handlers()

    def annualize_rate(self, rate, interval):
        """
        Перевод ставки фандинга за период в годовую ПРОЦЕНТНУЮ ставку (APR).
        rate — в процентах за интервал (0.01 = 0.01%)
        interval — длительность интервала в часах
        """
        try:
            if interval in (None, "", "?"):
                hours = 8.0
            else:
                hours = float(interval)
        except (TypeError, ValueError):
            hours = 8.0

        if hours <= 0:
            hours = 8.0

        periods_per_year = 365.0 * 24.0 / hours
        annual_percent = rate * periods_per_year
        return annual_percent

    def format_annual_rate(self, annual_rate: float) -> str:
        v = float(annual_rate)
        if abs(v) >= 10:
            return f"{v:+.2f}%"
        elif abs(v) >= 1:
            return f"{v:+.3f}%"
        elif abs(v) >= 0.1:
            return f"{v:+.4f}%"
        else:
            return f"{v:+.5f}%"

    def get_exchange_emoji(self, exchange: str) -> str:
        if isinstance(exchange, str):
            name = exchange.lower()
            if name == "hyperliquid":
                return "🌊"
            if "paradex" in name:
                return "🌀"
            if "edgex" in name:
                return "🧊"
            if "lighter" in name:
                return "🔥"
        return "🏦"

    async def update_funding_cache(self, context: ContextTypes.DEFAULT_TYPE):
        async with self.cache_lock:
            try:
                logger.info("Начало обновления кэша фандинга...")
                data = await asyncio.to_thread(self.api.get_funding_rates)
                if data:
                    self.funding_cache = data
                    self.funding_cache_updated_at = datetime.now(timezone.utc)
                    logger.info(
                        "Кэш фандинга успешно обновлён: %d записей",
                        len(self.funding_cache),
                    )
                else:
                    logger.warning("Не удалось получить данные от Coinglass/доп. источников")
            except Exception as e:
                logger.exception("Критическая ошибка при обновлении кэша: %s", e)

    def get_cached_funding(self, symbol=None):
        if not self.funding_cache:
            return None
        if symbol:
            symbol_upper = symbol.upper()
            return [
                item for item in self.funding_cache
                if item.get("symbol", "").upper() == symbol_upper
            ]
        return self.funding_cache

    def get_filtered_funding(self, funding_type="all"):
        data = self.get_cached_funding()
        if not data:
            return None
        if funding_type == "negative":
            filtered = [item for item in data if item.get("rate", 0) < 0]
            return sorted(filtered, key=lambda x: x["rate"])
        elif funding_type == "positive":
            filtered = [item for item in data if item.get("rate", 0) > 0]
            return sorted(filtered, key=lambda x: x["rate"], reverse=True)
        else:
            return data

    def get_all_exchanges(self):
        if not self.funding_cache:
            return None
        exchanges = set()
        for item in self.funding_cache:
            exchange = item.get("exchangeName", "")
            if exchange:
                exchanges.add(exchange)
        return sorted(list(exchanges))

    def setup_handlers(self):
        handlers = [
            CommandHandler("start", self.start),
            CommandHandler("negative", self.show_negative),
            CommandHandler("positive", self.show_positive),
            CommandHandler("top10", self.show_top10),
            CommandHandler("arbitrage_bundles", self.show_arbitrage_bundles),
            CommandHandler("price_arbitrage", self.show_price_arbitrage),
            CommandHandler("status", self.show_status),
            CommandHandler("exchanges", self.show_exchanges),
            CommandHandler("hyperliquid", self.show_hyperliquid),
            CommandHandler("edgex", self.show_edgex),
            CommandHandler("lighter", self.show_lighter),
            CallbackQueryHandler(self.button_handler, pattern="^(page_|nav_)"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message),
        ]
        for handler in handlers:
            self.application.add_handler(handler)
        self.application.add_error_handler(self.error_handler)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Специально игнорируем/логируем конфликт getUpdates, чтобы не спамить трейсами
        if isinstance(context.error, Conflict):
            logger.error(
                "⚠️ Telegram Conflict: бот уже запущен в другом процессе или среде. "
                "Убедись, что работает только один экземпляр с этим токеном."
            )
            return

        logger.error("Exception while handling an update:", exc_info=context.error)
        try:
            if update and hasattr(update, "effective_chat"):
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Произошла ошибка при обработке запроса. Попробуйте ещё раз.",
                )
        except Exception as e:
            logger.error("Ошибка при отправке сообщения об ошибке: %s", e)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("🔴 Все отрицательные", callback_data="nav_negative_1")],
            [InlineKeyboardButton("🟢 Все положительные", callback_data="nav_positive_1")],
            [InlineKeyboardButton("🚀 Топ 10 лучших", callback_data="nav_top10")],
            [InlineKeyboardButton("⚖️ Связки арбитража", callback_data="nav_arbitrage")],
            [InlineKeyboardButton("🌊 Hyperliquid", callback_data="nav_hyperliquid")],
            [InlineKeyboardButton("🏛️ Все биржи", callback_data="nav_exchanges")],
            [InlineKeyboardButton("💰 Ценовой арбитраж", callback_data="nav_price_arb")],
            [InlineKeyboardButton("📊 Статус бота", callback_data="nav_status")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        welcome_text = (
            "🤖 <b>Crypto Funding & Arbitrage Bot</b>\n\n"
            "📈 <b>Доступные команды:</b>\n"
            "/negative - все отрицательные фандинги\n"
            "/positive - все положительные фандинги\n"
            "/top10 - топ 10 положительных и отрицательных\n"
            "/arbitrage_bundles - связки арбитража фандинга\n"
            "/exchanges - все доступные биржи\n"
            "/price_arbitrage - ценовой арбитраж\n"
            "/hyperliquid - только пары с биржи Hyperliquid\n"
            "/edgex - только пары с биржи EdgeX\n"
            "/lighter - только пары с биржи Lighter\n"
            "/status - статус бота и кэша\n\n"
            "⚡ Особенности:\n"
            "• Пагинация по 20 записей\n"
            "• Сортировка: отрицательные по мере роста, положительные по мере убывания\n"
            "• Проверка времени выплат в арбитраже\n"
            "• Кэширование каждые 30 секунд\n"
            "• Поддержка Hyperliquid, Paradex, EdgeX и Lighter через нативный API\n\n"
            "Все ставки показываются в <b>процентах годовых (APR)</b>, рассчитанных из текущей ставки за интервал."
        )
        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode="HTML")

    async def show_negative(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_funding_page(update, context, "negative", 1)

    async def show_positive(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_funding_page(update, context, "positive", 1)

    async def show_funding_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, funding_type: str, page: int):
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            error_msg = (
                "⚠️ <b>Данные ещё не загружены</b>\n\n"
                "Кэш фандинга пуст. Возможные причины:\n"
                "• Бот только что запустился\n"
                "• Проблемы с API Coinglass\n"
                "• Превышены лимиты запросов\n\n"
                "Попробуйте через 30 секунд..."
            )
            await send_method(error_msg, parse_mode="HTML")
            return

        filtered_data = self.get_filtered_funding(funding_type)
        if not filtered_data:
            await send_method(
                "🤷‍♂️ <b>Нет данных для отображения</b>\n\nПопробуйте другой раздел.",
                parse_mode="HTML",
            )
            return

        items_per_page = 20
        total_items = len(filtered_data)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = filtered_data[start_idx:end_idx]

        context.user_data.update({
            "current_page": page,
            "total_pages": total_pages,
            "current_data_type": funding_type,
        })

        title_map = {
            "negative": "🔴 Отрицательные фандинги",
            "positive": "🟢 Положительные фандинги",
        }

        response = f"<b>{title_map[funding_type]} (APR)</b>\n"
        response += f"📄 Страница {page}/{total_pages} | Всего записей: {total_items}\n"
        response += (
            "💡 Показана приблизительная <b>годовая доходность (APR)</b> при линейном "
            "пересчёте текущей ставки за интервал.\n\n"
        )

        for item in page_data:
            symbol = item.get("symbol", "N/A")
            exchange = item.get("exchangeName", "N/A")
            raw_rate = item.get("rate", 0)
            interval = item.get("interval", 8)
            margin_type = item.get("marginType", "USDT")

            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)
            ex_emoji = self.get_exchange_emoji(exchange)
            emoji = "🔴" if funding_type == "negative" else "🟢"

            response += f"{emoji} <b>{symbol}</b>\n"
            response += f" {ex_emoji} {exchange} ({margin_type})\n"
            response += (
                f" 💰 {annual_str} годовых | ⏰ интервал: {interval}ч | "
                f"ставка за интервал: {raw_rate:.6f}%\n\n"
            )

        keyboard = []
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton("◀ Назад", callback_data=f"page_{funding_type}_{page-1}")
                )
            nav_buttons.append(
                InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info")
            )
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton("Вперёд ▶", callback_data=f"page_{funding_type}_{page+1}")
                )
            keyboard.append(nav_buttons)

            if total_pages > 5:
                quick_pages = set([1, max(1, page - 2), page, min(total_pages, page + 2), total_pages])
                quick_nav = []
                for quick_page in sorted(quick_pages):
                    if quick_page != page:
                        quick_nav.append(
                            InlineKeyboardButton(
                                str(quick_page),
                                callback_data=f"page_{funding_type}_{quick_page}",
                            )
                        )
                if quick_nav:
                    keyboard.append(quick_nav)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await send_method(response, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            logger.error("Ошибка при отправке сообщения: %s", e)
            await send_method(
                "❌ <b>Ошибка при отображении данных</b>\nПопробуйте ещё раз.",
                parse_mode="HTML",
            )

    async def show_top10(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        positive_data = self.get_filtered_funding("positive")[:10]
        negative_data = self.get_filtered_funding("negative")[:10]

        response = "<b>🚀 Топ 10 лучших фандингов (APR)</b>\n\n"
        response += "<b>🟢 Топ 10 положительных (годовых, по убыванию):</b>\n"
        for i, item in enumerate(positive_data, 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            interval = item.get("interval", 8)
            raw_rate = item.get("rate", 0)
            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)
            ex_emoji = self.get_exchange_emoji(exchange)
            response += (
                f"{i}. <b>{symbol}</b> - {annual_str} годовых "
                f"({ex_emoji} {exchange}, интервал: {interval}ч, ставка за интервал: {raw_rate:.6f}%)\n"
            )

        response += "\n<b>🔴 Топ 10 отрицательных (годовых, по мере роста):</b>\n"
        for i, item in enumerate(negative_data, 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            interval = item.get("interval", 8)
            raw_rate = item.get("rate", 0)
            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)
            ex_emoji = self.get_exchange_emoji(exchange)
            response += (
                f"{i}. <b>{symbol}</b> - {annual_str} годовых "
                f"({ex_emoji} {exchange}, интервал: {interval}ч, ставка за интервал: {raw_rate:.6f}%)\n"
            )

        if self.funding_cache_updated_at:
            cache_time = self.funding_cache_updated_at.strftime("%H:%M:%S")
            response += f"\n🕒 <i>Данные обновлены: {cache_time} UTC</i>"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_arbitrage_bundles(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
        """
        Арбитражные связки (APR) с пагинацией
        """
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        symbol_data = {}
        for item in self.funding_cache:
            symbol = item.get("symbol", "")
            if not symbol:
                continue
            rate = item.get("rate", 0)
            symbol_data.setdefault(symbol, []).append({
                "exchange": item.get("exchangeName", ""),
                "rate": rate,
                "interval": item.get("interval", 8),
                "marginType": item.get("marginType", ""),
            })

        opportunities = []
        for symbol, exchanges in symbol_data.items():
            if len(exchanges) < 2:
                continue
            valid_exchanges = exchanges
            if len(valid_exchanges) < 2:
                continue

            min_item = min(valid_exchanges, key=lambda x: x["rate"])
            max_item = max(valid_exchanges, key=lambda x: x["rate"])
            spread = max_item["rate"] - min_item["rate"]

            if abs(spread) < 0.0005:
                continue

            time_warning = ""
            if min_item["interval"] != max_item["interval"]:
                time_warning = " ⚠️ РАЗНОЕ ВРЕМЯ ВЫПЛАТ!"

            opportunities.append({
                "symbol": symbol,
                "min_exchange": min_item["exchange"],
                "max_exchange": max_item["exchange"],
                "min_rate": min_item["rate"],
                "max_rate": max_item["rate"],
                "min_interval": min_item["interval"],
                "max_interval": max_item["interval"],
                "spread": spread,
                "time_warning": time_warning,
            })

        opportunities.sort(key=lambda x: abs(x["spread"]), reverse=True)

        response = "<b>⚖️ Связки арбитража фандинга (APR)</b>\n\n"
        if not opportunities:
            response += (
                "🤷‍♂️ <b>Арбитражные возможности не найдены</b>\n\n"
                "Возможные причины:\n"
                "• Слишком маленький спред между биржами\n"
                "• Недостаточно данных по марже\n"
                "• Рынок в состоянии равновесия"
            )
            keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
            await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return

        items_per_page = 10
        total_items = len(opportunities)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = opportunities[start_idx:end_idx]

        context.user_data.update({
            "current_page": page,
            "total_pages": total_pages,
            "current_data_type": "arbitrage",
        })

        response += f"📊 Найдено возможностей: {total_items} | Страница {page}/{total_pages}\n"
        response += (
            "💡 Ставки показаны в <b>годовых процентах (APR)</b> "
            "с учётом интервала каждой биржи.\n\n"
        )

        for opp in page_data:
            min_annual = self.annualize_rate(opp["min_rate"], opp["min_interval"])
            max_annual = self.annualize_rate(opp["max_rate"], opp["max_interval"])
            spread_annual = max_annual - min_annual

            min_emoji = self.get_exchange_emoji(opp["min_exchange"])
            max_emoji = self.get_exchange_emoji(opp["max_exchange"])

            min_annual_str = self.format_annual_rate(min_annual)
            max_annual_str = self.format_annual_rate(max_annual)
            spread_annual_str = self.format_annual_rate(spread_annual)

            response += f"🎯 <b>{opp['symbol']}</b>{opp['time_warning']}\n"
            response += (
                f" 📉 {min_emoji} {opp['min_exchange']}: {min_annual_str} годовых "
                f"(интервал: {opp['min_interval']}ч, ставка за интервал: {opp['min_rate']:.6f}%)\n"
            )
            response += (
                f" 📈 {max_emoji} {opp['max_exchange']}: {max_annual_str} годовых "
                f"(интервал: {opp['max_interval']}ч, ставка за интервал: {opp['max_rate']:.6f}%)\n"
            )
            response += f" 💰 Спред (APR): {spread_annual_str}\n\n"

        keyboard = []
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton("◀ Назад", callback_data=f"page_arb_{page-1}")
                )
            nav_buttons.append(
                InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_arb_info")
            )
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton("Вперёд ▶", callback_data=f"page_arb_{page+1}")
                )
            keyboard.append(nav_buttons)

            if total_pages > 5:
                quick_pages = set([1, max(1, page - 2), page, min(total_pages, page + 2), total_pages])
                quick_row = []
                for p in sorted(quick_pages):
                    if p == page:
                        continue
                    quick_row.append(
                        InlineKeyboardButton(str(p), callback_data=f"page_arb_{p}")
                    )
                if quick_row:
                    keyboard.append(quick_row)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_method(response, reply_markup=reply_markup, parse_mode="HTML")

    async def show_exchanges(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список бирж + кнопки по каждой бирже"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        exchanges = self.get_all_exchanges()
        if not exchanges:
            await send_method("🤷‍♂️ Не удалось получить список бирж.")
            return

        response = "<b>🏛️ Все доступные биржи</b>\n\n"
        response += f"📊 Всего бирж: {len(exchanges)}\n\n"

        per_line = 3
        for i in range(0, len(exchanges), per_line):
            line = exchanges[i:i+per_line]
            decorated = [f"{self.get_exchange_emoji(ex)} {ex}" for ex in line]
            response += " • " + " • ".join(decorated) + "\n"

        unique_symbols = len(set(item.get("symbol", "") for item in self.funding_cache))
        total_records = len(self.funding_cache)

        response += "\n📈 <b>Статистика данных:</b>\n"
        response += f"• Всего записей: {total_records}\n"
        response += f"• Уникальных пар: {unique_symbols}\n"
        response += f"• Бирж: {len(exchanges)}\n"

        if self.funding_cache_updated_at:
            cache_time = self.funding_cache_updated_at.strftime("%H:%M:%S")
            response += f"\n🕒 <i>Данные обновлены: {cache_time} UTC</i>"

        keyboard = []
        row = []
        for ex in exchanges:
            row.append(
                InlineKeyboardButton(
                    f"{self.get_exchange_emoji(ex)} {ex}",
                    callback_data=f"nav_exch_{ex}",
                )
            )
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])

        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_price_arbitrage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ценовой арбитраж (BTC)"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        await send_method("🔍 Ищу арбитражные возможности по цене...")

        opportunities = self.api.get_arbitrage_opportunities()
        if not opportunities:
            await send_method("🤷‍♂️ Арбитражные возможности по цене не найдены")
            return

        response = "💸 <b>Арбитражные возможности по цене (BTC):</b>\n\n"
        for opp in opportunities[:10]:
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f" 📊 Спред: {opp['spread_percent']}%\n"
            response += f" 💰 Мин: ${opp['min_price']:.2f}\n"
            response += f" 💰 Макс: ${opp['max_price']:.2f}\n\n"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статус бота"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        cache_size = len(self.funding_cache) if self.funding_cache else 0
        last_update = (
            self.funding_cache_updated_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            if self.funding_cache_updated_at
            else "Никогда"
        )

        if self.funding_cache:
            positive_count = len([x for x in self.funding_cache if x.get("rate", 0) > 0])
            negative_count = len([x for x in self.funding_cache if x.get("rate", 0) < 0])
            zero_count = len([x for x in self.funding_cache if x.get("rate", 0) == 0])
            unique_symbols = len(set(item.get("symbol", "") for item in self.funding_cache))
            unique_exchanges = len(set(item.get("exchangeName", "") for item in self.funding_cache))
        else:
            positive_count = negative_count = zero_count = unique_symbols = unique_exchanges = 0

        response = (
            "📊 <b>Статус бота</b>\n\n"
            f"• 🗄️ Размер кэша: {cache_size} записей\n"
            f"• 🕒 Последнее обновление: {last_update}\n"
            f"• 📈 Уникальные символы: {unique_symbols}\n"
            f"• 🏛️ Уникальные биржи: {unique_exchanges}\n\n"
            f"<b>Статистика фандингов (по ставке за интервал):</b>\n"
            f"• 🟢 Положительные: {positive_count}\n"
            f"• 🔴 Отрицательные: {negative_count}\n"
            f"• ⚪ Нулевые: {zero_count}\n\n"
            "<i>Кэш обновляется каждые 30 секунд. Доходность в интерфейсе показана в годовых процентах (APR), "
            "исходя из последней ставки за интервал.</i>"
        )

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_hyperliquid(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
        """Пары только с биржи Hyperliquid (с пагинацией, данные из общего кэша)"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method(
                "⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.",
                parse_mode="HTML",
            )
            return

        hl_items = [
            item for item in self.funding_cache
            if isinstance(item.get("exchangeName"), str)
            and item["exchangeName"].lower() == "hyperliquid"
            and float(item.get("rate") or 0.0) != 0.0
        ]

        if not hl_items:
            msg = (
                "🌊 <b>Hyperliquid</b>\n\n"
                "В текущем кэше нет ни одной записи по бирже Hyperliquid с ненулевым фандингом.\n\n"
                "Возможные причины:\n"
                "• CoinGlass не отдаёт Hyperliquid на твоём тарифе\n"
                "• Нативный API Hyperliquid с сервера недоступен (фаервол/блокировка)\n"
                "• Временная ошибка сетевого запроса\n\n"
                "<i>Посмотри логи приложения: там должны быть строки "
                "\"Hyperliquid metaAndAssetCtxs\" или \"Hyperliquid predictedFundings\" "
                "с количеством записей.</i>"
            )
            await send_method(msg, parse_mode="HTML")
            return

        items_sorted = sorted(
            hl_items,
            key=lambda x: abs(self.annualize_rate(float(x.get("rate") or 0.0), x.get("interval", 8))),
            reverse=True,
        )

        items_per_page = 30
        total_items = len(items_sorted)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = items_sorted[start_idx:end_idx]

        context.user_data.update({
            "current_page": page,
            "total_pages": total_pages,
            "current_data_type": "hyperliquid",
        })

        response = "🌊 <b>Hyperliquid: funding (APR)</b>\n\n"
        response += f"📊 Всего записей: {total_items} | Страница {page}/{total_pages}\n"
        response += "💡 Ставки показаны как <b>годовые (APR)</b>, рассчитанные из текущей ставки за 8ч.\n\n"

        for item in page_data:
            symbol = item.get("symbol", "N/A")
            raw_rate = float(item.get("rate", 0) or 0.0)
            interval = item.get("interval", 8)
            margin_type = item.get("marginType", "USDC")
            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)

            emoji = "🟢" if raw_rate > 0 else "🔴" if raw_rate < 0 else "⚪"

            response += f"{emoji} <b>{symbol}</b> ({margin_type})\n"
            response += (
                f"  💰 {annual_str} | ⏰ интервал: {interval}ч "
                f"| ставка за интервал: {raw_rate:.6f}%\n\n"
            )

        keyboard = []
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton("◀ Назад", callback_data=f"page_hl_{page-1}")
                )
            nav_buttons.append(
                InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_hl_info")
            )
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton("Вперёд ▶", callback_data=f"page_hl_{page+1}")
                )
            keyboard.append(nav_buttons)

            if total_pages > 5:
                quick_pages = set([1, max(1, page - 2), page, min(total_pages, page + 2), total_pages])
                quick_row = []
                for p in sorted(quick_pages):
                    if p == page:
                        continue
                    quick_row.append(
                        InlineKeyboardButton(str(p), callback_data=f"page_hl_{p}")
                    )
                if quick_row:
                    keyboard.append(quick_row)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_method(response, reply_markup=reply_markup, parse_mode="HTML")

    async def show_edgex(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
        """Обёртка для отображения только биржи EdgeX через команду /edgex"""
        await self.show_exchange_funding(update, context, "EdgeX", page)

    async def show_lighter(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
        """Обёртка для отображения только биржи Lighter через команду /lighter"""
        await self.show_exchange_funding(update, context, "Lighter", page)

    async def show_exchange_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE, exchange_name: str, page: int = 1):
        """Вывод фандингов по одной бирже с пагинацией (включая Paradex, Hyperliquid, EdgeX, Lighter и др.)"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method(
                "⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.",
                parse_mode="HTML",
            )
            return

        ex_items = [
            item for item in self.funding_cache
            if isinstance(item.get("exchangeName"), str)
            and item["exchangeName"].lower() == exchange_name.lower()
            and float(item.get("rate") or 0.0) != 0.0
        ]

        if not ex_items:
            msg = (
                f"{self.get_exchange_emoji(exchange_name)} <b>{exchange_name}</b>\n\n"
                "В текущем кэше нет ни одной записи с ненулевым фандингом по этой бирже."
            )
            await send_method(msg, parse_mode="HTML")
            return

        items_sorted = sorted(
            ex_items,
            key=lambda x: abs(self.annualize_rate(float(x.get("rate") or 0.0), x.get("interval", 8))),
            reverse=True,
        )

        items_per_page = 20
        total_items = len(items_sorted)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = items_sorted[start_idx:end_idx]

        context.user_data.update({
            "current_page": page,
            "total_pages": total_pages,
            "current_data_type": "exchange",
            "current_exchange_name": exchange_name,
        })

        ex_emoji = self.get_exchange_emoji(exchange_name)
        response = f"{ex_emoji} <b>{exchange_name}: funding (APR)</b>\n\n"
        response += f"📊 Всего записей: {total_items} | Страница {page}/{total_pages}\n"
        response += "💡 Показаны только ненулевые ставки, пересчитанные в годовые (APR).\n\n"

        for item in page_data:
            symbol = item.get("symbol", "N/A")
            raw_rate = float(item.get("rate") or 0.0)
            interval = item.get("interval", 8)
            margin_type = item.get("marginType", "USDT")
            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)

            emoji = "🟢" if raw_rate > 0 else "🔴" if raw_rate < 0 else "⚪"

            response += f"{emoji} <b>{symbol}</b> ({margin_type})\n"
            response += (
                f"  💰 {annual_str} | ⏰ интервал: {interval}ч "
                f"| ставка за интервал: {raw_rate:.6f}%\n\n"
            )

        keyboard = []
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "◀ Назад",
                        callback_data=f"page_exch_{page-1}_{exchange_name}",
                    )
                )
            nav_buttons.append(
                InlineKeyboardButton(
                    f"📄 {page}/{total_pages}",
                    callback_data="page_exch_info",
                )
            )
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "Вперёд ▶",
                        callback_data=f"page_exch_{page+1}_{exchange_name}",
                    )
                )
            keyboard.append(nav_buttons)

            if total_pages > 5:
                quick_pages = set([1, max(1, page - 2), page, min(total_pages, page + 2), total_pages])
                quick_row = []
                for p in sorted(quick_pages):
                    if p == page:
                        continue
                    quick_row.append(
                        InlineKeyboardButton(
                            str(p),
                            callback_data=f"page_exch_{p}_{exchange_name}",
                        )
                    )
                if quick_row:
                    keyboard.append(quick_row)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_method(response, reply_markup=reply_markup, parse_mode="HTML")

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        try:
            if data.startswith("page_"):
                parts = data.split("_")
                if len(parts) == 3:
                    page_type = parts[1]
                    page = int(parts[2])
                    if page_type in ("negative", "positive"):
                        await self.show_funding_page(update, context, page_type, page)
                    elif page_type == "hl":
                        await self.show_hyperliquid(update, context, page)
                    elif page_type == "arb":
                        await self.show_arbitrage_bundles(update, context, page)
                elif len(parts) >= 4:
                    page_type = parts[1]
                    if page_type == "exch":
                        try:
                            page = int(parts[2])
                        except ValueError:
                            page = 1
                        exchange_name = "_".join(parts[3:])
                        await self.show_exchange_funding(update, context, exchange_name, page)
            elif data.startswith("nav_"):
                parts = data.split("_")
                nav_type = parts[1]
                if nav_type == "main":
                    await self.show_main_menu(update, context)
                elif nav_type == "negative":
                    await self.show_funding_page(update, context, "negative", 1)
                elif nav_type == "positive":
                    await self.show_funding_page(update, context, "positive", 1)
                elif nav_type == "top10":
                    await self.show_top10(update, context)
                elif nav_type == "arbitrage":
                    await self.show_arbitrage_bundles(update, context, 1)
                elif nav_type == "exchanges":
                    await self.show_exchanges(update, context)
                elif nav_type == "price_arb":
                    await self.show_price_arbitrage(update, context)
                elif nav_type == "status":
                    await self.show_status(update, context)
                elif nav_type == "hyperliquid":
                    await self.show_hyperliquid(update, context, 1)
                elif nav_type == "exch" and len(parts) >= 3:
                    exchange_name = "_".join(parts[2:])
                    await self.show_exchange_funding(update, context, exchange_name, 1)
        except Exception as e:
            logger.error("Ошибка в обработчике кнопок: %s", e)
            try:
                await query.edit_message_text(
                    "❌ <b>Произошла ошибка</b>\nПопробуйте ещё раз.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        if text.isdigit():
            page_num = int(text)
            user_data = context.user_data
            if "current_data_type" in user_data and "total_pages" in user_data:
                total_pages = user_data["total_pages"]
                data_type = user_data["current_data_type"]
                if 1 <= page_num <= total_pages:
                    if data_type in ("negative", "positive"):
                        await self.show_funding_page(update, context, data_type, page_num)
                        return
                    if data_type == "hyperliquid":
                        await self.show_hyperliquid(update, context, page_num)
                        return
                    if data_type == "arbitrage":
                        await self.show_arbitrage_bundles(update, context, page_num)
                        return
                    if data_type == "exchange":
                        exchange_name = user_data.get("current_exchange_name")
                        if exchange_name:
                            await self.show_exchange_funding(update, context, exchange_name, page_num)
                            return
                else:
                    await update.message.reply_text(
                        f"⚠️ Страница должна быть от 1 до {total_pages}"
                    )
                    return

        await update.message.reply_text(
            "ℹ️ <b>Быстрая навигация</b>\n\n"
            "Введите номер страницы для быстрого перехода\n"
            "Или используйте команды:\n"
            "/negative - отрицательные фандинги\n"
            "/positive - положительные фандинги\n"
            "/top10 - топ 10 фандингов\n"
            "/arbitrage_bundles - арбитражные связки\n"
            "/exchanges - все доступные биржи\n"
            "/hyperliquid - пары Hyperliquid\n"
            "/edgex - пары EdgeX\n"
            "/lighter - пары Lighter",
            parse_mode="HTML",
        )

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("🔴 Все отрицательные", callback_data="nav_negative_1")],
            [InlineKeyboardButton("🟢 Все положительные", callback_data="nav_positive_1")],
            [InlineKeyboardButton("🚀 Топ 10 лучших", callback_data="nav_top10")],
            [InlineKeyboardButton("⚖️ Связки арбитража", callback_data="nav_arbitrage")],
            [InlineKeyboardButton("🌊 Hyperliquid", callback_data="nav_hyperliquid")],
            [InlineKeyboardButton("🏛️ Все биржи", callback_data="nav_exchanges")],
            [InlineKeyboardButton("💰 Ценовой арбитраж", callback_data="nav_price_arb")],
            [InlineKeyboardButton("📊 Статус бота", callback_data="nav_status")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "📋 <b>Главное меню</b>\nВыберите раздел:"
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")

    def run(self):
        print("🤖 Бот запущен...")
        print("⚡ Кэширование каждые 30 секунд")
        print("📊 Мониторинг фандингов и арбитража")
        self.application.job_queue.run_repeating(
            self.update_funding_cache,
            interval=30,
            first=0,
        )
        try:
            self.application.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
        except Exception as e:
            logger.error("Ошибка при запуске бота: %s", e)
            print(f"❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    bot = CryptoArbBot()
    bot.run()
