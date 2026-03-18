#!/usr/bin/env python3
"""
snEco — МойСклад Data Sync v2
Вивантажує всі ключові дані з МойСклад API і зберігає в Excel-файли.
Запуск: python3 moysklad_sync.py

Надійність даних:
  ✅ ТОЧНІ:     demands (відвантаження), payments (оплати)
  ⚠️ НЕПОВНІ:  supply, processing, processingPlan (собівартість, виробництво)
  ✅ ДОВІРЛИВІ: counterparties, products, customerorders, salesreturn

Вимоги: pip install requests pandas openpyxl python-dotenv
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ── Конфігурація ──────────────────────────────────────────────────────────────

load_dotenv(Path(__file__).parent / ".env")

TOKEN       = os.getenv("MOYSKLAD_TOKEN")
BASE_URL    = "https://api.moysklad.ru/api/remap/1.2"

# Інкрементальний режим: тягнемо тільки останні 30 днів
_sync_from  = datetime.now() - timedelta(days=30)
DATE_FROM   = _sync_from.strftime("%Y-%m-%d 00:00:00")

OUTPUT_DIR  = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
}

# ── Утиліти ───────────────────────────────────────────────────────────────────

def fetch_all(endpoint: str, params: dict = None, date_filter: bool = True, expand: str = None) -> list:
    """Тягне всі записи з пагінацією (фільтр по moment)."""
    url = f"{BASE_URL}/{endpoint}"
    all_rows, offset, limit = [], 0, 1000
    base_params = {"limit": limit}
    if date_filter:
        base_params["filter"] = f"moment>={DATE_FROM}"
    if expand:
        base_params["expand"] = expand
    if params:
        base_params.update(params)
    while True:
        base_params["offset"] = offset
        resp = requests.get(url, headers=HEADERS, params=base_params)
        if resp.status_code != 200:
            print(f"  ⚠️  {endpoint} → HTTP {resp.status_code}: {resp.text[:200]}")
            break
        data  = resp.json()
        rows  = data.get("rows", [])
        total = data.get("meta", {}).get("size", 0)
        all_rows.extend(rows)
        offset += limit
        print(f"  {endpoint}: {min(offset, total)}/{total}")
        if offset >= total:
            break
    return all_rows


def fetch_report(endpoint: str, extra_params: dict = None) -> list:
    """Тягне звітні дані (momentFrom/momentTo)."""
    url = f"{BASE_URL}/{endpoint}"
    all_rows, offset, limit = [], 0, 1000
    base_params = {"limit": limit, "momentFrom": DATE_FROM}
    if extra_params:
        base_params.update(extra_params)
    while True:
        base_params["offset"] = offset
        resp = requests.get(url, headers=HEADERS, params=base_params)
        if resp.status_code != 200:
            print(f"  ⚠️  {endpoint} → HTTP {resp.status_code}: {resp.text[:200]}")
            break
        data  = resp.json()
        rows  = data.get("rows", [])
        total = data.get("meta", {}).get("size", 0)
        all_rows.extend(rows)
        offset += limit
        print(f"  {endpoint}: {min(offset, total)}/{total}")
        if offset >= total:
            break
    return all_rows


def safe(val, key="name"):
    if isinstance(val, dict):
        return val.get(key, "")
    return val or ""


def save_excel(df: pd.DataFrame, name: str, reliable: bool = True):
    """Upsert: мержить нові дані з існуючим файлом по колонці 'id' (якщо є)."""
    path = OUTPUT_DIR / f"{name}.xlsx"
    flag = "✅" if reliable else "⚠️ "

    if path.exists() and "id" in df.columns:
        try:
            existing = pd.read_excel(path)
            if "id" in existing.columns:
                # Видаляємо з існуючих ті рядки, що є в нових (оновлені записи)
                existing = existing[~existing["id"].isin(df["id"])]
                # Додаємо нові/оновлені рядки та сортуємо
                merged = pd.concat([existing, df], ignore_index=True)
                if "Дата" in merged.columns:
                    merged = merged.sort_values("Дата").reset_index(drop=True)
                df = merged
                print(f"  {flag} data/{name}.xlsx  ({len(df)} рядків, upsert)")
            else:
                df.to_excel(path, index=False)
                print(f"  {flag} data/{name}.xlsx  ({len(df)} рядків)")
        except Exception as e:
            print(f"  ⚠️  Не вдалось прочитати {name}.xlsx, перезаписую: {e}")
            df.to_excel(path, index=False)
            print(f"  {flag} data/{name}.xlsx  ({len(df)} рядків)")
    else:
        df.to_excel(path, index=False)
        print(f"  {flag} data/{name}.xlsx  ({len(df)} рядків)")

    df.to_excel(path, index=False)


# ── Парсери ───────────────────────────────────────────────────────────────────

def parse_demands(rows):
    records = []
    for r in rows:
        base = {
            "id":               r.get("id"),
            "Дата":             r.get("moment", "")[:10],
            "Номер":            r.get("name"),
            "Контрагент":       safe(r.get("agent")),
            "Організація":      safe(r.get("organization")),
            "Склад":            safe(r.get("store")),
            "Сума, грн":        r.get("sum", 0) / 100,
            "ПДВ, грн":         r.get("vatSum", 0) / 100,
            "Знижка, грн":      r.get("discountSum", 0) / 100,
            "Оплачено, грн":    r.get("payedSum", 0) / 100,
            "Стан":             safe(r.get("state")),
            "Проект":           safe(r.get("project")),
            "Канал збуту":      safe(r.get("salesChannel")),
            "Коментар":         r.get("description", ""),
        }
        positions = r.get("positions", {})
        pos_rows  = positions.get("rows", []) if isinstance(positions, dict) else []
        if pos_rows:
            for p in pos_rows:
                rec = base.copy()
                rec["Товар"]            = safe(p.get("assortment"))
                rec["Кількість"]        = p.get("quantity", 0)
                rec["Ціна, грн"]        = p.get("price", 0) / 100
                rec["Сума позиції, грн"]= p.get("sum", 0) / 100
                rec["Знижка %"]         = p.get("discount", 0)
                records.append(rec)
        else:
            records.append(base)
    return records


def parse_customerorders(rows):
    records = []
    for r in rows:
        base = {
            "id":                   r.get("id"),
            "Дата":                 r.get("moment", "")[:10],
            "Номер":                r.get("name"),
            "Контрагент":           safe(r.get("agent")),
            "Організація":          safe(r.get("organization")),
            "Сума, грн":            r.get("sum", 0) / 100,
            "Оплачено, грн":        r.get("payedSum", 0) / 100,
            "Відвантажено, грн":    r.get("shippedSum", 0) / 100,
            "Стан":                 safe(r.get("state")),
            "Проект":               safe(r.get("project")),
            "Канал збуту":          safe(r.get("salesChannel")),
            "Коментар":             r.get("description", ""),
        }
        positions = r.get("positions", {})
        pos_rows  = positions.get("rows", []) if isinstance(positions, dict) else []
        if pos_rows:
            for p in pos_rows:
                rec = base.copy()
                rec["Товар"]     = safe(p.get("assortment"))
                rec["Кількість"] = p.get("quantity", 0)
                rec["Ціна, грн"] = p.get("price", 0) / 100
                records.append(rec)
        else:
            records.append(base)
    return records


def parse_salesreturns(rows):
    records = []
    for r in rows:
        base = {
            "id":           r.get("id"),
            "Дата":         r.get("moment", "")[:10],
            "Номер":        r.get("name"),
            "Контрагент":   safe(r.get("agent")),
            "Склад":        safe(r.get("store")),
            "Сума, грн":    r.get("sum", 0) / 100,
            "Стан":         safe(r.get("state")),
            "Коментар":     r.get("description", ""),
        }
        positions = r.get("positions", {})
        pos_rows  = positions.get("rows", []) if isinstance(positions, dict) else []
        if pos_rows:
            for p in pos_rows:
                rec = base.copy()
                rec["Товар"]     = safe(p.get("assortment"))
                rec["Кількість"] = p.get("quantity", 0)
                rec["Ціна, грн"] = p.get("price", 0) / 100
                records.append(rec)
        else:
            records.append(base)
    return records


def parse_counterparties(rows):
    return [{
        "id":                   r.get("id"),
        "Назва":                r.get("name"),
        "Тип":                  r.get("companyType"),
        "Код":                  r.get("code"),
        "ЄДРПОУ/ІНН":          r.get("inn"),
        "Телефон":              r.get("phone"),
        "Email":                r.get("email"),
        "Теги":                 ", ".join(r.get("tags", [])),
        "Баланс, грн":          r.get("balance", 0) / 100 if r.get("balance") else 0,
        "Борг прострочений":    r.get("overdueDebt", 0) / 100 if r.get("overdueDebt") else 0,
        "Статус":               safe(r.get("state")),
        "Коментар":             r.get("description", ""),
    } for r in rows]


def parse_products(rows):
    return [{
        "id":               r.get("id"),
        "Назва":            r.get("name"),
        "Код":              r.get("code"),
        "Артикул":          r.get("article"),
        "Штрихкод":         ", ".join([b.get("ean13","") for b in r.get("barcodes",[]) if "ean13" in b]),
        "Група":            safe(r.get("productFolder")),
        "Одиниця":          safe(r.get("uom")),
        "Мін. залишок":     r.get("minimumBalance", 0),
        "Ціна продажу":     r.get("salePrices",[{}])[0].get("value",0)/100 if r.get("salePrices") else 0,
        "Ціна закупки":     r.get("buyPrice",{}).get("value",0)/100 if r.get("buyPrice") else 0,
        "Опис":             r.get("description",""),
        "Архів":            r.get("archived", False),
    } for r in rows]


def parse_productfolders(rows):
    return [{
        "id":       r.get("id"),
        "Назва":    r.get("name"),
        "Код":      r.get("code"),
        "Батьківська": safe(r.get("productFolder")),
    } for r in rows]


def categorize_product(name: str) -> str:
    """Визначає категорію товару за ключовими словами у назві."""
    n = (name or "").lower()
    # Сировина — свіжий сир
    if any(k in n for k in ["свежий сыр", "свіжий сир", "свежий сир", "cвежий сыр",
                             "fresh cheese", "сировина"]):
        return "Сировина"
    # Упаковка
    if any(k in n for k in ["упаковка", "стікер", "sticker", "короб", "showbox",
                             "гофро", "скотч", "плівка", "пакет", "етикетка",
                             "label", "box", "пачка"]):
        return "Упаковка"
    # Готова продукція — сушений сир snEco
    if any(k in n for k in ["сир сушений", "dried cheese", "хрусткий сир",
                             "sneco", "snEco".lower()]):
        return "Готова продукція"
    return "Інше"


def parse_stock(rows):
    return [{
        "Товар":        r.get("name"),
        "Код":          r.get("code"),
        "Артикул":      r.get("article"),
        "Склад":        safe(r.get("store")),
        "Категорія":    categorize_product(r.get("name", "")),
        "Залишок":      r.get("stock", 0),
        "Резерв":       r.get("reserve", 0),
        "Очікується":   r.get("inTransit", 0),
        "Доступно":     r.get("quantity", 0),
        "Ціна, грн":    r.get("price", 0) / 100,
        "Вартість, грн": round(r.get("stock", 0) * (r.get("price", 0) / 100), 2),
        "Сума, грн":    r.get("stockSum", 0) / 100,
    } for r in rows]


def parse_payments(rows, ptype):
    return [{
        "id":           r.get("id"),
        "Тип":          ptype,
        "Дата":         r.get("moment", "")[:10],
        "Номер":        r.get("name"),
        "Контрагент":   safe(r.get("agent")),
        "Сума, грн":    r.get("sum", 0) / 100,
        "Призначення":  r.get("paymentPurpose", ""),
        "Проект":       safe(r.get("project")),
    } for r in rows]


def parse_invoicesout(rows):
    return [{
        "id":           r.get("id"),
        "Дата":         r.get("moment", "")[:10],
        "Номер":        r.get("name"),
        "Контрагент":   safe(r.get("agent")),
        "Сума, грн":    r.get("sum", 0) / 100,
        "Оплачено, грн":r.get("payedSum", 0) / 100,
        "Стан":         safe(r.get("state")),
    } for r in rows]


def parse_supply(rows):  # ⚠️ дані можуть бути неповними
    records = []
    for r in rows:
        base = {
            "id":           r.get("id"),
            "Дата":         r.get("moment", "")[:10],
            "Номер":        r.get("name"),
            "Постачальник": safe(r.get("agent")),
            "Склад":        safe(r.get("store")),
            "Сума, грн":    r.get("sum", 0) / 100,
            "Стан":         safe(r.get("state")),
        }
        positions = r.get("positions", {})
        pos_rows  = positions.get("rows", []) if isinstance(positions, dict) else []
        if pos_rows:
            for p in pos_rows:
                rec = base.copy()
                rec["Товар"]     = safe(p.get("assortment"))
                rec["Кількість"] = p.get("quantity", 0)
                rec["Ціна, грн"] = p.get("price", 0) / 100
                records.append(rec)
        else:
            records.append(base)
    return records


def parse_processing(rows):  # ⚠️ дані можуть бути неповними
    return [{
        "id":           r.get("id"),
        "Дата":         r.get("moment", "")[:10],
        "Номер":        r.get("name"),
        "Техкарта":     safe(r.get("processingPlan")),
        "Організація":  safe(r.get("organization")),
        "Склад (матеріали)": safe(r.get("materialsStore")),
        "Склад (продукція)": safe(r.get("productsStore")),
        "Кількість":    r.get("quantity", 0),
        "Стан":         safe(r.get("state")),
        "Коментар":     r.get("description", ""),
    } for r in rows]


def parse_processingplans(rows):  # ⚠️ дані можуть бути неповними
    return [{
        "id":           r.get("id"),
        "Назва":        r.get("name"),
        "Код":          r.get("code"),
        "Продукт":      safe(r.get("product")),
    } for r in rows]


def parse_moves(rows):
    records = []
    for r in rows:
        base = {
            "id":           r.get("id"),
            "Дата":         r.get("moment", "")[:10],
            "Номер":        r.get("name"),
            "Зі складу":    safe(r.get("sourceStore")),
            "На склад":     safe(r.get("targetStore")),
            "Сума, грн":    r.get("sum", 0) / 100,
        }
        positions = r.get("positions", {})
        pos_rows  = positions.get("rows", []) if isinstance(positions, dict) else []
        if pos_rows:
            for p in pos_rows:
                rec = base.copy()
                rec["Товар"]     = safe(p.get("assortment"))
                rec["Кількість"] = p.get("quantity", 0)
                records.append(rec)
        else:
            records.append(base)
    return records


def parse_profit_report(rows, group_by: str):
    # МойСклад повертає назву в полі "assortment" (товари) або "counterparty" (контрагенти)
    api_key = {"Товар": "assortment", "Контрагент": "counterparty"}.get(group_by, group_by.lower())
    records = []
    for r in rows:
        entity = r.get(api_key) or {}
        name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        revenue  = r.get("sellSum", 0) / 100
        # sellCostSum — собівартість продажів (правильне поле МойСклад)
        cost     = r.get("sellCostSum", 0) / 100
        # grossProfit = виручка − собівартість (якщо API повертає)
        gp_raw   = r.get("grossProfit", 0) / 100
        # markup (наценка) в частках (1.3888 = 138.88%)
        markup_f = r.get("margin", 0)   # МойСклад повертає markup як десятковий дріб
        markup_pct = round(markup_f * 100, 2) if markup_f else 0

        # Прибуток: беремо grossProfit якщо є; інакше — виручка − собівартість;
        # якщо собівартість теж 0 — обчислюємо з markup:
        # profit = revenue × markup / (1 + markup)   [де markup у частках]
        if gp_raw != 0:
            profit = gp_raw
        elif cost != 0:
            profit = round(revenue - cost, 2)
        elif markup_f and markup_f > 0:
            profit = round(revenue * markup_f / (1 + markup_f), 2)
        else:
            profit = 0

        records.append({
            group_by:               name,
            "Продано, шт":          r.get("sellQuantity", 0),
            "Виручка, грн":         revenue,
            "Собівартість, грн":    cost,
            "Прибуток, грн":        profit,
            "Маржа %":              markup_pct,
            "Повернень, шт":        r.get("returnQuantity", 0),
            "Сума повернень, грн":  r.get("returnSum", 0) / 100,
        })
    return records


# ── Головна функція ───────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  snEco — МойСклад Sync v2")
    print(f"  Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"  Від: {DATE_FROM}")
    print(f"{'='*55}")
    print(f"  ✅ = точні дані  |  ⚠️  = можуть бути неповними")
    print(f"{'='*55}\n")

    if not TOKEN:
        print("❌ Токен не знайдено! Перевір файл .env")
        return

    # ── ✅ ТОЧНІ ДАНІ ─────────────────────────────────────

    print("✅ ТОЧНІ ДАНІ\n" + "-"*40)

    print("\n📦 Відвантаження...")
    rows = fetch_all("entity/demand", expand="agent,store,organization,state")
    save_excel(pd.DataFrame(parse_demands(rows)), "demands", reliable=True)

    print("\n💳 Оплати вхідні...")
    rows_in = fetch_all("entity/paymentin", expand="agent,state")
    print("💳 Оплати вихідні...")
    rows_out = fetch_all("entity/paymentout", expand="agent,state")
    records = parse_payments(rows_in, "Вхідний") + parse_payments(rows_out, "Вихідний")
    save_excel(pd.DataFrame(records), "payments", reliable=True)

    print("\n🛒 Замовлення покупців...")
    rows = fetch_all("entity/customerorder", expand="agent,state")
    save_excel(pd.DataFrame(parse_customerorders(rows)), "customer_orders", reliable=True)

    print("\n↩️  Повернення від покупців...")
    rows = fetch_all("entity/salesreturn", expand="agent,store,state")
    save_excel(pd.DataFrame(parse_salesreturns(rows)), "sales_returns", reliable=True)

    print("\n👥 Контрагенти...")
    rows = fetch_all("entity/counterparty", date_filter=False)
    save_excel(pd.DataFrame(parse_counterparties(rows)), "counterparties", reliable=True)

    print("\n🏷️  Товари...")
    rows = fetch_all("entity/product", date_filter=False)
    save_excel(pd.DataFrame(parse_products(rows)), "products", reliable=True)

    print("\n📁 Групи товарів...")
    rows = fetch_all("entity/productfolder", date_filter=False)
    save_excel(pd.DataFrame(parse_productfolders(rows)), "product_folders", reliable=True)

    print("\n🧾 Рахунки покупцям...")
    rows = fetch_all("entity/invoiceout")
    save_excel(pd.DataFrame(parse_invoicesout(rows)), "invoices_out", reliable=True)

    print("\n📊 Залишки (поточні)...")
    all_rows, offset = [], 0
    while True:
        resp = requests.get(f"{BASE_URL}/report/stock/all", headers=HEADERS,
                            params={"limit": 1000, "offset": offset})
        if resp.status_code != 200:
            print(f"  ⚠️ {resp.status_code}")
            break
        data = resp.json()
        rows = data.get("rows", [])
        all_rows.extend(rows)
        total = data.get("meta", {}).get("size", 0)
        offset += 1000
        print(f"  stock: {min(offset, total)}/{total}")
        if offset >= total:
            break
    stock_df = pd.DataFrame(parse_stock(all_rows))

    # ── Виключаємо технічні позиції (Маркетинг та подібні) ───
    EXCLUDE_STOCK = ['маркетинг']
    mask_exclude = stock_df['Товар'].str.lower().str.strip().apply(
        lambda n: any(ex in (n or '') for ex in EXCLUDE_STOCK)
    )
    if mask_exclude.any():
        print(f"  🚫 Виключено з складу: {list(stock_df.loc[mask_exclude, 'Товар'])}")
        stock_df = stock_df[~mask_exclude].reset_index(drop=True)

    # ── Підтягуємо Мін. залишок з products ───────────────
    prod_path = OUTPUT_DIR / "products.xlsx"
    if prod_path.exists():
        try:
            prod_df = pd.read_excel(prod_path)[['Назва', 'Мін. залишок']]
            prod_df = prod_df[prod_df['Мін. залишок'] > 0]
            stock_df = stock_df.merge(prod_df, left_on='Товар', right_on='Назва', how='left')
            stock_df['Мін. залишок'] = stock_df['Мін. залишок'].fillna(0)
            stock_df.drop(columns=['Назва'], errors='ignore', inplace=True)
        except Exception as e:
            print(f"  ⚠️  Не вдалося приєднати products: {e}")
            stock_df['Мін. залишок'] = 0
    else:
        stock_df['Мін. залишок'] = 0

    save_excel(stock_df, "stock", reliable=True)

    # ── Щоденний snapshot складу ──────────────────────────
    today_str = datetime.now().strftime("%Y-%m-%d")
    snapshot_path = OUTPUT_DIR / f"stock_{today_str}.xlsx"
    if not snapshot_path.exists():
        save_excel(stock_df, f"stock_{today_str}", reliable=False)
        print(f"  📸 Snapshot збережено: stock_{today_str}.xlsx")
    else:
        print(f"  ✅ Snapshot вже є: stock_{today_str}.xlsx")

    # ── ⚠️ НЕПОВНІ ДАНІ ───────────────────────────────────

    print("\n\n⚠️  ДАНІ (можуть бути неповними)\n" + "-"*40)

    print("\n🚚 Переміщення між складами...")
    rows = fetch_all("entity/move")
    save_excel(pd.DataFrame(parse_moves(rows)), "moves", reliable=False)

    print("\n📥 Надходження від постачальників...")
    rows = fetch_all("entity/supply", expand="agent,store,state")
    save_excel(pd.DataFrame(parse_supply(rows)), "supply", reliable=False)

    print("\n🏭 Виробничі замовлення...")
    rows = fetch_all("entity/processingorder", expand="processingPlan,materialsStore,productsStore,state")
    save_excel(pd.DataFrame(parse_processing(rows)), "production_orders", reliable=False)

    print("\n🏭 Виробництво (виконані)...")
    rows = fetch_all("entity/processing", expand="processingPlan,materialsStore,productsStore,state")
    save_excel(pd.DataFrame(parse_processing(rows)), "production_done", reliable=False)

    print("\n📋 Технологічні карти...")
    rows = fetch_all("entity/processingplan", date_filter=False, expand="materials,products")
    save_excel(pd.DataFrame(parse_processingplans(rows)), "processing_plans", reliable=False)

    print("\n📈 Звіт: прибутковість по товарах...")
    rows = fetch_report("report/profit/byproduct")
    save_excel(pd.DataFrame(parse_profit_report(rows, "Товар")), "report_profit_by_product", reliable=False)

    print("\n📈 Звіт: прибутковість по контрагентах...")
    rows = fetch_report("report/profit/bycounterparty")
    save_excel(pd.DataFrame(parse_profit_report(rows, "Контрагент")), "report_profit_by_counterparty", reliable=False)

    # ── Річні, квартальні та місячні звіти для фільтрації ─────
    import calendar as _cal
    now = datetime.now()
    current_year = now.year

    QUARTER_RANGES = {
        'Q1': ('01-01', '03-31'),
        'Q2': ('04-01', '06-30'),
        'Q3': ('07-01', '09-30'),
        'Q4': ('10-01', '12-31'),
    }

    def _profit_fetch_and_save(mf, mt, cp_name, prod_name, label):
        print(f"  📈 {label} — контрагенти...")
        rows = fetch_report("report/profit/bycounterparty",
                            extra_params={"momentFrom": mf, "momentTo": mt})
        if rows:
            save_excel(pd.DataFrame(parse_profit_report(rows, "Контрагент")),
                       cp_name, reliable=False)
        print(f"  📈 {label} — товари...")
        rows = fetch_report("report/profit/byproduct",
                            extra_params={"momentFrom": mf, "momentTo": mt})
        if rows:
            save_excel(pd.DataFrame(parse_profit_report(rows, "Товар")),
                       prod_name, reliable=False)

    for year in range(2023, current_year + 1):
        # Річний
        print(f"\n📅 Річні звіти {year}...")
        _profit_fetch_and_save(
            f"{year}-01-01 00:00:00", f"{year}-12-31 23:59:59",
            f"report_profit_cp_{year}", f"report_profit_prod_{year}",
            f"Рік {year}")

        current_q = (now.month - 1) // 3 + 1

        # Квартальні
        for q, (qs, qe) in QUARTER_RANGES.items():
            q_num = int(q[1])
            q_start_month = (q_num - 1) * 3 + 1
            # Пропускаємо майбутні квартали
            if year == current_year and q_start_month > now.month:
                continue
            cp_path  = OUTPUT_DIR / f"report_profit_cp_{year}_{q}.xlsx"
            prod_path = OUTPUT_DIR / f"report_profit_prod_{year}_{q}.xlsx"
            # Кешуємо минулі квартали — не перезавантажуємо якщо файл вже є
            is_current_q = (year == current_year and q_num == current_q)
            if not is_current_q and cp_path.exists() and prod_path.exists():
                print(f"  ✅ {year} {q} — кеш")
                continue
            _profit_fetch_and_save(
                f"{year}-{qs} 00:00:00", f"{year}-{qe} 23:59:59",
                f"report_profit_cp_{year}_{q}", f"report_profit_prod_{year}_{q}",
                f"{year} {q}")

        # Місячні
        for month in range(1, 13):
            if year == current_year and month > now.month:
                break
            cp_m_path   = OUTPUT_DIR / f"report_profit_cp_{year}_{month:02d}.xlsx"
            prod_m_path = OUTPUT_DIR / f"report_profit_prod_{year}_{month:02d}.xlsx"
            is_current_m = (year == current_year and month == now.month)
            if not is_current_m and cp_m_path.exists() and prod_m_path.exists():
                print(f"  ✅ {year}/{month:02d} — кеш")
                continue
            last_day = _cal.monthrange(year, month)[1]
            _profit_fetch_and_save(
                f"{year}-{month:02d}-01 00:00:00",
                f"{year}-{month:02d}-{last_day} 23:59:59",
                f"report_profit_cp_{year}_{month:02d}",
                f"report_profit_prod_{year}_{month:02d}",
                f"{year}/{month:02d}")

    # ── Генерація дашборду ────────────────────────────────
    print("\n🎨 Генерую dashboard.html...")
    try:
        generate_dashboard()
        print("  ✅ dashboard.html оновлено")
    except Exception as e:
        print(f"  ⚠️  Помилка генерації дашборду: {e}")

    # ── Git auto-push відключено — пуш робиш вручну через GitHub Desktop
    # git_push()

    # ── Підсумок ──────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  ✅ Синхронізацію завершено!")
    print(f"  📁 Файли збережено в: snEco/data/")
    print(f"  🌐 GitHub: https://github.com/dreamcarua/sneco")
    print(f"\n  Час: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"{'='*55}\n")


# ── Dashboard generator ───────────────────────────────────────────────────────

def generate_dashboard():
    """Генерує dashboard.html з актуальних даних."""
    import re as _re

    pay_path  = OUTPUT_DIR / "payments.xlsx"
    dem_path  = OUTPUT_DIR / "demands.xlsx"
    stk_path  = OUTPUT_DIR / "stock.xlsx"

    if not pay_path.exists() or not dem_path.exists():
        raise FileNotFoundError("Файли payments.xlsx або demands.xlsx не знайдено")

    pay = pd.read_excel(pay_path)
    dem = pd.read_excel(dem_path)
    stk = pd.read_excel(stk_path) if stk_path.exists() else pd.DataFrame()

    for df in [pay, dem]:
        df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')
        df['YM']   = df['Дата'].dt.strftime('%Y-%m')
        df['Рік']  = df['Дата'].dt.year
        df['М']    = df['Дата'].dt.month
        df['Q']    = df['Дата'].dt.to_period('Q').astype(str)

    inc = pay[pay['Тип'] == 'Вхідний'].copy()
    out = pay[pay['Тип'] == 'Вихідний'].copy()
    ret = out[out['Призначення'].str.contains('Возврат', na=False)].copy()
    exp = out[~out['Призначення'].str.contains('Возврат', na=False)].copy()

    def gs(s, k, d=0):
        try: return round(float(s.get(k, d)))
        except: return d

    inc_m = inc.groupby('YM')['Сума, грн'].sum()
    out_m = out.groupby('YM')['Сума, грн'].sum()
    ret_m = ret.groupby('YM')['Сума, грн'].sum()
    exp_m = exp.groupby('YM')['Сума, грн'].sum()
    dem_m = dem.groupby('YM')['Сума, грн'].sum()
    cnt_m = dem.groupby('YM')['id'].count()
    avg_m = dem.groupby('YM')['Сума, грн'].mean()

    all_ym = sorted(set(inc['YM'].dropna()) | set(dem['YM'].dropna()))
    monthly = []
    for ym in all_ym:
        i = gs(inc_m, ym)
        monthly.append({'ym': ym, 'year': ym[:4], 'month': int(ym[5:]),
            'income': i, 'outgoing': gs(out_m, ym), 'returns': gs(ret_m, ym),
            'expenses': gs(exp_m, ym), 'net': i - gs(out_m, ym),
            'shipments': gs(dem_m, ym), 'orders': int(gs(cnt_m, ym)),
            'avg_order': round(float(avg_m.get(ym, 0))),
        })

    annual = []
    for y in [2023, 2024, 2025, 2026]:
        yi = round(inc[inc['Рік']==y]['Сума, грн'].sum())
        yo = round(exp[exp['Рік']==y]['Сума, грн'].sum())
        yr = round(ret[ret['Рік']==y]['Сума, грн'].sum())
        yd = round(dem[dem['Рік']==y]['Сума, грн'].sum())
        yc = int(dem[dem['Рік']==y]['id'].count())
        ya = round(float(dem[dem['Рік']==y]['Сума, грн'].mean())) if yc else 0
        prev = next((a['income'] for a in annual if a['year'] == y-1), None)
        yoy = round((yi-prev)/prev*100, 1) if prev and prev > 0 else None
        annual.append({'year': y, 'income': yi, 'expenses': yo, 'returns': yr,
            'shipments': yd, 'orders': yc, 'avg_order': ya, 'yoy': yoy, 'partial': y == 2026})

    all_q = sorted(set(inc['Q'].dropna()) | set(dem['Q'].dropna()))
    inc_q = inc.groupby('Q')['Сума, грн'].sum()
    exp_q = exp.groupby('Q')['Сума, грн'].sum()
    ret_q = ret.groupby('Q')['Сума, грн'].sum()
    dem_q = dem.groupby('Q')['Сума, грн'].sum()
    cnt_q = dem.groupby('Q')['id'].count()
    quarterly = [{'q': q, 'income': gs(inc_q, q), 'expenses': gs(exp_q, q),
        'returns': gs(ret_q, q), 'shipments': gs(dem_q, q), 'orders': int(gs(cnt_q, q))}
        for q in all_q]

    inc35 = inc[inc['Рік'].isin([2023, 2024, 2025])]
    dem35 = dem[dem['Рік'].isin([2023, 2024, 2025])]
    seasonality = [{'month': m,
        'avg_income': round(float(inc35[inc35['М']==m]['Сума, грн'].mean() or 0)),
        'avg_shipments': round(float(dem35[dem35['М']==m]['Сума, грн'].mean() or 0))}
        for m in range(1, 13)]

    buckets = [0, 500, 1000, 2000, 5000, 10000, 25000, 50000, 1e9]
    labels_b = ['0–500', '500–1К', '1К–2К', '2К–5К', '5К–10К', '10К–25К', '25К–50К', '50К+']
    hist = [{'label': labels_b[i],
        'count': int(((inc['Сума, грн'] >= buckets[i]) & (inc['Сума, грн'] < buckets[i+1])).sum())}
        for i in range(len(buckets)-1)]

    # ── Аналітика складу ──────────────────────────────────────────────────────
    stock_data      = []   # повна таблиця (для старого SKU-widget)
    stock_detail    = {}   # розширений блок для procurement dashboard
    stock_history   = []   # динаміка залишків з щоденних snapshot

    if not stk.empty:
        s = stk.copy()
        # Вартість, якщо колонки немає (старі дані)
        if 'Вартість, грн' not in s.columns:
            s['Вартість, грн'] = s['Залишок'] * s.get('Ціна, грн', 0)
        if 'Категорія' not in s.columns:
            s['Категорія'] = s['Товар'].apply(categorize_product)
        if 'Мін. залишок' not in s.columns:
            s['Мін. залишок'] = 0

        # ── Повна таблиця ─────────────────────────────────
        for _, r in s[s['Залишок'] > 0].sort_values('Вартість, грн', ascending=False).iterrows():
            stock_data.append({
                'name':      r['Товар'],
                'category':  r.get('Категорія', 'Інше'),
                'stock':     round(float(r['Залишок']), 2),
                'reserve':   round(float(r['Резерв']), 2),
                'inTransit': round(float(r.get('Очікується', 0)), 2),
                'available': round(float(r['Доступно']), 2),
                'price':     round(float(r.get('Ціна, грн', 0)), 2),
                'value':     round(float(r['Вартість, грн']), 2),
                'minStock':  round(float(r.get('Мін. залишок', 0)), 2),
            })

        total_value = round(s['Вартість, грн'].sum())

        # ── KPI по категоріях ──────────────────────────────
        cat_stats = {}
        for cat, grp in s.groupby('Категорія'):
            cat_stats[cat] = {
                'value': round(grp['Вартість, грн'].sum()),
                'items': int((grp['Залишок'] > 0).sum()),
            }

        # ── Критичні позиції (доступно < мін. залишок) ────
        critical = []
        has_min = s[s['Мін. залишок'] > 0]
        for _, r in has_min.iterrows():
            if r['Доступно'] < r['Мін. залишок']:
                critical.append({
                    'name':      r['Товар'],
                    'category':  r.get('Категорія', 'Інше'),
                    'available': round(float(r['Доступно']), 2),
                    'minStock':  round(float(r['Мін. залишок']), 2),
                    'inTransit': round(float(r.get('Очікується', 0)), 2),
                    'deficit':   round(float(r['Мін. залишок'] - r['Доступно']), 2),
                })
        critical.sort(key=lambda x: x['deficit'], reverse=True)

        # ── Позиції в дорозі ──────────────────────────────
        in_transit = []
        for _, r in s[s.get('Очікується', s.get('Очікується', pd.Series(0, index=s.index))) > 0].iterrows():
            in_transit.append({
                'name':      r['Товар'],
                'category':  r.get('Категорія', 'Інше'),
                'inTransit': round(float(r.get('Очікується', 0)), 2),
                'available': round(float(r['Доступно']), 2),
                'stock':     round(float(r['Залишок']), 2),
            })

        # ── ABC-аналіз по вартості ────────────────────────
        abc_df = s[s['Вартість, грн'] > 0].sort_values('Вартість, грн', ascending=False).copy()
        abc_df['cum_share'] = abc_df['Вартість, грн'].cumsum() / abc_df['Вартість, грн'].sum()
        def abc_class(share):
            if share <= 0.80: return 'A'
            if share <= 0.95: return 'B'
            return 'C'
        abc_df['ABC'] = abc_df['cum_share'].apply(abc_class)
        abc_list = []
        for _, r in abc_df.iterrows():
            abc_list.append({
                'name':     r['Товар'],
                'category': r.get('Категорія', 'Інше'),
                'value':    round(float(r['Вартість, грп'] if 'Вартість, грп' in r else r['Вартість, грн']), 2),
                'stock':    round(float(r['Залишок']), 2),
                'abc':      r['ABC'],
                'cumShare': round(float(r['cum_share']) * 100, 1),
            })

        # ── ABC summary ───────────────────────────────────
        abc_summary = {}
        for cls in ['A', 'B', 'C']:
            grp = abc_df[abc_df['ABC'] == cls]
            abc_summary[cls] = {
                'items': len(grp),
                'value': round(grp['Вартість, грн'].sum()),
                'share': round(grp['Вартість, грн'].sum() / total_value * 100, 1) if total_value else 0,
            }

        stock_detail = {
            'total_value':  total_value,
            'total_items':  int((s['Залишок'] > 0).sum()),
            'critical_cnt': len(critical),
            'in_transit_cnt': len(in_transit),
            'categories':   cat_stats,
            'critical':     critical,
            'in_transit':   in_transit,
            'abc':          abc_list,
            'abc_summary':  abc_summary,
            'updated':      datetime.now().strftime('%d.%m.%Y %H:%M'),
        }

    # ── Динаміка залишків з щоденних snapshot ─────────────────────────────────
    import glob as _glob
    snap_files = sorted(_glob.glob(str(OUTPUT_DIR / "stock_20*.xlsx")))
    key_items = ['Сир сушений snEco "Чеддер", 28г',
                 'Сир сушений snEco "Гауда", 28г',
                 'Свежий Сыр Гауда Голландия',
                 'Упаковка snEco «Cheddar», 28г']
    history_dict = {item: [] for item in key_items}
    dates_seen = []
    for snap_path in snap_files[-30:]:   # останні 30 днів
        snap_date = snap_path.split('stock_')[-1].replace('.xlsx', '')
        try:
            snap_df = pd.read_excel(snap_path)
            dates_seen.append(snap_date)
            for item in key_items:
                row = snap_df[snap_df['Товар'] == item]
                val = round(float(row['Залишок'].iloc[0]), 2) if not row.empty else None
                history_dict[item].append(val)
        except Exception:
            pass
    if dates_seen:
        stock_history = {
            'dates': dates_seen,
            'series': [{'name': k, 'data': v} for k, v in history_dict.items()],
        }

    # ── Топ контрагентів (з profit-звіту — найточніші дані) ──────────────────
    top_clients = []
    cp_report_path = OUTPUT_DIR / "report_profit_by_counterparty.xlsx"
    if cp_report_path.exists():
        cp_df = pd.read_excel(cp_report_path)
        cp_df = cp_df[cp_df['Контрагент'].notna() & (cp_df['Виручка, грн'] > 0)]
        cp_df = cp_df.sort_values('Виручка, грн', ascending=False).head(50)
        for _, r in cp_df.iterrows():
            top_clients.append({
                'name':    str(r['Контрагент']),
                'revenue': round(float(r['Виручка, грн'])),
                'profit':  round(float(r.get('Прибуток, грн') or 0)),
                'qty':     int(r.get('Продано, шт') or 0),
                'margin':  round(float(r.get('Маржа %') or 0), 1),
                'returns': round(float(r.get('Сума повернень, грн') or 0)),
            })

    # ── Топ товарів ───────────────────────────────────────────────────────────
    top_products = []
    prod_report_path = OUTPUT_DIR / "report_profit_by_product.xlsx"
    if prod_report_path.exists():
        prod_df = pd.read_excel(prod_report_path)
        prod_df = prod_df[prod_df['Товар'].notna() & (prod_df['Виручка, грн'] > 0)]
        prod_df = prod_df.sort_values('Виручка, грн', ascending=False).head(50)
        for _, r in prod_df.iterrows():
            top_products.append({
                'name':    str(r['Товар']),
                'revenue': round(float(r['Виручка, грн'])),
                'profit':  round(float(r.get('Прибуток, грн') or 0)),
                'qty':     int(r.get('Продано, шт') or 0),
                'margin':  round(float(r.get('Маржа %') or 0), 1),
                'returns': round(float(r.get('Сума повернень, грн') or 0)),
            })

    # ── Per-year / quarter / month breakdowns for filtered analytics ─────────
    clients_by_year: dict    = {}
    products_by_year: dict   = {}
    clients_by_quarter: dict = {}
    products_by_quarter: dict = {}
    clients_by_month: dict   = {}
    products_by_month: dict  = {}

    def _read_profit_file(path, name_col):
        """Читає profit-файл, повертає список dict або []."""
        try:
            df = pd.read_excel(path)
            df = df[df[name_col].notna() & (df['Виручка, грн'] > 0)]
            df = df.sort_values('Виручка, грн', ascending=False)
            return [
                {'name':    str(r[name_col]),
                 'revenue': round(float(r['Виручка, грн'])),
                 'profit':  round(float(r.get('Прибуток, грн') or 0)),
                 'qty':     int(r.get('Продано, шт') or 0),
                 'margin':  round(float(r.get('Маржа %') or 0), 1),
                 'returns': round(float(r.get('Сума повернень, грн') or 0))}
                for _, r in df.iterrows()
            ]
        except Exception:
            return []

    QUARTERS = ['Q1', 'Q2', 'Q3', 'Q4']

    for year in range(2023, datetime.now().year + 1):
        # Річний
        cp_y   = OUTPUT_DIR / f"report_profit_cp_{year}.xlsx"
        prod_y = OUTPUT_DIR / f"report_profit_prod_{year}.xlsx"
        if cp_y.exists():
            clients_by_year[str(year)]  = _read_profit_file(cp_y,   'Контрагент')
        if prod_y.exists():
            products_by_year[str(year)] = _read_profit_file(prod_y, 'Товар')

        # Квартальні
        for q in QUARTERS:
            cp_qf   = OUTPUT_DIR / f"report_profit_cp_{year}_{q}.xlsx"
            prod_qf = OUTPUT_DIR / f"report_profit_prod_{year}_{q}.xlsx"
            key = f"{year}_{q}"
            if cp_qf.exists():
                clients_by_quarter[key]  = _read_profit_file(cp_qf,   'Контрагент')
            if prod_qf.exists():
                products_by_quarter[key] = _read_profit_file(prod_qf, 'Товар')

        # Місячні
        for month in range(1, 13):
            cp_mf   = OUTPUT_DIR / f"report_profit_cp_{year}_{month:02d}.xlsx"
            prod_mf = OUTPUT_DIR / f"report_profit_prod_{year}_{month:02d}.xlsx"
            key = f"{year}_{month:02d}"
            if cp_mf.exists():
                clients_by_month[key]  = _read_profit_file(cp_mf,   'Контрагент')
            if prod_mf.exists():
                products_by_month[key] = _read_profit_file(prod_mf, 'Товар')

    # ── Якщо є річні дані — будуємо all-time top_clients/top_products з них ──
    # (звіт за останні 30 днів не репрезентує весь час!)
    def _aggregate_by_year(by_year: dict) -> list:
        agg: dict = {}
        for year_data in by_year.values():
            for item in year_data:
                n = item['name']
                if n not in agg:
                    agg[n] = {'name': n, 'revenue': 0, 'profit': 0, 'qty': 0,
                               'margin_sum': 0.0, 'returns': 0, '_cnt': 0}
                agg[n]['revenue']     += item.get('revenue', 0)
                agg[n]['profit']      += item.get('profit', 0)
                agg[n]['qty']         += item.get('qty', 0)
                agg[n]['returns']     += item.get('returns', 0)
                agg[n]['margin_sum']  += item.get('margin', 0.0)
                agg[n]['_cnt']        += 1
        result = []
        for item in sorted(agg.values(), key=lambda x: x['revenue'], reverse=True)[:50]:
            result.append({
                'name':    item['name'],
                'revenue': item['revenue'],
                'profit':  item['profit'],
                'qty':     item['qty'],
                'margin':  round(item['margin_sum'] / item['_cnt'], 1) if item['_cnt'] else 0,
                'returns': item['returns'],
            })
        return result

    if clients_by_year:
        top_clients = _aggregate_by_year(clients_by_year)
    if products_by_year:
        top_products = _aggregate_by_year(products_by_year)

    # ── Daily data: last 30 / prev 30 / same period last year ────────────────
    try:
        inc_daily = inc.copy()
        inc_daily['Date'] = inc_daily['Дата'].dt.normalize()
        today_d = pd.Timestamp.now().normalize()
        d30_start   = today_d - pd.Timedelta(days=29)
        prev30_end   = d30_start - pd.Timedelta(days=1)
        prev30_start = prev30_end - pd.Timedelta(days=29)
        ly30_end     = today_d - pd.Timedelta(days=365)
        ly30_start   = d30_start - pd.Timedelta(days=365)

        def _daily_agg(start, end):
            mask = (inc_daily['Date'] >= start) & (inc_daily['Date'] <= end)
            grp = inc_daily[mask].groupby('Date')['Сума, грн'].sum()
            idx = pd.date_range(start, end)
            grp = grp.reindex(idx, fill_value=0)
            return [round(float(v)) for v in grp.values]

        daily_30 = {
            'labels':     [d.strftime('%m-%d') for d in pd.date_range(d30_start, today_d)],
            'curr':       _daily_agg(d30_start, today_d),
            'prev':       _daily_agg(prev30_start, prev30_end),
            'ly':         _daily_agg(ly30_start, ly30_end),
            'curr_label': f"{d30_start.strftime('%d.%m')}–{today_d.strftime('%d.%m.%Y')}",
            'prev_label': f"{prev30_start.strftime('%d.%m')}–{prev30_end.strftime('%d.%m.%Y')}",
            'ly_label':   f"{ly30_start.strftime('%d.%m')}–{ly30_end.strftime('%d.%m.%Y')}",
        }
        print(f"  📅 Daily sparkline: curr={len(daily_30['curr'])}д, prev={len(daily_30['prev'])}д, ly={len(daily_30['ly'])}д")
    except Exception as e:
        print(f"  ⚠️  daily_30 не побудовано: {e}")
        daily_30 = {}

    # ── GROSS MARGIN TREND (from profit reports) ─────────────────────────────
    margin_trend = []
    import glob as _glob
    for ym in all_ym:
        y, m = ym.split('-')
        mpath = OUTPUT_DIR / f"report_profit_cp_{y}_{m}.xlsx"
        if mpath.exists():
            try:
                mdf = pd.read_excel(mpath)
                rev = float(mdf['Виручка, грн'].sum())
                cogs = float(mdf.get('Собівартість, грн', pd.Series([0])).sum())
                profit = float(mdf.get('Прибуток, грн', pd.Series([0])).sum())
                margin_trend.append({'ym': ym, 'revenue': round(rev), 'cogs': round(cogs),
                    'profit': round(profit), 'margin_pct': round(profit / rev * 100, 1) if rev else 0})
            except: pass
    print(f"  📊 Margin trend: {len(margin_trend)} місяців")

    # ── AR (Accounts Receivable) from demands: shipped − paid ─────────────
    ar_monthly = []
    for ym in all_ym:
        ym_dem = dem[dem['YM'] == ym]
        shipped = float(ym_dem['Сума, грн'].sum())
        paid = float(ym_dem['Оплачено, грн'].sum())
        ar_monthly.append({'ym': ym, 'shipped': round(shipped), 'paid': round(paid), 'ar': round(shipped - paid)})
    ar_current = round(float(dem['Сума, грн'].sum() - dem['Оплачено, грн'].sum()))
    # DSO approximation: AR / (daily revenue)
    last3_inc = inc[inc['Дата'] >= pd.Timestamp.now() - pd.Timedelta(days=90)]
    avg_daily_rev = float(last3_inc['Сума, грн'].sum()) / 90 if len(last3_inc) else 1
    dso_days = round(ar_current / avg_daily_rev) if avg_daily_rev > 0 else 0
    print(f"  💰 AR: {ar_current:,.0f} грн, DSO: {dso_days} днів")

    # ── FUNNEL: ordered → shipped → paid (monthly) ────────────────────────
    co = pd.read_excel(OUTPUT_DIR / "customer_orders.xlsx")
    co['Дата'] = pd.to_datetime(co['Дата'], errors='coerce')
    co['YM'] = co['Дата'].dt.strftime('%Y-%m')
    co_m = co.groupby('YM')['Сума, грн'].sum()
    co_paid_m = co.groupby('YM')['Оплачено, грн'].sum()
    co_shipped_m = co.groupby('YM')['Відвантажено, грн'].sum()
    funnel_monthly = []
    for ym in all_ym:
        funnel_monthly.append({'ym': ym,
            'ordered': round(float(co_m.get(ym, 0))),
            'shipped': round(float(co_shipped_m.get(ym, 0))),
            'paid': round(float(co_paid_m.get(ym, 0)))})
    print(f"  🔄 Funnel: {len(funnel_monthly)} місяців")

    # ── GEOGRAPHY & CHANNEL SPLIT ─────────────────────────────────────────
    cp_df = pd.read_excel(OUTPUT_DIR / "counterparties.xlsx")
    _cp_tags = {}
    for _, r in cp_df.iterrows():
        n = str(r.get('Назва', '')).strip()
        if n: _cp_tags[n] = str(r.get('Теги', '') or '')

    def _classify_geo(name):
        n = name.upper()
        if any(p in n for p in ['HAB', 'ARVID', 'NORDQUIST', ' AB']): return 'Швеція'
        if 'GMBH' in n: return 'Німеччина'
        if any(p in n for p in ['S.R.O', 'SNECO SK']): return 'Словаччина'
        if 'SP. Z O.O' in n: return 'Польща'
        if any(p in n for p in [' LTD', ' LLC']) and not any(uk in n for uk in ['ТОВ', 'ФОП', 'ПП']): return 'Інше'
        return 'Україна'

    def _classify_channel(name, tags):
        t = tags.lower()
        if any(x in t for x in ['horeca', 'кавярня', 'спортзал', 'азс', 'пивная сеть', 'пивний магазин']): return 'HoReCa'
        if any(x in t for x in ['региональные сети', 'мережа магазинів']): return 'Ритейл'
        if any(x in t for x in ['b2b', 'дистрибьютор', 'прямая дистрибьюция', 'реализация']): return 'B2B'
        if any(x in t for x in ['клиенты интернет-магазинов', 'sneco tilda', 'sneco.ua', 'розетка', 'маркетплейс', 'інстаграм', 'новийсайт']): return 'Онлайн'
        if any(x in t for x in ['розница', 'единичный ритейл', 'продуктовий магазин', 'магазин', 'екомагазин', 'еколавка']): return 'Роздріб'
        return 'Інше'

    geo_split, channel_split = {}, {}
    for y in [2023, 2024, 2025, 2026]:
        ypath = OUTPUT_DIR / f"report_profit_cp_{y}.xlsx"
        if not ypath.exists(): continue
        try:
            ydf = pd.read_excel(ypath)
            geo_a, ch_a = {}, {}
            for _, r in ydf.iterrows():
                name = str(r.get('Контрагент', '')).strip()
                rev = float(r.get('Виручка, грн') or 0)
                tags = _cp_tags.get(name, '')
                g, c = _classify_geo(name), _classify_channel(name, tags)
                geo_a[g] = geo_a.get(g, 0) + rev
                ch_a[c] = ch_a.get(c, 0) + rev
            geo_split[str(y)] = {k: round(v) for k, v in sorted(geo_a.items(), key=lambda x: -x[1])}
            channel_split[str(y)] = {k: round(v) for k, v in sorted(ch_a.items(), key=lambda x: -x[1])}
        except: pass
    print(f"  🌍 Geo/Channel: {list(geo_split.keys())}")

    # ── NEW vs RETURNING CLIENTS + SLEEPING ───────────────────────────────
    all_cp_months = {}
    for f in sorted(_glob.glob(str(OUTPUT_DIR / "report_profit_cp_????_??.xlsx"))):
        ym_key = f.split('_cp_')[1].replace('.xlsx', '').replace('_', '-')
        try:
            mdf = pd.read_excel(f)
            all_cp_months[ym_key] = set(mdf['Контрагент'].dropna().str.strip())
        except: pass
    client_cohort = []
    seen_ever = set()
    for ym in sorted(all_cp_months.keys()):
        curr_set = all_cp_months[ym]
        new_c = curr_set - seen_ever
        ret_c = curr_set & seen_ever
        seen_ever |= curr_set
        client_cohort.append({'ym': ym, 'total': len(curr_set), 'new': len(new_c), 'returning': len(ret_c)})
    # Sleeping: had >10k lifetime but no activity this month
    now_ym = datetime.now().strftime('%Y-%m')
    curr_active = all_cp_months.get(now_ym, set())
    all_time_rev = {}
    for ypath in sorted(OUTPUT_DIR.glob("report_profit_cp_20??.xlsx")):
        try:
            ydf = pd.read_excel(ypath)
            for _, r in ydf.iterrows():
                n = str(r['Контрагент']).strip()
                all_time_rev[n] = all_time_rev.get(n, 0) + float(r.get('Виручка, грн') or 0)
        except: pass
    sleeping = [{'name': n, 'revenue': round(v)}
        for n, v in sorted(all_time_rev.items(), key=lambda x: -x[1])
        if v >= 10000 and n not in curr_active][:50]
    print(f"  👥 Cohort: {len(client_cohort)} міс, sleeping: {len(sleeping)}")

    # ── PRODUCTION ────────────────────────────────────────────────────────
    PRODUCTION_CAPACITY_KG = 5000  # 5 тон/місяць
    try:
        prod_df = pd.read_excel(OUTPUT_DIR / "production_done.xlsx")
        prod_df['Дата'] = pd.to_datetime(prod_df['Дата'], errors='coerce')
        prod_df['YM'] = prod_df['Дата'].dt.strftime('%Y-%m')
        prod_monthly = []
        for ym in sorted(prod_df['YM'].dropna().unique()):
            p = prod_df[prod_df['YM'] == ym]
            qty = round(float(p['Кількість'].sum()), 1)
            prod_monthly.append({'ym': ym, 'qty': qty, 'batches': len(p)})
        print(f"  🏭 Production: {len(prod_monthly)} місяців")
    except:
        prod_monthly = []

    # ── SUPPLY (procurement) ──────────────────────────────────────────────
    try:
        sup_df = pd.read_excel(OUTPUT_DIR / "supply.xlsx")
        sup_df['Дата'] = pd.to_datetime(sup_df['Дата'], errors='coerce')
        sup_df['YM'] = sup_df['Дата'].dt.strftime('%Y-%m')
        supply_monthly = []
        for ym in sorted(sup_df['YM'].dropna().unique()):
            s = sup_df[sup_df['YM'] == ym]
            supply_monthly.append({'ym': ym, 'amount': round(float(s['Сума, грн'].sum())), 'count': len(s)})
        print(f"  📦 Supply: {len(supply_monthly)} місяців")
    except:
        supply_monthly = []

    # ── PROCESSING PLANS (tech cards) ─────────────────────────────────────
    try:
        pp_df = pd.read_excel(OUTPUT_DIR / "processing_plans.xlsx")
        techcards = [{'name': str(r['Назва'])} for _, r in pp_df.iterrows() if pd.notna(r.get('Назва'))]
    except:
        techcards = []

    data = {
        'monthly': monthly, 'annual': annual, 'quarterly': quarterly,
        'seasonality': seasonality, 'hist': hist,
        'stock': stock_data, 'stock_detail': stock_detail, 'stock_history': stock_history,
        'top_clients': top_clients, 'top_products': top_products,
        'clients_by_year': clients_by_year, 'products_by_year': products_by_year,
        'clients_by_quarter': clients_by_quarter, 'products_by_quarter': products_by_quarter,
        'clients_by_month': clients_by_month, 'products_by_month': products_by_month,
        'daily_30': daily_30,
        'margin_trend': margin_trend,
        'ar_monthly': ar_monthly, 'ar_current': ar_current, 'dso_days': dso_days,
        'funnel_monthly': funnel_monthly,
        'geo_split': geo_split, 'channel_split': channel_split,
        'client_cohort': client_cohort, 'sleeping_clients': sleeping,
        'production': prod_monthly, 'production_capacity': PRODUCTION_CAPACITY_KG,
        'supply_monthly': supply_monthly,
        'techcards': techcards,
        'generated': datetime.now().strftime('%d.%m.%Y %H:%M'),
        'summary': {
            'total_inc': round(inc['Сума, грн'].sum()),
            'total_exp': round(exp['Сума, грн'].sum()),
            'total_ret': round(ret['Сума, грн'].sum()),
            'total_dem': round(dem['Сума, грн'].sum()),
            'total_orders': int(len(dem)),
            'median_pay': round(float(inc['Сума, грн'].median())),
            'return_rate': round(round(ret['Сума, грн'].sum()) / max(round(dem['Сума, грн'].sum()), 1) * 100, 2),
        }
    }

    # Read template or use inline
    tpl_path = Path(__file__).parent / "dashboard_template.html"
    if tpl_path.exists():
        tpl = tpl_path.read_text(encoding='utf-8')
        html = tpl.replace('/*DATA_PLACEHOLDER*/', json.dumps(data, ensure_ascii=False, default=str))
    else:
        # Embed data directly into existing dashboard
        dash_path = Path(__file__).parent / "dashboard.html"
        if dash_path.exists():
            html = dash_path.read_text(encoding='utf-8')
            html = _re.sub(
                r'const D = \{.*?\};',
                f'const D = {json.dumps(data, ensure_ascii=False, default=str)};',
                html, flags=_re.DOTALL
            )
        else:
            raise FileNotFoundError("dashboard.html не знайдено. Запусти sync вперше вручну.")

    out_path = Path(__file__).parent / "dashboard.html"
    out_path.write_text(html, encoding='utf-8')


# ── Git auto-push ─────────────────────────────────────────────────────────────

def git_push():
    """Комітить оновлені файли і пушить у GitHub."""
    import subprocess
    repo_dir = str(Path(__file__).parent)

    def run(cmd):
        result = subprocess.run(cmd, cwd=repo_dir, capture_output=True, text=True)
        return result.returncode, result.stdout.strip(), result.stderr.strip()

    print("\n🔄 Git push...")

    # Перевіряємо чи є git репо
    code, _, _ = run(['git', 'status'])
    if code != 0:
        print("  ⚠️  Git репо не ініціалізовано. Пропускаю.")
        print("  💡 Щоб налаштувати: відкрий GitHub Desktop і клонуй dreamcarua/sneco")
        return

    # Додаємо файли
    run(['git', 'add', 'dashboard.html'])
    run(['git', 'add', 'moysklad_sync.py', 'setup_schedule_mac.sh'])

    # Перевіряємо чи є що комітити
    code, out, _ = run(['git', 'status', '--porcelain'])
    if not out:
        print("  ℹ️  Змін немає — push не потрібен")
        return

    # Коміт
    msg = f"Auto-sync {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    code, out, err = run(['git', 'commit', '-m', msg])
    if code != 0:
        print(f"  ⚠️  Commit failed: {err}")
        return
    print(f"  ✅ Commit: {msg}")

    # Push
    code, out, err = run(['git', 'push'])
    if code != 0:
        print(f"  ⚠️  Push failed: {err[:200]}")
        print("  💡 Перевір налаштування GitHub Desktop або запусти push вручну")
    else:
        print(f"  ✅ Запушено → github.com/dreamcarua/sneco")


if __name__ == "__main__":
    main()
