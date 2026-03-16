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
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# ── Конфігурація ──────────────────────────────────────────────────────────────

load_dotenv(Path(__file__).parent / ".env")

TOKEN       = os.getenv("MOYSKLAD_TOKEN")
BASE_URL    = "https://api.moysklad.ru/api/remap/1.2"
DATE_FROM   = "2023-01-01 00:00:00"
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
    path = OUTPUT_DIR / f"{name}.xlsx"
    df.to_excel(path, index=False)
    flag = "✅" if reliable else "⚠️ "
    print(f"  {flag} data/{name}.xlsx  ({len(df)} рядків)")


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


def parse_stock(rows):
    return [{
        "Товар":        r.get("name"),
        "Код":          r.get("code"),
        "Артикул":      r.get("article"),
        "Склад":        safe(r.get("store")),
        "Залишок":      r.get("stock", 0),
        "Резерв":       r.get("reserve", 0),
        "Очікується":   r.get("inTransit", 0),
        "Доступно":     r.get("quantity", 0),
        "Ціна, грн":    r.get("price", 0) / 100,
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
    records = []
    for r in rows:
        records.append({
            group_by:               r.get("name", safe(r.get(group_by.lower(), {}))),
            "Продано, шт":          r.get("sellQuantity", 0),
            "Виручка, грн":         r.get("sellSum", 0) / 100,
            "Собівартість, грн":    r.get("buySum", 0) / 100,       # ⚠️ може бути неповною
            "Прибуток, грн":        r.get("grossProfit", 0) / 100,  # ⚠️ може бути неповним
            "Маржа %":              round(r.get("margin", 0) * 100, 2) if r.get("margin") else 0,
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
    save_excel(pd.DataFrame(parse_stock(all_rows)), "stock", reliable=True)

    # ── ⚠️ НЕПОВНІ ДАНІ ───────────────────────────────────

    print("\n\n⚠️  ДАНІ (можуть бути неповними)\n" + "-"*40)

    print("\n🚚 Переміщення між складами...")
    rows = fetch_all("entity/move")
    save_excel(pd.DataFrame(parse_moves(rows)), "moves", reliable=False)

    print("\n📥 Надходження від постачальників...")
    rows = fetch_all("entity/supply")
    save_excel(pd.DataFrame(parse_supply(rows)), "supply", reliable=False)

    print("\n🏭 Виробничі замовлення...")
    rows = fetch_all("entity/processingorder")
    save_excel(pd.DataFrame(parse_processing(rows)), "production_orders", reliable=False)

    print("\n🏭 Виробництво (виконані)...")
    rows = fetch_all("entity/processing")
    save_excel(pd.DataFrame(parse_processing(rows)), "production_done", reliable=False)

    print("\n📋 Технологічні карти...")
    rows = fetch_all("entity/processingplan", date_filter=False)
    save_excel(pd.DataFrame(parse_processingplans(rows)), "processing_plans", reliable=False)

    print("\n📈 Звіт: прибутковість по товарах...")
    rows = fetch_report("report/profit/byproduct")
    save_excel(pd.DataFrame(parse_profit_report(rows, "Товар")), "report_profit_by_product", reliable=False)

    print("\n📈 Звіт: прибутковість по контрагентах...")
    rows = fetch_report("report/profit/bycounterparty")
    save_excel(pd.DataFrame(parse_profit_report(rows, "Контрагент")), "report_profit_by_counterparty", reliable=False)

    # ── Підсумок ──────────────────────────────────────────

    print(f"\n{'='*55}")
    print(f"  ✅ Синхронізацію завершено!")
    print(f"  📁 Файли збережено в: snEco/data/")
    print(f"\n  Час: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
