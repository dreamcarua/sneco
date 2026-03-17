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
    # МойСклад повертає назву в полі "assortment" (товари) або "counterparty" (контрагенти)
    api_key = {"Товар": "assortment", "Контрагент": "counterparty"}.get(group_by, group_by.lower())
    records = []
    for r in rows:
        entity = r.get(api_key) or {}
        name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        records.append({
            group_by:               name,
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

    stock_data = []
    if not stk.empty:
        for _, r in stk[stk['Залишок'] > 0].sort_values('Залишок', ascending=False).head(25).iterrows():
            stock_data.append({'name': r['Товар'], 'stock': round(r['Залишок']),
                'reserve': round(r['Резерв']), 'available': round(r['Доступно']),
                'sum': round(r['Сума, грн'])})

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
                    agg[n] = {'name': n, 'revenue': 0, 'qty': 0,
                               'margin_sum': 0.0, 'returns': 0, '_cnt': 0}
                agg[n]['revenue']     += item.get('revenue', 0)
                agg[n]['qty']         += item.get('qty', 0)
                agg[n]['returns']     += item.get('returns', 0)
                agg[n]['margin_sum']  += item.get('margin', 0.0)
                agg[n]['_cnt']        += 1
        result = []
        for item in sorted(agg.values(), key=lambda x: x['revenue'], reverse=True)[:50]:
            result.append({
                'name':    item['name'],
                'revenue': item['revenue'],
                'qty':     item['qty'],
                'margin':  round(item['margin_sum'] / item['_cnt'], 1) if item['_cnt'] else 0,
                'returns': item['returns'],
            })
        return result

    if clients_by_year:
        top_clients = _aggregate_by_year(clients_by_year)
    if products_by_year:
        top_products = _aggregate_by_year(products_by_year)

    data = {
        'monthly': monthly, 'annual': annual, 'quarterly': quarterly,
        'seasonality': seasonality, 'hist': hist, 'stock': stock_data,
        'top_clients': top_clients, 'top_products': top_products,
        'clients_by_year': clients_by_year, 'products_by_year': products_by_year,
        'clients_by_quarter': clients_by_quarter, 'products_by_quarter': products_by_quarter,
        'clients_by_month': clients_by_month, 'products_by_month': products_by_month,
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
