#!/usr/bin/env python3
"""
BaseLinker Sales Dashboard - FastAPI Application
Displays all sold products with real-time updates, Excel export, and activity tracking
Enhanced: category detection, purchase orders, revenue tracking, sales channels, time frame filtering
"""

import os
import re
import json
import time
import asyncio
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict

# User is in Poland — all date filtering must use Polish timezone
POLISH_TZ = ZoneInfo("Europe/Warsaw")
from io import BytesIO
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, BackgroundTasks, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse

# Configuration from environment variables
BASELINKER_API_KEY = os.getenv("BASELINKER_API_KEY", "")
BASELINKER_INVENTORY_ID = int(os.getenv("BASELINKER_INVENTORY_ID", "58952"))
BASELINKER_API_URL = "https://api.baselinker.com/connector.php"
WYSLANE_STATUS_ID = 273568  # Wyslane (Shipped)
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Google Sheets cost loading
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
IMPORT_SHEET_ID = os.getenv("IMPORT_SHEET_ID", "1Ld-tbGXGIheTsLc0RCg5RdtbqkOty4gTf3wEFL9YufE")
FALLBACK_MARKUP_GROSS = 3.444  # cost = gross_price / 3.444 (2.8x net markup + 23% VAT)

# Currency conversion — BaseLinker returns price_brutto in the ORDER's currency
EXCHANGE_RATES_TO_PLN = {
    'PLN': 1.0,
    'EUR': 4.30,
    'USD': 4.05,
    'GBP': 5.10,
    'CZK': 0.175,
    'HUF': 0.0108,
    'SEK': 0.39,
    'DKK': 0.58,
    'NOK': 0.38,
    'CHF': 4.55,
    'RON': 0.87,
    'BGN': 2.20,
    'HRK': 0.57,
    'RUB': 0.045,
    'UAH': 0.11,
}


def convert_to_pln(amount: float, currency: str) -> float:
    """Convert amount from order currency to PLN."""
    if not currency or currency.upper() == 'PLN':
        return amount
    rate = EXCHANGE_RATES_TO_PLN.get(currency.upper())
    if rate:
        return amount * rate
    print(f"Unknown currency '{currency}' — treating as PLN")
    return amount

# Data cache
cache = {
    "data": None,
    "last_updated": None,
    "next_refresh": None,
    "is_refreshing": False,
    "purchase_orders": [],
    "raw_orders": None,
    "inventory": None,
    "po_items_map": None,
    "order_sources": {},
    "all_recent_orders": None,
    "costs": ({}, {}),
}

REFRESH_INTERVAL = 3600  # 1 hour in seconds

# Database connection
db_conn = None


def get_db_connection():
    """Get PostgreSQL connection"""
    global db_conn
    if not DATABASE_URL:
        return None
    try:
        import psycopg2
        if db_conn is None or db_conn.closed:
            db_conn = psycopg2.connect(DATABASE_URL)
        return db_conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def init_database():
    """Initialize database tables"""
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                action VARCHAR(50),
                total_orders INT,
                total_variants INT,
                total_units_sold INT,
                details JSONB
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales_snapshot (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                sku VARCHAR(100),
                product_name TEXT,
                units_sold INT,
                current_stock INT
            )
        """)
        conn.commit()
        cur.close()
        print("Database tables initialized")
    except Exception as e:
        print(f"Database init error: {e}")


def log_activity(action: str, data: dict = None):
    """Log activity to database"""
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO activity_log (action, total_orders, total_variants, total_units_sold, details)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            action,
            data.get('total_orders', 0) if data else 0,
            data.get('total_variants', 0) if data else 0,
            data.get('total_units_sold', 0) if data else 0,
            json.dumps(data) if data else None
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Log activity error: {e}")


def save_sales_snapshot(products: list):
    """Save current sales data snapshot"""
    conn = get_db_connection()
    if not conn or not products:
        return
    try:
        cur = conn.cursor()
        for p in products[:50]:
            cur.execute("""
                INSERT INTO sales_snapshot (sku, product_name, units_sold, current_stock)
                VALUES (%s, %s, %s, %s)
            """, (p['sku'], p['product_name'][:200], p['units_sold'], p['current_stock']))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Save snapshot error: {e}")


def call_baselinker(method: str, params: dict = None) -> dict:
    """Make BaseLinker API call"""
    if not BASELINKER_API_KEY:
        return {"error": "BASELINKER_API_KEY not configured"}

    data = {
        'token': BASELINKER_API_KEY,
        'method': method,
        'parameters': json.dumps(params or {})
    }
    try:
        response = requests.post(BASELINKER_API_URL, data=data, timeout=120)
        result = response.json()
        if result.get('status') == 'ERROR':
            return {"error": result.get('error_message', 'Unknown error')}
        return result
    except Exception as e:
        return {"error": str(e)}


def determine_category(name: str) -> str:
    """Determine product category from Polish product name"""
    name_lower = name.lower()

    # Coffee Tables (check BEFORE generic tables)
    if 'stolik kawowy' in name_lower or u'\u0142awa' in name_lower or 'lawa' in name_lower:
        return 'Coffee Tables'

    # Bar Stools (check BEFORE generic chairs)
    if 'hoker' in name_lower or 'barowy' in name_lower:
        return 'Bar Stools'

    # Ceramic Tables
    if 'ceramiczn' in name_lower or 'ceramic' in name_lower:
        if u'st\u00f3\u0142' in name_lower or 'stol' in name_lower or 'table' in name_lower:
            return 'Ceramic Tables'

    # Wooden Tables
    wood_keywords = ['drewn', 'industrialn', 'wooden', 'wood']
    if any(kw in name_lower for kw in wood_keywords):
        if u'st\u00f3\u0142' in name_lower or 'stol' in name_lower:
            if 'stolik' not in name_lower:
                return 'Wooden Tables'

    # Tables (generic, not caught above)
    if (u'st\u00f3\u0142' in name_lower or 'stol' in name_lower) and 'stolik' not in name_lower:
        return 'Tables'

    # Armchairs (check BEFORE generic chairs)
    if 'fotel' in name_lower or 'armchair' in name_lower:
        return 'Armchairs'

    # Chairs
    if u'krzes\u0142o' in name_lower or 'krzeslo' in name_lower or ('chair' in name_lower and 'armchair' not in name_lower):
        return 'Chairs'

    return 'Other'


# ========== COST LOADING ==========

def load_costs_from_import_sheet():
    """Load product costs from Google Sheets import tabs.
    Replicates logic from Marbily App's sales_center_service.py.
    """
    costs_by_sku = {}
    costs_by_base_sku = {}

    if not GOOGLE_CREDENTIALS_JSON:
        print("GOOGLE_CREDENTIALS_JSON not configured, skipping cost loading")
        return costs_by_sku, costs_by_base_sku

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(IMPORT_SHEET_ID)

        for ws in sheet.worksheets():
            all_data = ws.get_all_values()
            if len(all_data) < 9:
                continue

            # Find header row (scan first 15 rows)
            header_row_idx = None
            for i in range(min(15, len(all_data))):
                row_text = ' '.join([str(c).lower() for c in all_data[i][:20]])
                if 'model' in row_text and ('total cost' in row_text or 'unit price' in row_text):
                    header_row_idx = i
                    break

            if header_row_idx is None:
                continue

            headers = all_data[header_row_idx]

            # Dynamic column detection
            model_col = None
            color_col = None
            cost_col = None

            for idx, header in enumerate(headers):
                h = str(header).lower().strip()
                if model_col is None and ('model' in h or h == 'sku'):
                    model_col = idx
                if color_col is None and ('color' in h or 'kolor' in h):
                    color_col = idx
                if cost_col is None and 'total cost' in h and 'pln' in h and 'all units' not in h:
                    cost_col = idx

            if model_col is None or cost_col is None:
                continue

            count = 0
            for row in all_data[header_row_idx + 1:]:
                if len(row) <= max(model_col, cost_col):
                    continue

                model = row[model_col].strip().upper() if model_col < len(row) else ""
                color = row[color_col].strip().upper() if color_col and color_col < len(row) else ""
                cost_str = row[cost_col].strip() if cost_col < len(row) else ""

                if not model or not cost_str:
                    continue

                try:
                    cost = float(cost_str.replace('z\u0142', '').replace('PLN', '').replace(',', '').strip())
                    if cost > 0:
                        if color and color not in ['N/A', '-', '']:
                            costs_by_sku[f"{model}-{color}"] = cost
                        costs_by_base_sku[model] = cost
                        count += 1
                except:
                    continue

            if count > 0:
                print(f"  Sheet '{ws.title}': {count} costs loaded")

        print(f"Total costs loaded: {len(costs_by_sku)} SKU, {len(costs_by_base_sku)} base SKU")

    except Exception as e:
        print(f"Error loading costs from sheets: {e}")
        import traceback
        traceback.print_exc()

    return costs_by_sku, costs_by_base_sku


def find_cost_for_sku(sku, costs_by_sku, costs_by_base_sku):
    """Find cost for a SKU using multi-tier matching.
    Handles SKUs like S-91-451, DR2523-SZARY, DT-244-Czarny.
    Returns (cost, match_type).
    """
    if not sku:
        return 0, None

    sku_upper = sku.strip().upper()

    # 1. Exact match (full SKU including color suffix)
    if sku_upper in costs_by_sku:
        return costs_by_sku[sku_upper], 'exact'

    # 2. Progressive base SKU matching — try removing segments from the right
    # e.g. S-91-451 → try "S-91-451", "S-91", "S" against costs_by_base_sku
    parts = sku_upper.split('-')
    for i in range(len(parts) - 1, 0, -1):
        candidate = '-'.join(parts[:i])
        if candidate in costs_by_base_sku:
            return costs_by_base_sku[candidate], 'base'

    # Also try the full SKU as a base key (no-dash SKUs)
    if sku_upper in costs_by_base_sku:
        return costs_by_base_sku[sku_upper], 'base'

    # 3. Prefix match — find any cost key that starts with a base candidate
    for i in range(len(parts) - 1, 0, -1):
        candidate = '-'.join(parts[:i])
        for key, cost in costs_by_sku.items():
            if key.startswith(candidate + '-'):
                return cost, 'prefix'

    return 0, None


# ========== ORDER FETCHING ==========

def fetch_all_wyslane_orders() -> list:
    """Fetch ALL orders with 'Wyslane' status"""
    all_orders = []
    last_order_id = 0

    while True:
        params = {
            'status_id': WYSLANE_STATUS_ID,
            'get_unconfirmed_orders': False
        }
        if last_order_id > 0:
            params['id_from'] = last_order_id

        result = call_baselinker('getOrders', params)
        if "error" in result:
            break

        fetched_orders = result.get('orders', [])
        if not fetched_orders:
            break

        all_orders.extend(fetched_orders)
        last_order_id = fetched_orders[-1].get('order_id', 0)

        time.sleep(0.3)

        if len(fetched_orders) < 100:
            break

    return all_orders


def fetch_recent_orders_all_statuses(days: int = 90) -> list:
    """Fetch orders from ALL statuses for date-filtered views.

    When filtering by date (Today, Yesterday, etc.), we want ALL orders placed
    on that date regardless of their current status — not just shipped ones.
    """
    from_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    all_orders = []
    last_order_id = 0

    while True:
        params = {
            'date_from': from_ts,
            'get_unconfirmed_orders': False,
        }
        # NO status_id param -> fetches all statuses
        if last_order_id > 0:
            params['id_from'] = last_order_id

        result = call_baselinker('getOrders', params)
        if "error" in result:
            print(f"Error fetching all-status orders: {result.get('error')}")
            break

        fetched = result.get('orders', [])
        if not fetched:
            break

        all_orders.extend(fetched)
        last_order_id = fetched[-1].get('order_id', 0)
        time.sleep(0.3)

        if len(fetched) < 100:
            break

    print(f"Fetched {len(all_orders)} orders from all statuses (last {days} days)")
    return all_orders


def fetch_order_sources() -> dict:
    """Fetch order sources mapping (source_id -> name) from BaseLinker API.

    API returns nested dict: {sources: {type: {account_id: name_or_info}}}
    e.g. {sources: {allegro: {24535: "Marbily", 26446: "Glamova"}, shop: {5017156: "Marbily"}}}
    """
    result = call_baselinker('getOrderSources')
    if "error" in result:
        print(f"Error fetching order sources: {result.get('error')}")
        return {}

    source_names = {}

    # Iterate all top-level keys (skip 'status')
    for key, val in result.items():
        if key == 'status':
            continue
        # val is either the 'sources' wrapper dict or a direct type dict
        if not isinstance(val, dict):
            continue

        # Check if this is the 'sources' wrapper: {type: {id: name}}
        # or a direct type-level dict: {id: name}
        for source_type, accounts in val.items():
            if not isinstance(accounts, dict):
                continue
            # accounts = {account_id: name_string_or_dict}
            for acc_id, acc_name in accounts.items():
                if isinstance(acc_name, dict):
                    name = acc_name.get('name', str(acc_id))
                else:
                    name = str(acc_name)
                # Prefix with type for clarity (except personal/generic)
                type_label = source_type.capitalize()
                if type_label in ('Personal', 'Order_return', '0'):
                    source_names[str(acc_id)] = name
                else:
                    source_names[str(acc_id)] = f"{type_label} - {name}" if name != type_label else name

    print(f"Parsed order sources: {source_names}")
    return source_names


def aggregate_sales(orders: list, source_names: dict = None) -> dict:
    """Aggregate sales by variant with revenue and channel tracking.

    Includes outlier detection: if a variant has 3+ price instances and one
    price is >10x the median, it's excluded from revenue (Allegro sometimes
    reports order totals as per-unit price).

    Payment filter: only include COD or paid orders.
    """
    sales_by_variant = defaultdict(lambda: {
        'product_name': '',
        'sku': '',
        'units_sold': 0,
        'bl_product_id': '',
        'total_revenue': 0.0,
        'sales_by_channel': defaultdict(int),
        '_price_instances': [],  # track individual prices for outlier detection
    })

    for order in orders:
        # Detect sales channel
        source = order.get('order_source', 'unknown')
        source_id = str(order.get('order_source_id', ''))

        # Use the source name from the mapping if available
        if source_names and source_id in source_names:
            channel = source_names[source_id]
        elif 'allegro' in source.lower():
            channel = 'Allegro'
        elif 'shopify' in source.lower() or source == 'shop':
            channel = 'Shopify'
        elif 'olx' in source.lower():
            channel = 'OLX'
        else:
            channel = source

        # Get order currency for price conversion
        order_currency = (order.get('currency', '') or 'PLN').upper()

        for product in order.get('products', []):
            bl_product_id = str(product.get('variant_id', ''))
            if not bl_product_id or bl_product_id == '0':
                continue

            sku = product.get('sku', '') or ''
            name = product.get('name', '') or 'Unknown'
            qty = int(product.get('quantity', 1))
            price_original = float(product.get('price_brutto', 0))
            price = convert_to_pln(price_original, order_currency)

            variant_key = sku if sku else bl_product_id

            sales_by_variant[variant_key]['product_name'] = name
            sales_by_variant[variant_key]['sku'] = sku
            sales_by_variant[variant_key]['units_sold'] += qty
            sales_by_variant[variant_key]['bl_product_id'] = bl_product_id
            sales_by_variant[variant_key]['total_revenue'] += price * qty
            sales_by_variant[variant_key]['sales_by_channel'][channel] += qty
            sales_by_variant[variant_key]['_price_instances'].append({'price': price, 'qty': qty})

    # Post-process: detect and exclude outlier prices per variant
    result = {}
    for key, val in sales_by_variant.items():
        instances = val.pop('_price_instances', [])
        prices = [inst['price'] for inst in instances if inst['price'] > 0]

        if len(prices) >= 3:
            sorted_prices = sorted(prices)
            median_price = sorted_prices[len(sorted_prices) // 2]
            if median_price > 0:
                # Recalculate revenue excluding outliers (price > 10x median)
                clean_revenue = sum(
                    inst['price'] * inst['qty'] for inst in instances
                    if inst['price'] <= median_price * 10
                )
                if clean_revenue != val['total_revenue']:
                    excluded = val['total_revenue'] - clean_revenue
                    print(f"Outlier detected for {key}: excluded {excluded:.2f} PLN "
                          f"(median={median_price:.2f}, threshold={median_price*10:.2f})")
                    val['total_revenue'] = clean_revenue

        # Floor units_sold and revenue at 0 — negative means returns exceeded sales
        val['units_sold'] = max(0, val['units_sold'])
        val['total_revenue'] = max(0.0, val['total_revenue'])

        val['sales_by_channel'] = dict(val['sales_by_channel'])
        result[key] = val

    return result


def get_inventory(product_ids: list) -> dict:
    """Fetch current inventory for products, including Shopify variant IDs"""
    if not product_ids:
        return {}

    unique_ids = list(set(int(pid) for pid in product_ids if pid))
    inventory = {}

    for i in range(0, len(unique_ids), 100):
        batch = unique_ids[i:i+100]

        result = call_baselinker('getInventoryProductsData', {
            'inventory_id': BASELINKER_INVENTORY_ID,
            'products': batch
        })

        if "error" in result:
            continue

        products_data = result.get('products', {})

        for pid_str, product_info in products_data.items():
            stock_data = product_info.get('stock', {})
            total_stock = sum(int(s) for s in stock_data.values() if s)

            images = product_info.get('images', {})
            image_url = list(images.values())[0] if images else None

            # Extract Shopify variant ID from links
            links = product_info.get('links', {}).get('shop_5017156', {})
            shopify_variant_id = str(links.get('variant_id', ''))

            inventory[pid_str] = {
                'stock': total_stock,
                'image_url': image_url,
                'shopify_variant_id': shopify_variant_id
            }

        time.sleep(0.5)

    return inventory


def fetch_purchase_orders() -> list:
    """Fetch all purchase orders from BaseLinker warehouse control"""
    all_pos = []
    date_from = 1704067200  # Jan 1, 2024
    date_to = int(time.time())
    page = 0

    while True:
        result = call_baselinker('getInventoryPurchaseOrders', {
            'date_from': date_from,
            'date_to': date_to,
            'page': page
        })

        if "error" in result:
            print(f"PO fetch error: {result.get('error')}")
            break

        orders = result.get('purchase_orders', [])
        if not orders:
            break

        all_pos.extend(orders)
        page += 1
        time.sleep(0.6)

        # Stop if fewer than 100 results (last page)
        if len(orders) < 100:
            break

    return all_pos


def fetch_purchase_order_items(order_id: int) -> list:
    """Fetch items for a specific purchase order (paginated, 100 per page)"""
    all_items = []
    page = 0

    while True:
        params = {'order_id': order_id}
        if page > 0:
            params['page'] = page

        result = call_baselinker('getInventoryPurchaseOrderItems', params)

        if "error" in result:
            break

        items = result.get('items', [])
        if not items:
            break

        all_items.extend(items)
        page += 1

        if len(items) < 100:
            break

        time.sleep(0.3)

    return all_items


def filter_products(products: list, category: str = "", po: str = "", search: str = "") -> list:
    """Apply filters to product list (shared between API and Excel download)"""
    filtered = products

    if category:
        filtered = [p for p in filtered if p.get('category') == category]

    if po:
        po_data = next((p for p in cache.get("purchase_orders", []) if str(p.get('po_id')) == po), None)
        if po_data and po_data.get('product_ids'):
            pid_set = set(str(pid) for pid in po_data['product_ids'])
            filtered = [p for p in filtered if str(p.get('bl_product_id')) in pid_set]

    if search:
        variant_match = re.search(r'[?&]variant=(\d+)', search)
        if variant_match:
            vid = variant_match.group(1)
            filtered = [p for p in filtered if p.get('shopify_variant_id') == vid]
        else:
            q = search.lower()
            filtered = [p for p in filtered if
                q in (p.get('product_name', '') or '').lower() or
                q in (p.get('sku', '') or '').lower() or
                q in (p.get('bl_product_id', '') or '') or
                q in (p.get('category', '') or '').lower()
            ]

    return filtered


def build_response(orders: list, inventory: dict, po_items_map: dict, source_names: dict = None) -> dict:
    """Build product list from orders + inventory + PO data. Reusable for date-filtered views."""
    sales_data = aggregate_sales(orders, source_names)

    products = []
    total_revenue = 0.0
    po_match_count = 0
    global_channel_units = defaultdict(int)
    global_channel_revenue = defaultdict(float)

    for variant_key, data in sales_data.items():
        bl_pid = data['bl_product_id']
        inv_info = inventory.get(bl_pid, {})
        revenue = round(data['total_revenue'], 2)
        total_revenue += revenue
        product_pos = po_items_map.get(bl_pid, []) if po_items_map else []
        if product_pos:
            po_match_count += 1

        # Floor units_sold at 0 (negative quantities from returns get netted)
        units_sold = max(0, data['units_sold'])

        # Stock: keep raw value but flag negative
        raw_stock = inv_info.get('stock', 0)

        products.append({
            'image_url': inv_info.get('image_url', ''),
            'product_name': data['product_name'],
            'sku': data['sku'],
            'units_sold': units_sold,
            'current_stock': raw_stock,
            'total_revenue': revenue,
            'sales_by_channel': data['sales_by_channel'],
            'category': determine_category(data['product_name']),
            'shopify_variant_id': inv_info.get('shopify_variant_id', ''),
            'bl_product_id': bl_pid,
            'purchase_orders': product_pos
        })

        # Accumulate global channel breakdown
        for channel, qty in data['sales_by_channel'].items():
            global_channel_units[channel] += max(0, qty)
            # Estimate revenue per channel proportionally
        # Revenue by channel: distribute proportionally by qty
        total_qty = sum(data['sales_by_channel'].values())
        if total_qty > 0:
            for channel, qty in data['sales_by_channel'].items():
                if qty > 0:
                    global_channel_revenue[channel] += revenue * (qty / total_qty)

    products.sort(key=lambda x: x['units_sold'], reverse=True)

    # ========== COST ENRICHMENT ==========
    costs_by_sku, costs_by_base_sku = cache.get("costs", ({}, {}))
    total_cost = 0.0

    for product in products:
        cost, match_type = find_cost_for_sku(product['sku'], costs_by_sku, costs_by_base_sku)
        if cost == 0 and product['units_sold'] > 0 and product['total_revenue'] > 0:
            avg_price = product['total_revenue'] / product['units_sold']
            cost = avg_price / FALLBACK_MARKUP_GROSS
            match_type = 'fallback'
        product['unit_cost'] = round(cost, 2)
        product['total_cost'] = round(cost * product['units_sold'], 2)
        product['cost_match'] = match_type
        total_cost += product['total_cost']

    # Financial calculations
    net_revenue = round(total_revenue / 1.23, 2)
    profit = round(net_revenue - total_cost, 2)
    profit_margin = round((profit / net_revenue) * 100, 1) if net_revenue > 0 else 0

    # Build sorted channel breakdown (highest units first)
    channel_breakdown = []
    for ch in sorted(global_channel_units.keys(), key=lambda c: global_channel_units[c], reverse=True):
        channel_breakdown.append({
            "channel": ch,
            "units": global_channel_units[ch],
            "revenue": round(global_channel_revenue.get(ch, 0), 2)
        })

    return {
        "total_variants": len(products),
        "total_units_sold": sum(p['units_sold'] for p in products),
        "total_orders": len(orders),
        "total_revenue": round(total_revenue, 2),
        "net_revenue": net_revenue,
        "total_cost": round(total_cost, 2),
        "profit": profit,
        "profit_margin": profit_margin,
        "channel_breakdown": channel_breakdown,
        "products": products
    }


def refresh_data():
    """Fetch fresh data from BaseLinker"""
    if cache["is_refreshing"]:
        return cache["data"]

    cache["is_refreshing"] = True

    try:
        # Fetch order sources for proper channel names
        order_sources = fetch_order_sources()
        cache["order_sources"] = order_sources

        # Load costs from Google Sheets
        print("Loading costs from import sheets...")
        costs = load_costs_from_import_sheet()
        cache["costs"] = costs

        # Fetch orders and aggregate sales
        orders = fetch_all_wyslane_orders()

        # Cache raw orders (Wysłane only) for "All Time" view
        cache["raw_orders"] = orders

        # Also fetch recent orders from ALL statuses for date-filtered views
        # (Today/Yesterday/etc. should show all orders, not just shipped ones)
        all_recent = fetch_recent_orders_all_statuses(days=90)
        cache["all_recent_orders"] = all_recent

        sales_data = aggregate_sales(orders, order_sources)
        product_ids = set(d['bl_product_id'] for d in sales_data.values() if d['bl_product_id'])

        # Also include product IDs from recent all-status orders
        # so that Today/Yesterday products have images even if not yet shipped
        for order in all_recent:
            for product in order.get('products', []):
                vid = str(product.get('variant_id', ''))
                if vid and vid != '0':
                    product_ids.add(vid)

        inventory = get_inventory(list(product_ids))

        # Cache inventory for reuse
        cache["inventory"] = inventory

        # Fetch purchase orders
        print("Fetching purchase orders...")
        pos = fetch_purchase_orders()
        po_items_map = {}  # product_id -> [PO references]
        po_list = []

        for po in pos:
            po_id = po.get('id')
            if not po_id:
                continue

            doc_number = po.get('document_number', f'PO-{po_id}')
            items = fetch_purchase_order_items(po_id)
            time.sleep(0.3)

            po_entry = {
                'po_id': po_id,
                'document_number': doc_number,
                'status': po.get('status', 0),
                'date': po.get('date_created', ''),
                'items_count': po.get('items_count', len(items)),
                'total_quantity': po.get('total_quantity', 0),
                'total_cost': str(po.get('total_cost', '0')),
                'product_ids': []
            }

            for item in items:
                pid = str(item.get('product_id', ''))
                if pid and pid != '0':
                    po_entry['product_ids'].append(pid)
                    if pid not in po_items_map:
                        po_items_map[pid] = []
                    po_items_map[pid].append({
                        'po_id': po_id,
                        'document_number': doc_number,
                        'quantity_ordered': item.get('quantity', 0),
                        'completed_quantity': item.get('completed_quantity', 0),
                        'item_cost': float(item.get('item_cost', 0)),
                        'product_sku': item.get('product_sku', '')
                    })

            po_list.append(po_entry)

        print(f"Fetched {len(po_list)} purchase orders, {len(po_items_map)} unique product mappings")

        # Cache PO items map for reuse
        cache["po_items_map"] = po_items_map

        # Build full response using helper
        response_data = build_response(orders, inventory, po_items_map, order_sources)

        now = datetime.now(timezone.utc)
        cache["data"] = response_data
        cache["last_updated"] = now.isoformat()
        cache["next_refresh"] = (now + timedelta(seconds=REFRESH_INTERVAL)).isoformat()
        cache["purchase_orders"] = po_list

        # Log to database
        log_activity("refresh", cache["data"])
        save_sales_snapshot(response_data["products"])

    except Exception as e:
        print(f"Error refreshing data: {e}")
        import traceback
        traceback.print_exc()
        log_activity("error", {"message": str(e)})
    finally:
        cache["is_refreshing"] = False

    return cache["data"]


async def background_refresh_task():
    """Background task to refresh data every hour"""
    while True:
        await asyncio.sleep(REFRESH_INTERVAL)
        refresh_data()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    init_database()
    log_activity("startup")
    refresh_data()
    task = asyncio.create_task(background_refresh_task())
    yield
    task.cancel()


app = FastAPI(
    title="BaseLinker Sales Dashboard",
    description="Real-time sales tracking from BaseLinker",
    lifespan=lifespan
)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "last_updated": cache["last_updated"],
        "data_loaded": cache["data"] is not None,
        "database_connected": get_db_connection() is not None
    }


@app.get("/api/sales")
async def get_sales(
    date_from: str = Query("", description="Start date YYYY-MM-DD"),
    date_to: str = Query("", description="End date YYYY-MM-DD"),
):
    if cache["data"] is None:
        return JSONResponse(status_code=503, content={"error": "Data not loaded yet. Please wait..."})

    # No date params -> return cached "all time" data (fast path, no regression)
    if not date_from and not date_to:
        return {
            "last_updated": cache["last_updated"],
            "next_refresh": cache["next_refresh"],
            "is_refreshing": cache["is_refreshing"],
            **cache["data"]
        }

    # With date params -> use ALL-status orders (not just Wysłane)
    # so "Yesterday" shows all 12 orders, not just 2 shipped ones
    raw_orders = cache.get("all_recent_orders") or cache.get("raw_orders")
    inventory = cache.get("inventory") or {}
    po_items_map = cache.get("po_items_map") or {}
    order_sources = cache.get("order_sources", {})

    if raw_orders is None:
        return JSONResponse(status_code=503, content={"error": "Raw order data not available. Please wait for refresh."})

    # Parse date boundaries as Polish timezone (user is in Poland)
    # "Today" = midnight-to-midnight in Europe/Warsaw, not UTC
    try:
        if date_from:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=POLISH_TZ)
            ts_from = int(dt_from.timestamp())
        else:
            ts_from = 0
        if date_to:
            dt_to = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=POLISH_TZ) + timedelta(days=1)
            ts_to = int(dt_to.timestamp())
        else:
            ts_to = int(time.time()) + 86400
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid date format. Use YYYY-MM-DD."})

    # Filter orders by date_add (order creation time) — NOT date_confirmed
    # date_add is the actual order date; date_confirmed can be days later
    filtered_orders = []
    for order in raw_orders:
        order_ts = order.get("date_add", 0)
        if isinstance(order_ts, (int, float)) and ts_from <= order_ts < ts_to:
            filtered_orders.append(order)

    response_data = build_response(filtered_orders, inventory, po_items_map, order_sources)

    return {
        "last_updated": cache["last_updated"],
        "next_refresh": cache["next_refresh"],
        "is_refreshing": cache["is_refreshing"],
        "date_from": date_from,
        "date_to": date_to,
        **response_data
    }


@app.get("/api/purchase-orders")
async def get_purchase_orders():
    """Return the list of purchase orders"""
    return {
        "purchase_orders": cache.get("purchase_orders", [])
    }


@app.post("/api/refresh")
async def force_refresh(background_tasks: BackgroundTasks):
    if cache["is_refreshing"]:
        return {"status": "already_refreshing", "message": "Refresh already in progress"}

    log_activity("manual_refresh_requested")
    background_tasks.add_task(refresh_data)
    return {"status": "started", "message": "Refresh started in background"}


@app.get("/api/activity")
async def get_activity():
    """Get recent activity log"""
    conn = get_db_connection()
    if not conn:
        return {"error": "Database not connected", "logs": []}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, action, total_orders, total_variants, total_units_sold
            FROM activity_log ORDER BY timestamp DESC LIMIT 50
        """)
        rows = cur.fetchall()
        cur.close()
        return {
            "logs": [
                {
                    "timestamp": r[0].isoformat() if r[0] else None,
                    "action": r[1],
                    "total_orders": r[2],
                    "total_variants": r[3],
                    "total_units_sold": r[4]
                } for r in rows
            ]
        }
    except Exception as e:
        return {"error": str(e), "logs": []}


@app.get("/api/debug/orders")
async def debug_orders():
    """Debug: show timestamp fields for recent orders to diagnose date filtering"""
    # Use all-status orders for debug (shows the full picture)
    all_recent = cache.get("all_recent_orders")
    raw_orders = cache.get("raw_orders")
    debug_source = all_recent or raw_orders
    if not debug_source:
        return {"error": "No orders cached"}

    # Get the 20 most recent orders by date_add
    sorted_orders = sorted(debug_source, key=lambda o: o.get("date_add", 0), reverse=True)[:20]

    debug_list = []
    for o in sorted_orders:
        date_add = o.get("date_add", 0)
        date_confirmed = o.get("date_confirmed", 0)
        date_in_status = o.get("date_in_status", 0)

        debug_list.append({
            "order_id": o.get("order_id"),
            "date_add": date_add,
            "date_add_human": datetime.fromtimestamp(date_add, tz=POLISH_TZ).strftime("%Y-%m-%d %H:%M:%S") if date_add else None,
            "date_confirmed": date_confirmed,
            "date_confirmed_human": datetime.fromtimestamp(date_confirmed, tz=POLISH_TZ).strftime("%Y-%m-%d %H:%M:%S") if date_confirmed else None,
            "date_in_status": date_in_status,
            "date_in_status_human": datetime.fromtimestamp(date_in_status, tz=POLISH_TZ).strftime("%Y-%m-%d %H:%M:%S") if date_in_status else None,
            "order_source": o.get("order_source"),
            "order_source_id": o.get("order_source_id"),
            "products_count": len(o.get("products", [])),
            "products_summary": [
                {
                    "name": p.get("name", "")[:50],
                    "sku": p.get("sku", ""),
                    "qty": p.get("quantity"),
                    "price_brutto": p.get("price_brutto"),
                }
                for p in o.get("products", [])[:3]
            ]
        })

    return {
        "total_cached_wyslane_orders": len(raw_orders) if raw_orders else 0,
        "total_cached_all_status_orders": len(all_recent) if all_recent else 0,
        "source_names": cache.get("order_sources", {}),
        "recent_orders": debug_list
    }


@app.get("/api/download")
async def download_excel(
    category: str = Query("", description="Filter by category"),
    po: str = Query("", description="Filter by purchase order ID"),
    search: str = Query("", description="Search text or Shopify URL"),
    date_from: str = Query("", description="Start date YYYY-MM-DD"),
    date_to: str = Query("", description="End date YYYY-MM-DD"),
):
    if cache["data"] is None:
        return JSONResponse(status_code=503, content={"error": "Data not loaded yet"})

    # Determine source products based on date filter
    if date_from or date_to:
        # Use ALL-status orders for date-filtered views (same as /api/sales)
        raw_orders = cache.get("all_recent_orders") or cache.get("raw_orders")
        inventory = cache.get("inventory") or {}
        po_items_map = cache.get("po_items_map") or {}
        order_sources = cache.get("order_sources", {})

        if raw_orders is None:
            return JSONResponse(status_code=503, content={"error": "Raw order data not available"})

        try:
            if date_from:
                dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=POLISH_TZ)
                ts_from = int(dt_from.timestamp())
            else:
                ts_from = 0
            if date_to:
                dt_to = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=POLISH_TZ) + timedelta(days=1)
                ts_to = int(dt_to.timestamp())
            else:
                ts_to = int(time.time()) + 86400
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "Invalid date format. Use YYYY-MM-DD."})

        filtered_orders = [
            o for o in raw_orders
            if ts_from <= o.get("date_add", 0) < ts_to
        ]
        source_data = build_response(filtered_orders, inventory, po_items_map, order_sources)
    else:
        source_data = cache["data"]

    # Apply category/PO/search filters on top
    products = filter_products(source_data["products"], category, po, search)

    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Sales Report"

    header_font = Font(bold=True, size=11, color="FFFFFF")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    ws.merge_cells('A1:G1')
    ws['A1'] = "BASELINKER SALES REPORT - WYSLANE ORDERS"
    ws['A1'].font = Font(bold=True, size=14)

    # Show active filters in subtitle
    filter_parts = []
    if date_from or date_to:
        date_label = f"{date_from or 'start'} to {date_to or 'now'}"
        filter_parts.append(f"Period: {date_label}")
    if category:
        filter_parts.append(f"Category: {category}")
    if po:
        po_data = next((p for p in cache.get('purchase_orders', []) if str(p.get('po_id')) == po), None)
        if po_data:
            filter_parts.append(f"PO: {po_data.get('document_number', po)}")
    if search:
        filter_parts.append(f"Search: {search}")
    filter_text = f" | Filters: {', '.join(filter_parts)}" if filter_parts else ""

    ws.merge_cells('A2:G2')
    ws['A2'] = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}{filter_text}"

    headers = [
        ("Product Name", 50), ("SKU", 25), ("Category", 18),
        ("Units Sold", 12), ("Revenue (PLN)", 16),
        ("Current Stock", 14), ("Image URL", 40)
    ]

    for col, (header, width) in enumerate(headers, 1):
        cell = ws.cell(row=4, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = thin_border
        ws.column_dimensions[get_column_letter(col)].width = width

    for row_idx, product in enumerate(products, 5):
        ws.cell(row=row_idx, column=1, value=product['product_name'][:80]).border = thin_border
        ws.cell(row=row_idx, column=2, value=product['sku']).border = thin_border
        ws.cell(row=row_idx, column=3, value=product.get('category', 'Other')).border = thin_border
        units_cell = ws.cell(row=row_idx, column=4, value=product['units_sold'])
        units_cell.border = thin_border
        units_cell.alignment = Alignment(horizontal='center')
        rev_cell = ws.cell(row=row_idx, column=5, value=round(product.get('total_revenue', 0), 2))
        rev_cell.border = thin_border
        rev_cell.alignment = Alignment(horizontal='right')
        rev_cell.number_format = '#,##0.00'
        stock_cell = ws.cell(row=row_idx, column=6, value=product['current_stock'])
        stock_cell.border = thin_border
        stock_cell.alignment = Alignment(horizontal='center')
        if product['current_stock'] == 0:
            stock_cell.font = Font(bold=True, color="FF0000")
        elif product['current_stock'] < 5:
            stock_cell.font = Font(color="FF6600")
        ws.cell(row=row_idx, column=7, value=product.get('image_url', '') or '').border = thin_border

    summary_row = len(products) + 5
    ws.cell(row=summary_row, column=1, value=f"TOTAL: {len(products)} variants").font = Font(bold=True)
    ws.cell(row=summary_row, column=4, value=sum(p['units_sold'] for p in products)).font = Font(bold=True)
    ws.cell(row=summary_row, column=5, value=round(sum(p.get('total_revenue', 0) for p in products), 2)).font = Font(bold=True)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"baselinker_sales_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename={filename}"})


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
