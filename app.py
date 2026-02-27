#!/usr/bin/env python3
"""
BaseLinker Sales Dashboard - FastAPI Application
Displays all sold products with real-time updates, Excel export, and activity tracking
Enhanced: category detection, purchase orders, revenue tracking, sales channels
"""

import os
import re
import json
import time
import asyncio
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
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

# Data cache
cache = {
    "data": None,
    "last_updated": None,
    "next_refresh": None,
    "is_refreshing": False,
    "purchase_orders": []
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


def aggregate_sales(orders: list) -> dict:
    """Aggregate sales by variant with revenue and channel tracking"""
    sales_by_variant = defaultdict(lambda: {
        'product_name': '',
        'sku': '',
        'units_sold': 0,
        'bl_product_id': '',
        'total_revenue': 0.0,
        'sales_by_channel': defaultdict(int)
    })

    for order in orders:
        # Detect sales channel
        source = order.get('order_source', 'unknown')
        if 'allegro' in source.lower():
            channel = 'Allegro'
        elif 'shopify' in source.lower() or source == 'shop':
            channel = 'Shopify'
        elif 'olx' in source.lower():
            channel = 'OLX'
        else:
            channel = source

        for product in order.get('products', []):
            bl_product_id = str(product.get('variant_id', ''))
            if not bl_product_id or bl_product_id == '0':
                continue

            sku = product.get('sku', '') or ''
            name = product.get('name', '') or 'Unknown'
            qty = int(product.get('quantity', 1))
            price = float(product.get('price_brutto', 0))

            variant_key = sku if sku else bl_product_id

            sales_by_variant[variant_key]['product_name'] = name
            sales_by_variant[variant_key]['sku'] = sku
            sales_by_variant[variant_key]['units_sold'] += qty
            sales_by_variant[variant_key]['bl_product_id'] = bl_product_id
            sales_by_variant[variant_key]['total_revenue'] += price * qty
            sales_by_variant[variant_key]['sales_by_channel'][channel] += qty

    # Convert defaultdicts to regular dicts for JSON serialization
    result = {}
    for key, val in sales_by_variant.items():
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


def refresh_data():
    """Fetch fresh data from BaseLinker"""
    if cache["is_refreshing"]:
        return cache["data"]

    cache["is_refreshing"] = True

    try:
        # Fetch orders and aggregate sales
        orders = fetch_all_wyslane_orders()
        sales_data = aggregate_sales(orders)
        product_ids = [d['bl_product_id'] for d in sales_data.values() if d['bl_product_id']]
        inventory = get_inventory(product_ids)

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

        print(f"Fetched {len(po_list)} purchase orders")

        # Build final products list
        products = []
        total_revenue = 0.0

        for variant_key, data in sales_data.items():
            bl_pid = data['bl_product_id']
            inv_info = inventory.get(bl_pid, {})
            revenue = round(data['total_revenue'], 2)
            total_revenue += revenue

            products.append({
                'image_url': inv_info.get('image_url', ''),
                'product_name': data['product_name'],
                'sku': data['sku'],
                'units_sold': data['units_sold'],
                'current_stock': inv_info.get('stock', 0),
                'total_revenue': revenue,
                'sales_by_channel': data['sales_by_channel'],
                'category': determine_category(data['product_name']),
                'shopify_variant_id': inv_info.get('shopify_variant_id', ''),
                'bl_product_id': bl_pid,
                'purchase_orders': po_items_map.get(bl_pid, [])
            })

        products.sort(key=lambda x: x['units_sold'], reverse=True)

        now = datetime.now(timezone.utc)
        cache["data"] = {
            "total_variants": len(products),
            "total_units_sold": sum(p['units_sold'] for p in products),
            "total_orders": len(orders),
            "total_revenue": round(total_revenue, 2),
            "products": products
        }
        cache["last_updated"] = now.isoformat()
        cache["next_refresh"] = (now + timedelta(seconds=REFRESH_INTERVAL)).isoformat()
        cache["purchase_orders"] = po_list

        # Log to database
        log_activity("refresh", cache["data"])
        save_sales_snapshot(products)

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
async def get_sales():
    if cache["data"] is None:
        return JSONResponse(status_code=503, content={"error": "Data not loaded yet. Please wait..."})

    return {
        "last_updated": cache["last_updated"],
        "next_refresh": cache["next_refresh"],
        "is_refreshing": cache["is_refreshing"],
        **cache["data"]
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


@app.get("/api/download")
async def download_excel(
    category: str = Query("", description="Filter by category"),
    po: str = Query("", description="Filter by purchase order ID"),
    search: str = Query("", description="Search text or Shopify URL")
):
    if cache["data"] is None:
        return JSONResponse(status_code=503, content={"error": "Data not loaded yet"})

    # Apply same filters as frontend
    products = filter_products(cache["data"]["products"], category, po, search)

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
