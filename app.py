#!/usr/bin/env python3
"""
BaseLinker Sales Dashboard - FastAPI Application
Displays all sold products with real-time updates, Excel export, and activity tracking
"""

import os
import json
import time
import asyncio
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from io import BytesIO
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, BackgroundTasks
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
    "is_refreshing": False
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
        # Activity log table
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
        # Sales snapshot table (tracks changes over time)
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
        for p in products[:50]:  # Save top 50 products
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
    """Aggregate sales by variant"""
    sales_by_variant = defaultdict(lambda: {
        'product_name': '',
        'sku': '',
        'units_sold': 0,
        'bl_product_id': ''
    })

    for order in orders:
        for product in order.get('products', []):
            bl_product_id = str(product.get('variant_id', ''))
            if not bl_product_id or bl_product_id == '0':
                continue

            sku = product.get('sku', '') or ''
            name = product.get('name', '') or 'Unknown'
            qty = int(product.get('quantity', 1))

            variant_key = sku if sku else bl_product_id

            sales_by_variant[variant_key]['product_name'] = name
            sales_by_variant[variant_key]['sku'] = sku
            sales_by_variant[variant_key]['units_sold'] += qty
            sales_by_variant[variant_key]['bl_product_id'] = bl_product_id

    return dict(sales_by_variant)


def get_inventory(product_ids: list) -> dict:
    """Fetch current inventory for products"""
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

            inventory[pid_str] = {
                'stock': total_stock,
                'image_url': image_url
            }

        time.sleep(0.5)

    return inventory


def refresh_data():
    """Fetch fresh data from BaseLinker"""
    if cache["is_refreshing"]:
        return cache["data"]

    cache["is_refreshing"] = True

    try:
        orders = fetch_all_wyslane_orders()
        sales_data = aggregate_sales(orders)
        product_ids = [d['bl_product_id'] for d in sales_data.values() if d['bl_product_id']]
        inventory = get_inventory(product_ids)

        products = []
        for variant_key, data in sales_data.items():
            bl_pid = data['bl_product_id']
            inv_info = inventory.get(bl_pid, {})

            products.append({
                'image_url': inv_info.get('image_url', ''),
                'product_name': data['product_name'],
                'sku': data['sku'],
                'units_sold': data['units_sold'],
                'current_stock': inv_info.get('stock', 0)
            })

        products.sort(key=lambda x: x['units_sold'], reverse=True)

        now = datetime.now(timezone.utc)
        cache["data"] = {
            "total_variants": len(products),
            "total_units_sold": sum(p['units_sold'] for p in products),
            "total_orders": len(orders),
            "products": products
        }
        cache["last_updated"] = now.isoformat()
        cache["next_refresh"] = (now + timedelta(seconds=REFRESH_INTERVAL)).isoformat()

        # Log to database
        log_activity("refresh", cache["data"])
        save_sales_snapshot(products)

    except Exception as e:
        print(f"Error refreshing data: {e}")
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
async def download_excel():
    if cache["data"] is None:
        return JSONResponse(status_code=503, content={"error": "Data not loaded yet"})

    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Sales Report"

    header_font = Font(bold=True, size=11, color="FFFFFF")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    ws.merge_cells('A1:E1')
    ws['A1'] = "BASELINKER SALES REPORT - WYSLANE ORDERS"
    ws['A1'].font = Font(bold=True, size=14)

    ws.merge_cells('A2:E2')
    ws['A2'] = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    headers = [("Product Name", 50), ("SKU", 25), ("Units Sold", 12), ("Current Stock", 14), ("Image URL", 40)]

    for col, (header, width) in enumerate(headers, 1):
        cell = ws.cell(row=4, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = thin_border
        ws.column_dimensions[get_column_letter(col)].width = width

    for row_idx, product in enumerate(cache["data"]["products"], 5):
        ws.cell(row=row_idx, column=1, value=product['product_name'][:80]).border = thin_border
        ws.cell(row=row_idx, column=2, value=product['sku']).border = thin_border
        units_cell = ws.cell(row=row_idx, column=3, value=product['units_sold'])
        units_cell.border = thin_border
        units_cell.alignment = Alignment(horizontal='center')
        stock_cell = ws.cell(row=row_idx, column=4, value=product['current_stock'])
        stock_cell.border = thin_border
        stock_cell.alignment = Alignment(horizontal='center')
        if product['current_stock'] == 0:
            stock_cell.font = Font(bold=True, color="FF0000")
        elif product['current_stock'] < 5:
            stock_cell.font = Font(color="FF6600")
        ws.cell(row=row_idx, column=5, value=product['image_url'] or '').border = thin_border

    summary_row = len(cache["data"]["products"]) + 5
    ws.cell(row=summary_row, column=1, value=f"TOTAL: {cache['data']['total_variants']} variants").font = Font(bold=True)
    ws.cell(row=summary_row, column=3, value=cache['data']['total_units_sold']).font = Font(bold=True)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"baselinker_sales_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename={filename}"})


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
