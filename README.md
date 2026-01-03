# BaseLinker Sales Dashboard

Real-time sales dashboard for BaseLinker showing all shipped orders (Wysłane) with auto-refresh and Excel export.

## Features

- Real-time sales data from BaseLinker API
- Auto-refresh every 5 minutes
- Manual refresh button
- Excel export functionality
- Product images, SKU, units sold, current stock
- Sortable columns
- Stock level highlighting (red = 0, orange = <5)

## Deployment to Railway

1. Create a new project on [Railway](https://railway.app)
2. Connect this GitHub repository
3. Set environment variables:
   - `BASELINKER_API_KEY` - Your BaseLinker API token
   - `BASELINKER_INVENTORY_ID` - Your inventory ID (default: 58952)
4. Deploy!

## Local Development

```bash
pip install -r requirements.txt
python app.py
```

Open http://localhost:8000
