# HKEX Multi-Asset Market Monitor

A comprehensive market monitoring system for the Hong Kong Stock Exchange (HKEX), tracking multiple asset classes with real-time data collection, daily change tracking, and alerting.

## Features

### Asset Class Coverage
- **Equities**: Top 50 most liquid Hong Kong listed stocks
- **ETFs**: Exchange traded funds with NAV and AUM tracking
- **Derivatives**: HSI/HSCEI futures and single stock options
- **CBBCs**: Callable Bull/Bear Contracts with call level monitoring
- **Warrants**: Equity warrants with Greeks and premium tracking

### Data Collection
- Real-time snapshots every 30 seconds during market hours
- End-of-day data collection at market close
- Historical data storage and retrieval
- Automatic daily change calculations

### Monitoring & Alerts
- Price movement alerts (±5% threshold)
- Volume spike detection
- Market-wide summary statistics
- WebSocket real-time updates

### API & Dashboard
- RESTful API with filtering and sorting
- WebSocket for live market updates
- Interactive web dashboard with:
  - Asset class filtering
  - Performance charts
  - Top gainers/losers
  - Volume analysis
  - Alert notifications

## Quick Start

### 1. Installation

```bash
cd hkex-monitor

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Initialize Database

```bash
python main.py init
```

### 3. Run Data Collection

```bash
# One-time collection for all asset classes
python main.py collect

# Collection for specific asset classes
python main.py collect --asset-classes equity etf
```

### 4. Start the API Server

```bash
python main.py api
```

The API will be available at http://localhost:8000

### 5. Start Full System (Scheduler + API)

```bash
python main.py run
```

### 6. View Dashboard

Open `web/index.html` in your browser:

```bash
# Using Python's simple HTTP server
cd web && python3 -m http.server 8080
```

Then open http://localhost:8080

## API Endpoints

### Market Data
- `GET /api/v1/assets/{asset_class}` - List assets by class
- `GET /api/v1/snapshot/current` - Current market snapshot
- `GET /api/v1/daily/{date}` - End-of-day data for date (YYYY-MM-DD)

### Changes & Analysis
- `GET /api/v1/changes/daily` - Daily price changes
- `GET /api/v1/history/{symbol}` - Historical data for symbol
- `GET /api/v1/summary` - Market summary by asset class

### Alerts
- `GET /api/v1/alerts` - Active market alerts

### WebSocket
- `WS /ws/market` - Real-time market updates

### Examples

```bash
# Get all equities
curl http://localhost:8000/api/v1/assets/equity

# Get daily changes
curl http://localhost:8000/api/v1/changes/daily

# Get symbol history
curl http://localhost:8000/api/v1/history/0700.HK?days=30
```

## Scheduled Collection

The system can automatically collect data at scheduled times:

```bash
# Start scheduler only
python main.py schedule

# Start full system (scheduler + API)
python main.py run
```

### Default Schedule (HKEX Trading Hours)
- **08:45** - Pre-market snapshot
- **09:30** - Market open snapshot
- **10:45** - Mid-morning snapshot
- **11:45** - Pre-lunch snapshot
- **13:00** - Post-lunch snapshot
- **14:30** - Mid-afternoon snapshot
- **15:45** - Pre-close snapshot
- **16:30** - Daily close (EOD data collection)
- Every **30 seconds** - Real-time updates during market hours

## Configuration

Edit `config/settings.yaml` to customize:

- Asset classes and metrics to track
- Collection schedules
- Alert thresholds
- API settings
- Notification channels

## Project Structure

```
hkex-monitor/
├── config/              # Configuration files
├── data/                # SQLite database and backups
├── logs/                # Application logs
├── src/
│   ├── collectors/      # Data collection modules
│   │   ├── base.py
│   │   ├── equity_collector.py
│   │   ├── etf_collector.py
│   │   ├── derivative_collector.py
│   │   └── cbbc_collector.py
│   ├── database/        # Database models and data store
│   │   ├── models.py
│   │   └── data_store.py
│   ├── api/             # FastAPI server
│   │   └── server.py
│   ├── scheduler/       # Job scheduling
│   │   ├── job_scheduler.py
│   │   └── collection_jobs.py
│   └── alerts/          # Alert engine
│       └── alert_engine.py
├── web/                 # Dashboard frontend
│   └── index.html
├── main.py              # Application entry point
├── requirements.txt     # Python dependencies
└── README.md
```

## Database Schema

### Key Tables
- `assets` - Asset master data
- `market_snapshots` - Real-time market data
- `daily_bars` - End-of-day OHLCV data
- `daily_changes` - Pre-calculated daily changes
- `alerts` - Market alerts and notifications
- `asset_class_summaries` - Daily statistics by asset class
- `collection_logs` - Data collection audit trail

## Development

### Running Tests

```bash
pytest tests/
```

### Adding New Collectors

1. Create a new collector class inheriting from `BaseCollector`
2. Implement `get_asset_class()`, `fetch_top_liquid()`, and `fetch_snapshot()`
3. Register in `main.py` and `CollectionJobs`

### Custom Alert Handlers

```python
from src.alerts import AlertEngine, NotificationHandler

class MyNotificationHandler(NotificationHandler):
    async def __call__(self, alert_data):
        # Send notification via your preferred channel
        print(f"Alert: {alert_data['message']}")

# Register handler
engine = AlertEngine(data_store)
engine.register_handler(MyNotificationHandler())
```

## Production Deployment

### Docker (Optional)

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py", "run"]
```

### Systemd Service

Create `/etc/systemd/system/hkex-monitor.service`:

```ini
[Unit]
Description=HKEX Market Monitor
After=network.target

[Service]
Type=simple
User=hkex
WorkingDirectory=/opt/hkex-monitor
ExecStart=/opt/hkex-monitor/venv/bin/python main.py run
Restart=always

[Install]
WantedBy=multi-user.target
```

## License

MIT License - See LICENSE file

## Disclaimer

This system is for informational and educational purposes only. Market data is simulated for demonstration. For actual trading, please refer to official HKEX data sources.

## Support

For issues or questions, please open a GitHub issue or contact the maintainers.
# exchange-monitor
# exchange-monitor
# exchange-monitor
# exchange-monitor
