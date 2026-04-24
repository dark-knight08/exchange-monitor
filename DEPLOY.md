# HKEX Monitor - Deployment Guide

## Quick Start Options

### Option 1: Docker Compose (Recommended)

The easiest way to deploy the full system:

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

Access the dashboard at `http://your-server-ip:8000`

---

### Option 2: Free Cloud Hosting - Railway/Render

#### Railway.app (Recommended for beginners)

1. Push your code to GitHub
2. Go to [railway.app](https://railway.app) and sign in with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your `hkex-monitor` repository
5. Railway will auto-detect the Dockerfile
6. Add environment variables if needed:
   - `DATABASE_PATH`: `/app/data/hkex_monitor.db`
7. Deploy! Railway gives you a free HTTPS URL

#### Render.com

1. Push code to GitHub
2. Go to [render.com](https://render.com) → "New Web Service"
3. Connect your GitHub repo
4. Select "Docker" environment
5. Set start command: `python main.py run --host 0.0.0.0 --port 8000`
6. Deploy

---

### Option 3: VPS (DigitalOcean, AWS, Hetzner)

#### Ubuntu Server Setup

```bash
# 1. SSH into your server
ssh root@your-server-ip

# 2. Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# 3. Clone your repository
git clone https://github.com/yourusername/hkex-monitor.git
cd hkex-monitor

# 4. Start with Docker Compose
docker-compose up -d

# 5. (Optional) Setup Nginx for port 80/443
sudo apt install nginx
sudo cp nginx.conf /etc/nginx/sites-available/hkex-monitor
sudo ln -s /etc/nginx/sites-available/hkex-monitor /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

#### Add HTTPS with Let's Encrypt

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

---

### Option 4: Fly.io (Free Tier)

```bash
# Install flyctl
brew install flyctl  # macOS
# or see https://fly.io/docs/hands-on/install-flyctl/

# Login
fly auth login

# Launch (from project directory)
cd hkex-monitor
fly launch

# Follow prompts - fly will detect Dockerfile
# Deploy
fly deploy
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_PATH` | `./data/hkex_monitor.db` | SQLite database location |
| `API_PORT` | `8000` | API server port |
| `LOG_LEVEL` | `INFO` | Logging level |

### API Endpoints

Once deployed, your API is available at:

- `GET /api/v1/assets/{asset_class}` - List assets (equity, etf, derivative, cbbc, warrant)
- `GET /api/v1/snapshot/current` - Current market snapshot
- `GET /api/v1/daily/{YYYY-MM-DD}` - Historical daily data
- `GET /api/v1/changes/daily` - Daily price changes
- `GET /api/v1/alerts` - Market alerts
- `GET /api/v1/summary` - Market summary
- `GET /api/v1/search?query=xxx` - Symbol search
- `WS /ws/market` - WebSocket for real-time updates

---

## Production Checklist

- [ ] Change CORS settings in `src/api/server.py` (line 34: `allow_origins=["*"]`)
- [ ] Use a proper database (PostgreSQL) instead of SQLite for high traffic
- [ ] Add authentication if needed
- [ ] Setup monitoring and logging (DataDog, Sentry)
- [ ] Configure backup for database
- [ ] Use a process manager (systemd/supervisor) without Docker

---

## Troubleshooting

### Dashboard shows "Disconnected"

- Check API is running: `curl http://localhost:8000/api/v1/health`
- Check firewall allows port 8000
- Verify `API_BASE_URL` in `web/index.html` matches your server

### Database locked errors

- SQLite doesn't handle concurrent writes well
- Consider migrating to PostgreSQL for production

### Data not updating

- The scheduler runs during HKEX market hours (9:30-16:30 HKT)
- Check logs: `docker-compose logs -f`
- Run manual collection: `python main.py collect`
