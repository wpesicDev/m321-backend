# m321-backend

FastAPI backend for the m321 sensor network.

## Local development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make run-api
```

The API is then available on http://127.0.0.1:8000.

## Deployment (systemd)

The repo ships a `m321-backend.service` unit that runs uvicorn on `0.0.0.0:8000` as the `admin` user from `/home/admin/m321-backend`. Adjust `User=`, `WorkingDirectory=`, `EnvironmentFile=` and `ExecStart=` if your layout differs.

### 1. Get the code on the host

```bash
git clone https://github.com/wpesicDev/m321-backend.git
cd m321-backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file alongside the code if you need env vars (it is optional — the unit uses `EnvironmentFile=-` so missing is fine).

### 2. Install the unit

```bash
sudo cp m321-backend.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now m321-backend
```

### 3. Manage the service

```bash
sudo systemctl status m321-backend     # check status
sudo systemctl restart m321-backend    # restart after pulling new code
sudo systemctl stop m321-backend
journalctl -u m321-backend -f          # live logs
```

### Updating

```bash
cd ~/m321-backend
git pull
source .venv/bin/activate
pip install -r requirements.txt        # only if deps changed
sudo systemctl restart m321-backend
```
