# Bare metal

Running the Smart Router binary directly on a VM or physical host. Best when you want minimum operational surface — one process, one config file, one systemd unit.

## Install the binary

```bash
git clone https://github.com/Magma-Devs/smart-router.git
cd smart-router
make install-all
```

This drops `smartrouter` and `lavap` into `$GOBIN` (typically `~/go/bin`). Copy them to a system location:

```bash
sudo install -m 0755 "$(go env GOPATH)/bin/smartrouter" /usr/local/bin/smartrouter
```

## Lay out the files

```
/etc/smartrouter/
├── config.yml                   # your YAML
└── specs/                       # copy of repo's specs/ directory
/var/lib/smartrouter/
└── (cache data, if cache uses persistent storage)
/var/log/smartrouter/
└── (only if you redirect logs here; systemd journal works fine)
```

## systemd unit — router

`/etc/systemd/system/smartrouter.service`:

```ini
[Unit]
Description=Smart Router
After=network-online.target
Wants=network-online.target
# If you also run the cache on this host:
After=smartrouter-cache.service
Requires=smartrouter-cache.service

[Service]
Type=simple
User=smartrouter
Group=smartrouter
ExecStart=/usr/local/bin/smartrouter rpcsmartrouter \
  /etc/smartrouter/config.yml \
  --geolocation 1 \
  --use-static-spec /etc/smartrouter/specs/ \
  --cache-be 127.0.0.1:7778 \
  --log_level info
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/log/smartrouter

[Install]
WantedBy=multi-user.target
```

## systemd unit — cache (optional but recommended)

`/etc/systemd/system/smartrouter-cache.service`:

```ini
[Unit]
Description=Smart Router cache
After=network-online.target

[Service]
Type=simple
User=smartrouter
Group=smartrouter
ExecStart=/usr/local/bin/smartrouter cache --port 7778
Restart=on-failure
RestartSec=5

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

## Create the user

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin smartrouter
sudo chown -R smartrouter:smartrouter /etc/smartrouter /var/lib/smartrouter
```

## Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now smartrouter-cache.service
sudo systemctl enable --now smartrouter.service

# Verify
systemctl status smartrouter
journalctl -u smartrouter -f
```

## Behind a reverse proxy

For TLS termination, gzip, and per-listener routing, put NGINX or HAProxy in front:

```nginx
upstream smartrouter_eth {
  server 127.0.0.1:3360;
}

server {
  listen 443 ssl http2;
  server_name rpc.example.com;

  ssl_certificate     /etc/letsencrypt/live/rpc.example.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/rpc.example.com/privkey.pem;

  location / {
    proxy_pass http://smartrouter_eth;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_read_timeout 60s;
  }
}
```

The router honours `X-Forwarded-For` for IP-based logic.

## Updating

```bash
cd /path/to/smart-router
git pull
make install-all
sudo install -m 0755 "$(go env GOPATH)/bin/smartrouter" /usr/local/bin/smartrouter
sudo systemctl restart smartrouter
```

For zero-downtime updates, run two router replicas behind the reverse proxy and restart them sequentially.
