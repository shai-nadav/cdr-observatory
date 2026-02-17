# CDR Processor Observatory

Web-based viewer and tracer for Mitel OSV CDR processing. Upload SIP endpoints + raw CDR files, process them server-side through the CDR Processor engine, and explore decoded CDRs, legs, SIP endpoints, and processing logs — all in the browser.

## Features

- **Server-side processing** — upload raw `.csv`/`.bf` CDR files + SIP endpoints XML, process via the real `CdrProcessorEngine`
- **Viewer mode** — load pre-processed decoded CDRs, legs CSVs, and engine logs
- **Tracer integration** — `IProcessingTracer` captures direction reasoning, suppressed legs, field mappings, merges, SIP classification, transfers
- **Cross-correlation** — click any CDR/leg row to highlight related rows across all tabs + log lines (by ThreadId/SourceLine)
- **Color-coded** — thread IDs, extensions, SIP endpoints each get consistent colors; unknown SIPs highlighted in red
- **Export** — CSV export from any filtered view

## Requirements

- .NET 8.0 SDK

## Quick Start

```bash
git clone https://github.com/shain73/cdr-observatory.git
cd cdr-observatory
dotnet run
```

By default listens on `http://localhost:5000`. Override with:

```bash
ASPNETCORE_URLS=http://0.0.0.0:8090 dotnet run
```

Open the URL in your browser, upload SIP endpoints + CDR files, hit **Process**.

## Deployment (Linux systemd)

1. Clone and build:
```bash
cd /opt/cdr-observatory
dotnet build
```

2. Create `/etc/systemd/system/cdr-observatory.service`:
```ini
[Unit]
Description=CDR Observatory
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/cdr-observatory
ExecStart=/usr/bin/dotnet run --project /opt/cdr-observatory/CdrObservatory.csproj
Environment=ASPNETCORE_URLS=http://0.0.0.0:8090
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

3. Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cdr-observatory
```

### HTTPS with Caddy (reverse proxy)

```
osv.example.com {
    reverse_proxy localhost:8090
}
```

## Project Structure

```
├── Program.cs                 # ASP.NET Minimal API — /api/process endpoint + static files
├── LogCollector.cs            # IProcessorLogger implementation that captures log lines
├── CdrObservatory.csproj      # .NET 8 Web SDK project
├── wwwroot/
│   └── index.html             # Single-page frontend (vanilla JS, no frameworks)
└── src/OSVParser/             # CDR Processor engine source
    ├── Processing/
    │   ├── CdrProcessorEngine.cs
    │   ├── IProcessingTracer.cs
    │   ├── ProcessingTracer.cs
    │   ├── DirectionResolver.cs
    │   └── ...
    ├── Config/
    ├── Models/
    └── Adapters/
```

## API

### `POST /api/process`

Multipart form upload:
- `sipEndpointsFile` — SIP endpoints XML
- `cdrFiles` — one or more raw CDR files (.csv/.bf)

Returns JSON with `decodedCdrs`, `legs`, `sipEndpoints`, `log`, `tracerData`, and `stats`.
