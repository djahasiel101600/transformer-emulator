#!/usr/bin/env python3
"""ESP32 reading simulator for PoleTransMonitor.

This script simulates device readings by posting to the backend endpoint used by firmware:
POST /api/readings/

It supports:
- baseline_noise mode
- scenario_profiles mode
- csv_replay mode
- hybrid target loading (manual config + optional admin transformer discovery)
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import random
import signal
import socket
import ssl
import struct
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib import error, parse, request


DEFAULT_CONFIG = Path("tools/device_simulator/config.example.json")
CONDITION_CHOICES = {
    "normal",
    "heavy_peak_load",
    "danger_zone",
    "overload",
    "severe_overload",
    "heavy_load",
    "abnormal",
    "poor_power_quality",
    "critical",
}


class ConfigError(Exception):
    pass


class WsConnectError(Exception):
    pass


# ---------------------------------------------------------------------------
# Runtime parameter overrides + embedded browser control panel
# ---------------------------------------------------------------------------

_PANEL_HTML = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Simulator Control</title><style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;padding:1.5rem}h1{font-size:1.15rem;font-weight:600;margin-bottom:1.2rem}h2{font-size:.75rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-bottom:.6rem}.g{display:grid;grid-template-columns:1fr 1fr;gap:1.2rem}.card{background:#1e293b;border-radius:.5rem;padding:1rem}.note{font-size:.73rem;color:#64748b;margin-bottom:.7rem}.ch{display:grid;grid-template-columns:145px 1fr 1fr;gap:.4rem;margin-bottom:.25rem}.ch span{font-size:.68rem;color:#64748b;text-align:center}.ch span:first-child{text-align:left}.row{display:grid;grid-template-columns:145px 1fr 1fr;gap:.4rem;align-items:center;margin-bottom:.38rem}.lbl{font-size:.78rem;color:#cbd5e1}.unit{font-size:.68rem;color:#64748b}input[type=number]{width:100%;background:#0f172a;border:1px solid #334155;border-radius:.25rem;padding:.28rem .4rem;color:#f1f5f9;font-size:.78rem}input[type=number]:focus{outline:none;border-color:#3b82f6}.acts{display:flex;gap:.5rem;margin-top:.8rem}button{border:none;border-radius:.35rem;padding:.4rem 1rem;font-size:.82rem;cursor:pointer}.bp{background:#3b82f6;color:#fff}.bp:hover{background:#2563eb}.br{background:#334155;color:#cbd5e1}.br:hover{background:#475569}.msg{font-size:.75rem;min-height:1.1em;margin-top:.4rem}.ok{color:#22c55e}.er{color:#ef4444}table{width:100%;border-collapse:collapse;font-size:.73rem}th{text-align:left;color:#64748b;font-weight:500;padding:.22rem .38rem;border-bottom:1px solid #334155}td{padding:.22rem .38rem;border-bottom:1px solid #1e293b;color:#cbd5e1}tr:last-child td{border-bottom:none}.b{display:inline-block;padding:.1rem .32rem;border-radius:.2rem;font-size:.68rem;font-weight:500}.bn{background:#166534;color:#86efac}.bh{background:#713f12;color:#fde68a}.bo{background:#7c2d12;color:#fca5a5}.bx{background:#1e3a5f;color:#93c5fd}</style></head><body><h1>PoleTransMonitor &mdash; Simulator Control Panel</h1><div class="g">
<div class="card"><h2>Parameter Clamps</h2><p class="note">Leave blank to disable a clamp. Applied on top of normal reading generation.</p>
<div class="ch"><span>Parameter</span><span>Min</span><span>Max</span></div>
<form id="pf"><div id="rows"></div><div class="acts"><button type="submit" class="bp">Apply</button><button type="button" class="br" id="cb">Clear All</button></div>
<div id="msg" class="msg"></div></form></div>
<div class="card"><h2>Recent Readings</h2><div id="rd"><p class="note">Waiting for readings&hellip;</p></div></div>
</div><script>
const P=[
{k:"voltage",l:"Voltage",u:"V",n:0,x:400,s:1},
{k:"current",l:"Current",u:"A",n:0,x:500,s:0.1},
{k:"apparent_power",l:"Apparent Power",u:"VA",n:0,x:100000,s:100},
{k:"power_factor",l:"Power Factor",u:"",n:0,x:1,s:0.01},
{k:"frequency",l:"Frequency",u:"Hz",n:40,x:70,s:0.1},
{k:"oil_temp",l:"Oil Temp",u:"\u00b0C",n:0,x:200,s:0.5},
];let st={};
function build(p){const c=document.getElementById("rows");c.innerHTML="";
P.forEach(f=>{const r=document.createElement("div");r.className="row";
const lo=p[f.k+"_min"]??"",hi=p[f.k+"_max"]??"";
r.innerHTML='<span class="lbl">'+f.l+' <span class="unit">'+f.u+'</span></span>'+
'<input type="number" name="'+f.k+'_min" placeholder="\u2014" min="'+f.n+'" max="'+f.x+'" step="'+f.s+'" value="'+lo+'">'+
'<input type="number" name="'+f.k+'_max" placeholder="\u2014" min="'+f.n+'" max="'+f.x+'" step="'+f.s+'" value="'+hi+'">';
c.appendChild(r);});}
function bc(c){if(!c||c==="normal")return"bn";if(c.includes("overload")||c==="critical"||c==="severe_overload")return"bo";if(c.includes("heavy")||c==="danger_zone")return"bh";return"bx";}
function rend(a){if(!a||!a.length){document.getElementById("rd").innerHTML='<p class="note">No readings yet.</p>';return;}
let h='<table><thead><tr><th>Time</th><th>VA</th><th>V</th><th>A</th><th>PF</th><th>Temp</th><th>Condition</th></tr></thead><tbody>';
a.slice(-10).reverse().forEach(r=>{const t=r.ts?new Date(r.ts).toLocaleTimeString():"\u2014";
const c=r.reading?.condition||"\u2014";
h+='<tr><td>'+t+'</td><td>'+(r.reading?.apparent_power??"\u2014")+'</td><td>'+(r.reading?.voltage??"\u2014")+'</td><td>'+(r.reading?.current??"\u2014")+'</td><td>'+(r.reading?.power_factor??"\u2014")+'</td><td>'+(r.reading?.oil_temp??"\u2014")+'\u00b0</td><td><span class="b '+bc(c)+'">'+c+'</span></td></tr>';});
h+='</tbody></table>';document.getElementById("rd").innerHTML=h;}
async function lp(){try{const r=await fetch("/api/params");st=await r.json();build(st);}catch(e){}}
async function ls(){try{const r=await fetch("/api/status");const d=await r.json();rend(d.readings||[]);}catch(e){}}
document.getElementById("pf").addEventListener("submit",async e=>{
e.preventDefault();const fd=new FormData(e.target),p={};
P.forEach(f=>{
const lo=fd.get(f.k+"_min"),hi=fd.get(f.k+"_max");
p[f.k+"_min"]=(lo!=null&&lo!="")? parseFloat(lo):null;
p[f.k+"_max"]=(hi!=null&&hi!="")? parseFloat(hi):null;
});
const el=document.getElementById("msg");
try{const r=await fetch("/api/params",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(p)});
if(r.ok){st=p;el.textContent="Applied \u2713";el.className="msg ok";setTimeout(()=>{el.textContent="";},2500);}else{el.textContent="Error "+r.status;el.className="msg er";}
}catch(ex){el.textContent=""+ex;el.className="msg er";}
});
document.getElementById("cb").addEventListener("click",()=>{
const p={};P.forEach(f=>{p[f.k+"_min"]=null;p[f.k+"_max"]=null;});
fetch("/api/params",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(p)}).then(()=>lp());
});
lp();ls();setInterval(ls,5000);
</script></body></html>"""

_overrides: dict[str, float | None] = {
    "voltage_min": None, "voltage_max": None,
    "current_min": None, "current_max": None,
    "apparent_power_min": None, "apparent_power_max": None,
    "power_factor_min": None, "power_factor_max": None,
    "frequency_min": None, "frequency_max": None,
    "oil_temp_min": None, "oil_temp_max": None,
}
_override_lock = threading.Lock()
_last_readings: list[dict[str, Any]] = []
_last_readings_lock = threading.Lock()
_MAX_LAST_READINGS = 50


def _record_reading(transformer_id: int, reading: dict[str, Any]) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "transformer_id": transformer_id, "reading": reading}
    with _last_readings_lock:
        _last_readings.append(entry)
        if len(_last_readings) > _MAX_LAST_READINGS:
            _last_readings.pop(0)


def _apply_overrides(reading: dict[str, Any]) -> dict[str, Any]:
    """Apply runtime min/max clamps from the control panel to a generated reading."""
    with _override_lock:
        ovr = dict(_overrides)
    if not any(v is not None for v in ovr.values()):
        return reading
    result = dict(reading)

    def _cf(key: str, lo_k: str, hi_k: str) -> None:
        val = result.get(key)
        if val is None:
            return
        lo_v, hi_v = ovr.get(lo_k), ovr.get(hi_k)
        if lo_v is not None:
            val = max(float(lo_v), float(val))
        if hi_v is not None:
            val = min(float(hi_v), float(val))
        result[key] = val

    _cf("voltage", "voltage_min", "voltage_max")
    _cf("current", "current_min", "current_max")
    _cf("power_factor", "power_factor_min", "power_factor_max")
    _cf("frequency", "frequency_min", "frequency_max")
    _cf("oil_temp", "oil_temp_min", "oil_temp_max")

    # Recompute apparent_power from (possibly clamped) V and I.
    v = float(result.get("voltage") or 230.0)
    i_val = float(result.get("current") or 0.0)
    result["apparent_power"] = max(0.0, v * i_val)

    # Apply explicit apparent_power clamp and back-adjust current for consistency.
    ap_min = ovr.get("apparent_power_min")
    ap_max = ovr.get("apparent_power_max")
    if ap_min is not None:
        result["apparent_power"] = max(float(ap_min), result["apparent_power"])
    if ap_max is not None:
        result["apparent_power"] = min(float(ap_max), result["apparent_power"])
    result["current"] = (result["apparent_power"] / v) if v > 0 else 0.0

    # Recompute real_power.
    result["real_power"] = max(0.0, result["apparent_power"] * float(result.get("power_factor") or 1.0))

    # Round all fields.
    for _k, _d in [("voltage", 3), ("current", 3), ("apparent_power", 3), ("real_power", 3),
                   ("power_factor", 4), ("frequency", 3), ("oil_temp", 3)]:
        if _k in result:
            result[_k] = round(float(result[_k]), _d)

    return result


class SimControlHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for the browser control panel."""

    def log_message(self, fmt: str, *args: Any) -> None:  # type: ignore[override]
        pass  # suppress request logs

    def _send(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = parse.urlparse(self.path).path
        if path == "/":
            self._send(200, "text/html; charset=utf-8", _PANEL_HTML.encode("utf-8"))
        elif path == "/api/params":
            with _override_lock:
                body = json.dumps(dict(_overrides)).encode("utf-8")
            self._send(200, "application/json", body)
        elif path == "/api/status":
            with _last_readings_lock:
                body = json.dumps({"readings": list(_last_readings)}).encode("utf-8")
            self._send(200, "application/json", body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self) -> None:
        if parse.urlparse(self.path).path != "/api/params":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length") or "0")
        body_bytes = self.rfile.read(length) if length > 0 else b""
        try:
            data = json.loads(body_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.send_response(400)
            self.end_headers()
            return
        with _override_lock:
            for key in list(_overrides):
                if key in data:
                    val = data[key]
                    _overrides[key] = float(val) if val is not None else None
        self._send(200, "application/json", json.dumps({"ok": True}).encode("utf-8"))


def _start_control_panel(port: int) -> None:
    server = HTTPServer(("127.0.0.1", port), SimControlHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="sim-control-panel")
    t.start()
    print(f"[INFO] Control panel: http://localhost:{port}/")


class ApiClient:
    def __init__(self, backend_url: str, timeout_seconds: float, access_token: str | None = None):
        self.backend_url = backend_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.access_token = access_token

    def _api_url(self, api_path: str) -> str:
        # Accept either backend root (http://host:8000) or direct /api URL.
        if self.backend_url.endswith("/api"):
            return f"{self.backend_url}{api_path}"
        return f"{self.backend_url}/api{api_path}"

    def _request(
        self,
        method: str,
        api_path: str,
        payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[int, Any, str]:
        url = self._api_url(api_path)
        body = None
        headers: dict[str, str] = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        if extra_headers:
            headers.update(extra_headers)

        req = request.Request(url=url, data=body, method=method.upper(), headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                parsed = _safe_json(raw)
                return int(resp.status), parsed, raw
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            parsed = _safe_json(raw)
            return int(exc.code), parsed, raw

    def obtain_access_token(self, username: str, password: str) -> str:
        code, data, raw = self._request(
            "POST",
            "/token/",
            payload={"username": username, "password": password},
        )
        if code != 200 or not isinstance(data, dict) or not data.get("access"):
            raise RuntimeError(f"Token request failed (HTTP {code}): {raw[:240]}")
        token = str(data["access"])
        self.access_token = token
        return token

    def fetch_transformers(self) -> list[dict[str, Any]]:
        code, data, raw = self._request("GET", "/transformers/")
        if code != 200:
            raise RuntimeError(f"Transformer discovery failed (HTTP {code}): {raw[:240]}")

        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]

        if isinstance(data, dict):
            results = data.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]

        raise RuntimeError("Unexpected transformers payload shape")

    def health(self) -> tuple[int, Any, str]:
        return self._request("GET", "/health/")

    def me(self) -> tuple[int, Any, str]:
        return self._request("GET", "/me/")

    def post_reading(self, payload: dict[str, Any]) -> tuple[int, Any, str]:
        return self._request("POST", "/readings/", payload=payload)


class _MinimalWsClient:
    """Minimal RFC 6455 WebSocket client — stdlib only, no external deps.

    Connects to a ws:// or wss:// URL, sends JSON text frames (client-masked
    per spec), and supports a clean close. Intended for the device simulator.
    """

    def __init__(self, url: str):
        self._url = url
        self._sock: socket.socket | None = None
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self) -> None:
        parsed = parse.urlparse(self._url)
        scheme = parsed.scheme  # "ws" or "wss"
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        raw_sock = socket.create_connection((host, port), timeout=10)
        if scheme == "wss":
            ctx = ssl.create_default_context()
            sock: socket.socket = ctx.wrap_socket(raw_sock, server_hostname=host)
        else:
            sock = raw_sock

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        handshake = "\r\n".join([
            f"GET {path} HTTP/1.1",
            f"Host: {host}:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            "",
            "",
        ])
        sock.sendall(handshake.encode("utf-8"))

        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                sock.close()
                raise WsConnectError("Connection closed during WebSocket handshake")
            response += chunk

        status_line = response.split(b"\r\n", 1)[0].decode("utf-8", errors="replace")
        if "101" not in status_line:
            sock.close()
            raise WsConnectError(f"WebSocket upgrade failed: {status_line}")

        expected_accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
        ).decode("ascii")
        if expected_accept not in response.decode("utf-8", errors="replace"):
            sock.close()
            raise WsConnectError("Invalid Sec-WebSocket-Accept in handshake response")

        self._sock = sock
        self._connected = True

    def send_json(self, data: dict[str, Any]) -> None:
        if not self._connected or self._sock is None:
            raise WsConnectError("WebSocket not connected")
        self._send_frame(0x01, json.dumps(data).encode("utf-8"))

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        # RFC 6455 §5.3: client-to-server frames MUST be masked.
        length = len(payload)
        masking_key = os.urandom(4)
        masked = bytes(b ^ masking_key[i % 4] for i, b in enumerate(payload))
        frame = bytearray()
        frame.append(0x80 | opcode)  # FIN=1 + opcode
        if length < 126:
            frame.append(0x80 | length)  # MASK=1 + 7-bit length
        elif length < 65536:
            frame.append(0x80 | 126)
            frame.extend(struct.pack(">H", length))
        else:
            frame.append(0x80 | 127)
            frame.extend(struct.pack(">Q", length))
        frame.extend(masking_key)
        frame.extend(masked)
        assert self._sock is not None
        self._sock.sendall(bytes(frame))

    def close(self) -> None:
        self._connected = False
        if self._sock is not None:
            try:
                self._send_frame(0x08, b"")  # close frame
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


@dataclass
class TargetSpec:
    transformer_id: int
    interval_seconds: float
    mode: str
    seed: int | None
    rated_kva: float
    enabled: bool
    config: dict[str, Any]
    transport: str = "http"


class BaselineNoiseEngine:
    def __init__(self, target: TargetSpec):
        self.target = target
        self.rng = random.Random(target.seed)
        baseline = target.config.get("baseline", {})
        noise = target.config.get("noise", {})

        self.v_nom = float(baseline.get("voltage", 230.0))
        self.i_nom = float(baseline.get("current", 25.0))
        self.pf_nom = float(baseline.get("power_factor", 0.95))
        self.f_nom = float(baseline.get("frequency", 60.0))
        self.t_nom = float(baseline.get("oil_temp", 43.0))
        self.energy_kwh = float(baseline.get("energy_kwh_start", 0.0))

        self.v_noise = float(noise.get("voltage", 2.0))
        self.i_noise = float(noise.get("current", 2.0))
        self.pf_noise = float(noise.get("power_factor", 0.02))
        self.f_noise = float(noise.get("frequency", 0.04))
        self.t_noise = float(noise.get("oil_temp", 0.5))

    def next_reading(self, dt_seconds: float) -> dict[str, Any]:
        voltage = max(80.0, self.v_nom + self.rng.uniform(-self.v_noise, self.v_noise))
        current = max(0.0, self.i_nom + self.rng.uniform(-self.i_noise, self.i_noise))
        apparent_power = max(0.0, voltage * current)
        power_factor = _clamp(self.pf_nom + self.rng.uniform(-self.pf_noise, self.pf_noise), 0.55, 1.0)
        real_power = max(0.0, apparent_power * power_factor)
        frequency = max(40.0, self.f_nom + self.rng.uniform(-self.f_noise, self.f_noise))
        oil_temp = max(10.0, self.t_nom + self.rng.uniform(-self.t_noise, self.t_noise))

        self.energy_kwh += (real_power / 1000.0) * (max(0.01, dt_seconds) / 3600.0)

        condition = _condition_from_loading(apparent_power, self.target.rated_kva)

        return {
            "voltage": round(voltage, 3),
            "current": round(current, 3),
            "apparent_power": round(apparent_power, 3),
            "real_power": round(real_power, 3),
            "power_factor": round(power_factor, 4),
            "frequency": round(frequency, 3),
            "oil_temp": round(oil_temp, 3),
            "energy_kwh": round(self.energy_kwh, 6),
            "condition": condition,
        }


class ScenarioProfilesEngine:
    def __init__(self, target: TargetSpec):
        self.target = target
        self.rng = random.Random(target.seed)
        scenario = target.config.get("scenario", {})
        steps = scenario.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ConfigError(
                f"Target {target.transformer_id}: scenario_profiles mode requires scenario.steps"
            )

        self.steps: list[dict[str, Any]] = []
        for idx, step in enumerate(steps, start=1):
            if not isinstance(step, dict):
                raise ConfigError(f"Target {target.transformer_id}: step {idx} must be an object")
            cond = str(step.get("condition", "normal"))
            if cond not in CONDITION_CHOICES:
                raise ConfigError(
                    f"Target {target.transformer_id}: invalid condition '{cond}' in scenario step {idx}"
                )
            duration = float(step.get("duration_seconds", 60))
            if duration <= 0:
                raise ConfigError(
                    f"Target {target.transformer_id}: scenario step {idx} duration_seconds must be > 0"
                )
            self.steps.append({"condition": cond, "duration_seconds": duration, "overrides": step})

        self.repeat = bool(scenario.get("repeat", True))
        self.current_step_index = 0
        self.current_step_elapsed = 0.0
        self.energy_kwh = float(scenario.get("energy_kwh_start", 0.0))

    def next_reading(self, dt_seconds: float) -> dict[str, Any]:
        step = self.steps[self.current_step_index]
        cond = step["condition"]

        defaults = _defaults_for_condition(cond, self.target.rated_kva)
        overrides = step.get("overrides", {})
        base = {
            "voltage": float(overrides.get("voltage", defaults["voltage"])),
            "current": float(overrides.get("current", defaults["current"])),
            "power_factor": float(overrides.get("power_factor", defaults["power_factor"])),
            "frequency": float(overrides.get("frequency", defaults["frequency"])),
            "oil_temp": float(overrides.get("oil_temp", defaults["oil_temp"])),
        }

        noise_scale = float(overrides.get("noise_scale", 1.0))
        voltage = max(80.0, base["voltage"] + self.rng.uniform(-1.6, 1.6) * noise_scale)
        current = max(0.0, base["current"] + self.rng.uniform(-1.1, 1.1) * noise_scale)
        apparent_power = max(0.0, voltage * current)
        power_factor = _clamp(base["power_factor"] + self.rng.uniform(-0.015, 0.015), 0.5, 1.0)
        real_power = max(0.0, apparent_power * power_factor)
        frequency = max(40.0, base["frequency"] + self.rng.uniform(-0.05, 0.05) * noise_scale)
        oil_temp = max(10.0, base["oil_temp"] + self.rng.uniform(-0.35, 0.35) * noise_scale)

        self.energy_kwh += (real_power / 1000.0) * (max(0.01, dt_seconds) / 3600.0)

        self.current_step_elapsed += dt_seconds
        if self.current_step_elapsed >= float(step["duration_seconds"]):
            self.current_step_elapsed = 0.0
            self.current_step_index += 1
            if self.current_step_index >= len(self.steps):
                self.current_step_index = 0 if self.repeat else len(self.steps) - 1

        return {
            "voltage": round(voltage, 3),
            "current": round(current, 3),
            "apparent_power": round(apparent_power, 3),
            "real_power": round(real_power, 3),
            "power_factor": round(power_factor, 4),
            "frequency": round(frequency, 3),
            "oil_temp": round(oil_temp, 3),
            "energy_kwh": round(self.energy_kwh, 6),
            "condition": cond,
        }


class CsvReplayEngine:
    def __init__(self, target: TargetSpec):
        csv_cfg = target.config.get("csv", {})
        csv_path = Path(str(csv_cfg.get("path", ""))).expanduser()
        if not csv_path.is_absolute():
            cfg_path = Path(target.config["_config_path"]).resolve().parent
            csv_path = (cfg_path / csv_path).resolve()

        if not csv_path.exists():
            raise ConfigError(f"Target {target.transformer_id}: csv file not found: {csv_path}")

        self.rows = _load_csv_rows(csv_path, target.transformer_id)
        self.loop = bool(csv_cfg.get("loop", True))
        self.cursor = 0
        self.energy_kwh = float(csv_cfg.get("energy_kwh_start", 0.0))

    def next_reading(self, dt_seconds: float) -> dict[str, Any]:
        if not self.rows:
            raise RuntimeError("CSV replay has no rows")

        row = self.rows[self.cursor]
        self.cursor += 1
        if self.cursor >= len(self.rows):
            if self.loop:
                self.cursor = 0
            else:
                self.cursor = len(self.rows) - 1

        voltage = _must_float(row, "voltage")
        current = _must_float(row, "current")
        apparent_power = row.get("apparent_power")
        if apparent_power is None:
            apparent_power = voltage * current
        else:
            apparent_power = float(apparent_power)

        power_factor = row.get("power_factor")
        if power_factor is None:
            power_factor = 0.94
        power_factor = _clamp(float(power_factor), 0.0, 1.0)

        real_power = row.get("real_power")
        if real_power is None:
            real_power = apparent_power * power_factor
        else:
            real_power = max(0.0, float(real_power))

        frequency = float(row.get("frequency") or 60.0)
        oil_temp = float(row.get("oil_temp") or 42.0)

        incoming_energy = row.get("energy_kwh")
        if incoming_energy is None:
            self.energy_kwh += (real_power / 1000.0) * (max(0.01, dt_seconds) / 3600.0)
            energy_kwh = self.energy_kwh
        else:
            energy_kwh = max(0.0, float(incoming_energy))
            self.energy_kwh = energy_kwh

        condition = str(row.get("condition") or "normal")
        if condition not in CONDITION_CHOICES:
            condition = "normal"

        return {
            "voltage": round(voltage, 3),
            "current": round(current, 3),
            "apparent_power": round(apparent_power, 3),
            "real_power": round(real_power, 3),
            "power_factor": round(power_factor, 4),
            "frequency": round(frequency, 3),
            "oil_temp": round(oil_temp, 3),
            "energy_kwh": round(energy_kwh, 6),
            "condition": condition,
        }


class ClockScheduleEngine:
    """Simulates readings based on the current local wall-clock time.

    Each window in ``clock.windows`` specifies a time range (``from`` / ``to``
    in ``"HH:MM"`` 24-hour format) and one or more weighted conditions that are
    randomly sampled on every reading.  Windows are matched in order; the first
    one whose range covers the current time is used.  If no window matches, the
    ``clock.default_condition`` is used (default: ``"normal"``).

    Example config::

        "clock": {
            "timezone": "local",          // "local" or "utc" (default: "local")
            "default_condition": "normal",
            "windows": [
                { "from": "07:00", "to": "15:00", "conditions": [
                    { "condition": "normal", "weight": 1 }
                ]},
                { "from": "15:00", "to": "20:00", "conditions": [
                    { "condition": "normal",   "weight": 3 },
                    { "condition": "overload", "weight": 1 }
                ]},
                { "from": "20:00", "to": "23:00", "conditions": [
                    { "condition": "heavy_load", "weight": 1 }
                ]}
            ]
        }

    Overnight windows (``from`` > ``to``, e.g. ``"22:00"`` to ``"06:00"``) are
    supported automatically.
    """

    def __init__(self, target: TargetSpec):
        self.target = target
        self.rng = random.Random(target.seed)
        clock_cfg = target.config.get("clock", {})
        if not isinstance(clock_cfg, dict) or not clock_cfg.get("windows"):
            raise ConfigError(
                f"Target {target.transformer_id}: clock_schedule mode requires clock.windows"
            )

        tz_setting = str(clock_cfg.get("timezone", "local")).lower()
        if tz_setting not in {"local", "utc"}:
            raise ConfigError(
                f"Target {target.transformer_id}: clock.timezone must be 'local' or 'utc'"
            )
        self.use_utc = tz_setting == "utc"
        self.default_condition = str(clock_cfg.get("default_condition", "normal"))
        if self.default_condition not in CONDITION_CHOICES:
            raise ConfigError(
                f"Target {target.transformer_id}: clock.default_condition '{self.default_condition}' is not valid"
            )

        self.windows: list[dict[str, Any]] = []
        for idx, win in enumerate(clock_cfg["windows"], start=1):
            if not isinstance(win, dict):
                raise ConfigError(f"Target {target.transformer_id}: clock.windows[{idx}] must be an object")
            from_str = str(win.get("from", ""))
            to_str = str(win.get("to", ""))
            try:
                from_minutes = self._parse_hhmm(from_str)
                to_minutes = self._parse_hhmm(to_str)
            except ValueError:
                raise ConfigError(
                    f"Target {target.transformer_id}: clock.windows[{idx}] invalid time format "
                    f"(expected HH:MM, got from='{from_str}' to='{to_str}')"
                )

            raw_conds = win.get("conditions")
            if not isinstance(raw_conds, list) or not raw_conds:
                raise ConfigError(
                    f"Target {target.transformer_id}: clock.windows[{idx}] requires at least one entry in 'conditions'"
                )

            population: list[str] = []
            for cidx, ce in enumerate(raw_conds, start=1):
                if not isinstance(ce, dict):
                    raise ConfigError(
                        f"Target {target.transformer_id}: clock.windows[{idx}].conditions[{cidx}] must be an object"
                    )
                cond = str(ce.get("condition", "normal"))
                if cond not in CONDITION_CHOICES:
                    raise ConfigError(
                        f"Target {target.transformer_id}: clock.windows[{idx}].conditions[{cidx}] "
                        f"invalid condition '{cond}'"
                    )
                weight = max(1, int(ce.get("weight", 1)))
                population.extend([cond] * weight)

            self.windows.append({
                "from_minutes": from_minutes,
                "to_minutes": to_minutes,
                "population": population,
            })

        self.energy_kwh = float(clock_cfg.get("energy_kwh_start", 0.0))

    @staticmethod
    def _parse_hhmm(value: str) -> int:
        """Return total minutes since midnight for an HH:MM string."""
        parts = value.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Expected HH:MM, got '{value}'")
        h, m = int(parts[0]), int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError(f"Out of range: {value}")
        return h * 60 + m

    def _current_condition(self) -> str:
        now = datetime.now(timezone.utc) if self.use_utc else datetime.now()
        now_minutes = now.hour * 60 + now.minute

        for win in self.windows:
            f, t = win["from_minutes"], win["to_minutes"]
            if f < t:
                # Normal window e.g. 07:00 → 15:00
                in_window = f <= now_minutes < t
            else:
                # Overnight window e.g. 22:00 → 06:00
                in_window = now_minutes >= f or now_minutes < t
            if in_window:
                return self.rng.choice(win["population"])

        return self.default_condition

    def next_reading(self, dt_seconds: float) -> dict[str, Any]:
        cond = self._current_condition()
        defaults = _defaults_for_condition(cond, self.target.rated_kva)

        noise_scale = 1.0
        voltage = max(80.0, defaults["voltage"] + self.rng.uniform(-1.6, 1.6) * noise_scale)
        current = max(0.0, defaults["current"] + self.rng.uniform(-1.1, 1.1) * noise_scale)
        apparent_power = max(0.0, voltage * current)
        power_factor = _clamp(defaults["power_factor"] + self.rng.uniform(-0.015, 0.015), 0.5, 1.0)
        real_power = max(0.0, apparent_power * power_factor)
        frequency = max(40.0, defaults["frequency"] + self.rng.uniform(-0.05, 0.05))
        oil_temp = max(10.0, defaults["oil_temp"] + self.rng.uniform(-0.35, 0.35))

        self.energy_kwh += (real_power / 1000.0) * (max(0.01, dt_seconds) / 3600.0)

        return {
            "voltage": round(voltage, 3),
            "current": round(current, 3),
            "apparent_power": round(apparent_power, 3),
            "real_power": round(real_power, 3),
            "power_factor": round(power_factor, 4),
            "frequency": round(frequency, 3),
            "oil_temp": round(oil_temp, 3),
            "energy_kwh": round(self.energy_kwh, 6),
            "condition": cond,
        }


@dataclass
class TargetRunner:
    spec: TargetSpec
    engine: Any
    next_due: float
    last_sent_monotonic: float
    ws_client: Any = None


def _safe_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _is_local_backend(url: str) -> bool:
    parsed = parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def _ws_url_for_transformer(backend_url: str, transformer_id: int, has_token: bool) -> str:
    parsed = parse.urlparse(backend_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    path = (parsed.path or "").rstrip("/")
    if path.endswith("/api"):
        path = path[:-4]
    base = parse.urlunparse((scheme, parsed.netloc, path, "", "", "")).rstrip("/")
    token_suffix = "?token=<access_token>" if has_token else ""
    return f"{base}/ws/monitor/{transformer_id}/{token_suffix}"


def _device_ws_url(backend_url: str, transformer_id: int, device_api_key: str) -> str:
    parsed = parse.urlparse(backend_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    path = (parsed.path or "").rstrip("/")
    if path.endswith("/api"):
        path = path[:-4]
    base = parse.urlunparse((scheme, parsed.netloc, path, "", "", "")).rstrip("/")
    return f"{base}/ws/device/{transformer_id}/?key={parse.quote(device_api_key, safe='')}"


def _condition_from_loading(apparent_power_va: float, rated_kva: float) -> str:
    rated_va = max(1000.0, rated_kva * 1000.0)
    pct = apparent_power_va / rated_va
    if pct >= 1.2:
        return "critical"
    if pct >= 1.1:
        return "severe_overload"
    if pct >= 1.0:
        return "overload"
    if pct >= 0.92:
        return "heavy_load"
    if pct >= 0.85:
        return "heavy_peak_load"
    return "normal"


def _defaults_for_condition(condition: str, rated_kva: float) -> dict[str, float]:
    kv = max(1.0, rated_kva)
    rated_current = (kv * 1000.0) / 230.0
    presets = {
        "normal": {"current_factor": 0.62, "power_factor": 0.96, "oil_temp": 42.0},
        "heavy_peak_load": {"current_factor": 0.84, "power_factor": 0.94, "oil_temp": 49.0},
        "heavy_load": {"current_factor": 0.92, "power_factor": 0.93, "oil_temp": 52.0},
        "danger_zone": {"current_factor": 0.98, "power_factor": 0.90, "oil_temp": 56.0},
        "overload": {"current_factor": 1.05, "power_factor": 0.88, "oil_temp": 60.0},
        "severe_overload": {"current_factor": 1.18, "power_factor": 0.86, "oil_temp": 66.0},
        "abnormal": {"current_factor": 0.77, "power_factor": 0.80, "oil_temp": 58.0},
        "poor_power_quality": {"current_factor": 0.70, "power_factor": 0.72, "oil_temp": 50.0},
        "critical": {"current_factor": 1.25, "power_factor": 0.84, "oil_temp": 72.0},
    }
    p = presets.get(condition, presets["normal"])
    return {
        "voltage": 230.0,
        "current": rated_current * p["current_factor"],
        "power_factor": p["power_factor"],
        "frequency": 60.0,
        "oil_temp": p["oil_temp"],
    }


def _must_float(row: dict[str, Any], key: str) -> float:
    if row.get(key) is None:
        raise ConfigError(f"CSV row missing required field '{key}'")
    try:
        return float(row[key])
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"CSV field '{key}' is not numeric: {row[key]}") from exc


def _load_csv_rows(path: Path, transformer_id: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        if not reader.fieldnames:
            raise ConfigError(f"Target {transformer_id}: csv has no header")
        for idx, row in enumerate(reader, start=2):
            clean = {str(k).strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
            if not any(clean.values()):
                continue
            rows.append({k: (None if v == "" else v) for k, v in clean.items()})
            if "condition" in rows[-1] and rows[-1]["condition"] not in (None, *CONDITION_CHOICES):
                raise ConfigError(
                    f"Target {transformer_id}: invalid condition at CSV line {idx}: {rows[-1]['condition']}"
                )

    if not rows:
        raise ConfigError(f"Target {transformer_id}: csv has no data rows")
    return rows


def load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Config JSON parse error: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError("Config must be a JSON object")

    data["_config_path"] = str(config_path.resolve())
    return data


def build_targets(config: dict[str, Any], discovered: list[dict[str, Any]]) -> list[TargetSpec]:
    defaults = config.get("defaults", {}) if isinstance(config.get("defaults"), dict) else {}
    default_interval = float(defaults.get("interval_seconds", 5.0))
    default_mode = str(defaults.get("mode", "baseline_noise"))
    default_rated_kva = float(defaults.get("rated_kva", 15.0))

    targets_by_id: dict[int, dict[str, Any]] = {}

    # Manual targets from config.
    for item in config.get("targets", []):
        if not isinstance(item, dict):
            raise ConfigError("Each targets[] entry must be an object")
        if "transformer_id" not in item:
            raise ConfigError("Each targets[] entry requires transformer_id")
        tid = int(item["transformer_id"])
        merged = dict(item)
        merged["_source"] = "manual"
        targets_by_id[tid] = merged

    # Optional discovery targets.
    discovery = config.get("discovery", {}) if isinstance(config.get("discovery"), dict) else {}
    if bool(discovery.get("enabled", False)):
        ids_filter = discovery.get("transformer_ids")
        include_inactive = bool(discovery.get("include_inactive", False))
        allowed: set[int] | None = None
        if isinstance(ids_filter, list) and ids_filter:
            allowed = {int(x) for x in ids_filter}

        for tr in discovered:
            tid = int(tr.get("id"))
            if allowed is not None and tid not in allowed:
                continue
            if not include_inactive and tr.get("is_active") is False:
                continue
            existing = targets_by_id.get(tid, {})
            merged = dict(existing)
            merged.setdefault("transformer_id", tid)
            merged.setdefault("rated_kva", tr.get("rated_kva"))
            merged.setdefault("enabled", True)
            merged.setdefault("mode", default_mode)
            merged.setdefault("interval_seconds", default_interval)
            merged.setdefault("_source", "discovery")
            targets_by_id[tid] = merged

    specs: list[TargetSpec] = []
    for tid in sorted(targets_by_id):
        item = targets_by_id[tid]
        enabled = bool(item.get("enabled", True))
        interval_seconds = float(item.get("interval_seconds", default_interval))
        mode = str(item.get("mode", default_mode))
        if mode not in {"baseline_noise", "scenario_profiles", "csv_replay", "clock_schedule"}:
            raise ConfigError(f"Target {tid}: unsupported mode '{mode}'")
        if interval_seconds <= 0:
            raise ConfigError(f"Target {tid}: interval_seconds must be > 0")

        rated_kva = item.get("rated_kva", default_rated_kva)
        if rated_kva is None:
            rated_kva = default_rated_kva
        rated_kva = float(rated_kva)
        if rated_kva <= 0:
            raise ConfigError(f"Target {tid}: rated_kva must be > 0")

        item["_config_path"] = config["_config_path"]

        seed = item.get("seed")
        transport = str(item.get("transport", "http"))
        if transport not in {"http", "websocket"}:
            raise ConfigError(f"Target {tid}: unsupported transport '{transport}', must be 'http' or 'websocket'")
        if transport == "websocket" and not str(item.get("device_api_key", "")).strip():
            raise ConfigError(f"Target {tid}: transport=websocket requires device_api_key in target config")
        specs.append(
            TargetSpec(
                transformer_id=tid,
                interval_seconds=interval_seconds,
                mode=mode,
                seed=int(seed) if seed is not None else None,
                rated_kva=rated_kva,
                enabled=enabled,
                config=item,
                transport=transport,
            )
        )

    enabled_specs = [s for s in specs if s.enabled]
    if not enabled_specs:
        raise ConfigError("No enabled targets were resolved from config/discovery")
    return enabled_specs


def create_engine(spec: TargetSpec) -> Any:
    if spec.mode == "baseline_noise":
        return BaselineNoiseEngine(spec)
    if spec.mode == "scenario_profiles":
        return ScenarioProfilesEngine(spec)
    if spec.mode == "csv_replay":
        return CsvReplayEngine(spec)
    if spec.mode == "clock_schedule":
        return ClockScheduleEngine(spec)
    raise ConfigError(f"Unsupported mode: {spec.mode}")


def resolve_access_token(config: dict[str, Any], client: ApiClient) -> str | None:
    auth = config.get("auth", {}) if isinstance(config.get("auth"), dict) else {}
    mode = str(auth.get("mode", "hybrid"))
    explicit_token = auth.get("access_token")

    if explicit_token:
        client.access_token = str(explicit_token)
        return client.access_token

    if mode in {"admin_lookup", "hybrid"}:
        username = auth.get("admin_username")
        password = auth.get("admin_password")
        if username and password:
            return client.obtain_access_token(str(username), str(password))

    if mode == "admin_lookup":
        raise ConfigError("auth.mode=admin_lookup requires access_token or admin_username/admin_password")

    return None


def enforce_safety(config: dict[str, Any], target_specs: list[TargetSpec]) -> None:
    safety = config.get("safety", {}) if isinstance(config.get("safety"), dict) else {}
    backend_url = str(config.get("backend_url", "")).strip()
    allow_nonlocal = bool(safety.get("allow_nonlocal", False))
    nonlocal_min_interval = float(safety.get("nonlocal_min_interval_seconds", 5.0))

    if not backend_url:
        raise ConfigError("backend_url is required")

    if not _is_local_backend(backend_url) and not allow_nonlocal:
        raise ConfigError(
            "Refusing non-local backend URL. Set safety.allow_nonlocal=true for staging/production-like runs."
        )

    if not _is_local_backend(backend_url):
        for spec in target_specs:
            if spec.interval_seconds < nonlocal_min_interval:
                raise ConfigError(
                    f"Target {spec.transformer_id}: interval_seconds={spec.interval_seconds} is below "
                    f"safety.nonlocal_min_interval_seconds={nonlocal_min_interval}"
                )


def format_log(level: str, transformer_id: int, message: str) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    return f"{ts} [{level}] transformer={transformer_id} {message}"


def post_reading_with_retry(
    client: ApiClient,
    payload: dict[str, Any],
    retries: int,
    retry_backoff_seconds: float,
) -> tuple[int, Any, str, int, float]:
    attempts = 0
    started = time.monotonic()
    while True:
        attempts += 1
        status, data, raw = client.post_reading(payload)
        if 200 <= status < 300:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            return status, data, raw, attempts, elapsed_ms

        # Do not retry for obvious client-side payload errors.
        if status in {400, 401, 403, 404}:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            return status, data, raw, attempts, elapsed_ms

        if attempts > retries:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            return status, data, raw, attempts, elapsed_ms

        time.sleep(max(0.0, retry_backoff_seconds) * attempts)


def _ws_send_with_reconnect(
    runner: TargetRunner,
    backend_url: str,
    full_payload: dict[str, Any],
    retry_backoff_seconds: float,
) -> tuple[bool, float, str | None]:
    """Send one reading frame via WebSocket, reconnecting once on failure.

    Returns (success, elapsed_ms, error_or_None).
    Updates runner.ws_client in-place for connection reuse across calls.
    """
    device_key = str(runner.spec.config.get("device_api_key", "")).strip()
    ws_url = _device_ws_url(backend_url, runner.spec.transformer_id, device_key)
    started = time.monotonic()

    for attempt in range(2):
        # (Re)connect if we have no live socket.
        if runner.ws_client is None or not runner.ws_client.connected:
            if runner.ws_client is not None:
                runner.ws_client.close()
                runner.ws_client = None
            new_ws = _MinimalWsClient(ws_url)
            try:
                new_ws.connect()
                runner.ws_client = new_ws
            except (WsConnectError, OSError) as exc:
                runner.ws_client = None
                if attempt == 0:
                    time.sleep(max(0.0, retry_backoff_seconds))
                    continue
                return False, (time.monotonic() - started) * 1000.0, f"connect_error={exc}"

        try:
            runner.ws_client.send_json(full_payload)
            return True, (time.monotonic() - started) * 1000.0, None
        except (WsConnectError, OSError) as exc:
            runner.ws_client.close()
            runner.ws_client = None
            if attempt == 0:
                time.sleep(max(0.0, retry_backoff_seconds))
                continue
            return False, (time.monotonic() - started) * 1000.0, f"send_error={exc}"

    return False, (time.monotonic() - started) * 1000.0, "send_failed_after_reconnect"


def run_once(
    runners: list[TargetRunner],
    client: ApiClient,
    dry_run: bool,
    post_retries: int,
    retry_backoff_seconds: float,
) -> int:
    failures = 0
    for runner in runners:
        dt = runner.spec.interval_seconds
        payload = _apply_overrides(runner.engine.next_reading(dt))
        _record_reading(runner.spec.transformer_id, payload)
        full_payload = {"transformer_id": runner.spec.transformer_id, **payload}

        if dry_run:
            print(format_log("DRY", runner.spec.transformer_id, f"payload={json.dumps(full_payload)}"))
            continue

        if runner.spec.transport == "websocket":
            success, elapsed_ms, err = _ws_send_with_reconnect(
                runner=runner,
                backend_url=client.backend_url,
                full_payload=full_payload,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            if success:
                print(
                    format_log(
                        "OK",
                        runner.spec.transformer_id,
                        f"mode={runner.spec.mode} transport=ws latency_ms={elapsed_ms:.1f} condition={payload['condition']}",
                    )
                )
            else:
                failures += 1
                print(
                    format_log(
                        "ERR",
                        runner.spec.transformer_id,
                        f"mode={runner.spec.mode} transport=ws latency_ms={elapsed_ms:.1f} {err}",
                    ),
                    file=sys.stderr,
                )
            # run_once uses a fresh connection per call; close it.
            if runner.ws_client is not None:
                runner.ws_client.close()
                runner.ws_client = None
        else:
            status, data, raw, attempts, elapsed_ms = post_reading_with_retry(
                client=client,
                payload=full_payload,
                retries=post_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )

            if 200 <= status < 300:
                print(
                    format_log(
                        "OK",
                        runner.spec.transformer_id,
                        f"mode={runner.spec.mode} status={status} latency_ms={elapsed_ms:.1f} attempts={attempts} condition={payload['condition']}",
                    )
                )
            else:
                failures += 1
                details = data if data is not None else raw[:200]
                print(
                    format_log(
                        "ERR",
                        runner.spec.transformer_id,
                        f"mode={runner.spec.mode} status={status} latency_ms={elapsed_ms:.1f} attempts={attempts} response={details}",
                    ),
                    file=sys.stderr,
                )
    return failures


def run_continuous(
    runners: list[TargetRunner],
    client: ApiClient,
    dry_run: bool,
    max_iterations: int | None,
    post_retries: int,
    retry_backoff_seconds: float,
) -> int:
    # Establish persistent WebSocket connections for websocket-transport targets.
    if not dry_run:
        for runner in runners:
            if runner.spec.transport == "websocket":
                device_key = str(runner.spec.config.get("device_api_key", "")).strip()
                ws_url = _device_ws_url(client.backend_url, runner.spec.transformer_id, device_key)
                ws = _MinimalWsClient(ws_url)
                try:
                    ws.connect()
                    runner.ws_client = ws
                    print(format_log("INFO", runner.spec.transformer_id, "transport=ws connected"))
                except (WsConnectError, OSError) as exc:
                    runner.ws_client = None
                    print(
                        format_log("WARN", runner.spec.transformer_id, f"transport=ws initial connect failed: {exc}"),
                        file=sys.stderr,
                    )

    failures = 0
    iterations = 0
    stop = {"value": False}

    def _request_stop(_signum: int, _frame: Any) -> None:
        stop["value"] = True

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    try:
        while not stop["value"]:
            now = time.monotonic()
            due = [r for r in runners if now >= r.next_due]
            if not due:
                time.sleep(0.1)
                continue

            for runner in due:
                dt = max(0.01, now - runner.last_sent_monotonic)
                payload = _apply_overrides(runner.engine.next_reading(dt))
                _record_reading(runner.spec.transformer_id, payload)
                full_payload = {"transformer_id": runner.spec.transformer_id, **payload}

                if dry_run:
                    print(format_log("DRY", runner.spec.transformer_id, f"payload={json.dumps(full_payload)}"))
                elif runner.spec.transport == "websocket":
                    success, elapsed_ms, err = _ws_send_with_reconnect(
                        runner=runner,
                        backend_url=client.backend_url,
                        full_payload=full_payload,
                        retry_backoff_seconds=retry_backoff_seconds,
                    )
                    if success:
                        print(
                            format_log(
                                "OK",
                                runner.spec.transformer_id,
                                f"mode={runner.spec.mode} transport=ws latency_ms={elapsed_ms:.1f} condition={payload['condition']}",
                            )
                        )
                    else:
                        failures += 1
                        print(
                            format_log(
                                "ERR",
                                runner.spec.transformer_id,
                                f"mode={runner.spec.mode} transport=ws latency_ms={elapsed_ms:.1f} {err}",
                            ),
                            file=sys.stderr,
                        )
                else:
                    status, data, raw, attempts, elapsed_ms = post_reading_with_retry(
                        client=client,
                        payload=full_payload,
                        retries=post_retries,
                        retry_backoff_seconds=retry_backoff_seconds,
                    )

                    if 200 <= status < 300:
                        print(
                            format_log(
                                "OK",
                                runner.spec.transformer_id,
                                f"mode={runner.spec.mode} status={status} latency_ms={elapsed_ms:.1f} attempts={attempts} condition={payload['condition']}",
                            )
                        )
                    else:
                        failures += 1
                        details = data if data is not None else raw[:220]
                        print(
                            format_log(
                                "ERR",
                                runner.spec.transformer_id,
                                f"mode={runner.spec.mode} status={status} latency_ms={elapsed_ms:.1f} attempts={attempts} response={details}",
                            ),
                            file=sys.stderr,
                        )

                runner.last_sent_monotonic = now
                runner.next_due = now + runner.spec.interval_seconds
                iterations += 1

                if max_iterations is not None and iterations >= max_iterations:
                    stop["value"] = True
                    break
    finally:
        # Close any open WebSocket connections on exit (SIGINT/SIGTERM/max_iterations).
        for runner in runners:
            if runner.ws_client is not None:
                runner.ws_client.close()
                runner.ws_client = None

    return failures


def run_health_check(
    config: dict[str, Any],
    client: ApiClient,
    specs: list[TargetSpec],
    discovered: list[dict[str, Any]],
) -> int:
    failures = 0

    backend_url = str(config.get("backend_url", "")).strip()
    print(f"Backend: {backend_url}")
    print(f"Safety: allow_nonlocal={bool(config.get('safety', {}).get('allow_nonlocal', False))}")

    code, data, raw = client.health()
    if code == 200:
        details = data if isinstance(data, dict) else {"response": raw[:120]}
        print(f"[OK] API health check status=200 details={details}")
    else:
        failures += 1
        details = data if data is not None else raw[:220]
        print(f"[ERR] API health check status={code} details={details}", file=sys.stderr)

    if client.access_token:
        code, data, raw = client.me()
        if code == 200:
            if isinstance(data, dict):
                username = data.get("username", "<unknown>")
                is_staff = bool(data.get("is_staff", False))
                print(f"[OK] Auth check user={username} is_staff={is_staff}")
            else:
                print("[OK] Auth check passed")
        else:
            failures += 1
            details = data if data is not None else raw[:220]
            print(f"[ERR] Auth check status={code} details={details}", file=sys.stderr)
    else:
        print("[WARN] No access token configured; websocket clients still need JWT token in querystring")

    transformer_rows = discovered
    if not transformer_rows and client.access_token:
        try:
            transformer_rows = client.fetch_transformers()
            print(f"[OK] Transformer visibility count={len(transformer_rows)}")
        except RuntimeError as exc:
            print(f"[WARN] Transformer visibility check skipped: {exc}")

    by_id = {
        int(x.get("id")): x
        for x in transformer_rows
        if isinstance(x, dict) and x.get("id") is not None
    }

    print("Targets:")
    for spec in specs:
        trow = by_id.get(spec.transformer_id)
        active_info = "unknown"
        if trow is not None:
            active_info = "active" if bool(trow.get("is_active", False)) else "inactive"
        ws_url = _ws_url_for_transformer(
            backend_url=backend_url,
            transformer_id=spec.transformer_id,
            has_token=bool(client.access_token),
        )
        if spec.transport == "websocket":
            device_key = str(spec.config.get("device_api_key", "")).strip()
            device_ws = _device_ws_url(backend_url, spec.transformer_id, device_key)
            print(
                f"- transformer_id={spec.transformer_id} mode={spec.mode} transport=ws interval={spec.interval_seconds}s "
                f"status={active_info} device_ws={device_ws} monitor_ws={ws_url}"
            )
        else:
            print(
                f"- transformer_id={spec.transformer_id} mode={spec.mode} transport=http interval={spec.interval_seconds}s "
                f"status={active_info} ws={ws_url}"
            )

    return 1 if failures else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ESP32 reading simulator for PoleTransMonitor")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to simulator JSON config",
    )
    parser.add_argument(
        "--backend-url",
        type=str,
        default=None,
        help="Optional override for backend_url from config",
    )
    parser.add_argument(
        "--target",
        type=int,
        nargs="*",
        default=None,
        help="Optional transformer_id filter",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate payloads but do not POST",
    )
    parser.add_argument(
        "--control-panel",
        metavar="PORT",
        type=int,
        nargs="?",
        const=8888,
        default=None,
        help="Start the browser control panel (optionally specify PORT, default 8888)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate-config", help="Validate config and print resolved targets")
    subparsers.add_parser("health-check", help="Check API/auth/target websocket readiness")
    subparsers.add_parser("once", help="Send one reading per target and exit")

    run_parser = subparsers.add_parser("run", help="Run continuously")
    run_parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Stop after N total sends (useful for tests)",
    )

    return parser.parse_args()


def _apply_env_overrides(config: dict[str, Any]) -> None:
    """Apply SIMULATOR_* environment variable overrides to config in-place.

    Useful when the simulator is deployed as a cloud worker (e.g. Render)
    where secrets cannot be stored in the committed config file.

    Supported variables:
      SIMULATOR_BACKEND_URL      — overrides config.backend_url
      SIMULATOR_DEVICE_API_KEY   — overrides device_api_key on all enabled targets
      SIMULATOR_TRANSFORMER_ID   — enables only the target with this transformer_id
                                    (disables all others)
      SIMULATOR_MODE             — overrides mode on all enabled targets
      SIMULATOR_TRANSPORT        — overrides transport on all enabled targets
      SIMULATOR_INTERVAL_SECONDS — overrides interval_seconds on all enabled targets
      SIMULATOR_ALLOW_NONLOCAL   — set "true" to allow non-local backend URLs
    """
    env = os.environ.get

    backend_url = env("SIMULATOR_BACKEND_URL")
    if backend_url:
        config["backend_url"] = backend_url.strip()

    allow_nonlocal = env("SIMULATOR_ALLOW_NONLOCAL")
    if allow_nonlocal and allow_nonlocal.lower() in {"1", "true", "yes"}:
        safety = config.setdefault("safety", {})
        safety["allow_nonlocal"] = True

    api_key = env("SIMULATOR_DEVICE_API_KEY")
    mode_override = env("SIMULATOR_MODE")
    transport_override = env("SIMULATOR_TRANSPORT")
    interval_override = env("SIMULATOR_INTERVAL_SECONDS")
    tid_override = env("SIMULATOR_TRANSFORMER_ID")

    for target in config.get("targets", []):
        if not isinstance(target, dict):
            continue
        if api_key:
            target["device_api_key"] = api_key.strip()
        if mode_override:
            target["mode"] = mode_override.strip()
        if transport_override:
            target["transport"] = transport_override.strip()
        if interval_override:
            try:
                target["interval_seconds"] = float(interval_override)
            except ValueError:
                pass
        if tid_override:
            try:
                target["enabled"] = int(target.get("transformer_id", -1)) == int(tid_override)
            except ValueError:
                pass

    cp_port = env("SIMULATOR_CONTROL_PANEL_PORT")
    if cp_port:
        try:
            panel = config.setdefault("control_panel", {})
            panel["enabled"] = True
            panel["port"] = int(cp_port)
        except ValueError:
            pass


def main() -> int:
    args = parse_args()
    try:
        config = load_config(args.config)
        _apply_env_overrides(config)
        if args.backend_url:
            config["backend_url"] = args.backend_url

        timeout_seconds = float(config.get("request_timeout_seconds", 10.0))
        client = ApiClient(backend_url=str(config.get("backend_url", "")), timeout_seconds=timeout_seconds)

        token = resolve_access_token(config, client)
        if token:
            print("Using authenticated transformer discovery")

        discovery = config.get("discovery", {}) if isinstance(config.get("discovery"), dict) else {}
        discovered: list[dict[str, Any]] = []
        if bool(discovery.get("enabled", False)):
            discovered = client.fetch_transformers()
            print(f"Discovered {len(discovered)} transformers from API")

        specs = build_targets(config, discovered)
        if args.target:
            wanted = {int(x) for x in args.target}
            specs = [s for s in specs if s.transformer_id in wanted]
            if not specs:
                raise ConfigError("No targets left after --target filter")

        enforce_safety(config, specs)

        panel_port: int | None = getattr(args, "control_panel", None)
        if panel_port is None:
            panel_cfg = config.get("control_panel", {})
            if isinstance(panel_cfg, dict) and panel_cfg.get("enabled"):
                panel_port = int(panel_cfg.get("port", 8888))
        if panel_port is not None:
            _start_control_panel(panel_port)

        runtime_cfg = config.get("runtime", {}) if isinstance(config.get("runtime"), dict) else {}
        post_retries = int(runtime_cfg.get("post_retries", 2))
        retry_backoff_seconds = float(runtime_cfg.get("retry_backoff_seconds", 0.8))
        exit_nonzero_on_send_failures = bool(runtime_cfg.get("exit_nonzero_on_send_failures", False))

        runners = [
            TargetRunner(
                spec=s,
                engine=create_engine(s),
                next_due=time.monotonic(),
                last_sent_monotonic=time.monotonic(),
            )
            for s in specs
        ]

        if args.command == "validate-config":
            print(f"Config valid. Resolved {len(runners)} target(s):")
            for r in runners:
                print(
                    f"- transformer_id={r.spec.transformer_id} mode={r.spec.mode} "
                    f"transport={r.spec.transport} interval_seconds={r.spec.interval_seconds} rated_kva={r.spec.rated_kva}"
                )
            return 0

        if args.command == "health-check":
            return run_health_check(
                config=config,
                client=client,
                specs=specs,
                discovered=discovered,
            )

        if args.command == "once":
            failures = run_once(
                runners,
                client,
                dry_run=args.dry_run,
                post_retries=post_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            if failures and not exit_nonzero_on_send_failures:
                print(
                    f"Completed with send failures={failures} (non-fatal by runtime.exit_nonzero_on_send_failures=false)",
                    file=sys.stderr,
                )
                return 0
            return 1 if failures else 0

        if args.command == "run":
            max_iterations = getattr(args, "max_iterations", None)
            failures = run_continuous(
                runners=runners,
                client=client,
                dry_run=args.dry_run,
                max_iterations=max_iterations,
                post_retries=post_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            if failures and not exit_nonzero_on_send_failures:
                print(
                    f"Stopped with send failures={failures} (non-fatal by runtime.exit_nonzero_on_send_failures=false)",
                    file=sys.stderr,
                )
                return 0
            return 1 if failures else 0

        raise ConfigError(f"Unsupported command: {args.command}")

    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"Runtime error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
