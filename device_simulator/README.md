# ESP32 Python Simulator

This simulator posts synthetic readings to the same backend endpoint used by firmware:

- POST /api/readings/

Goal: keep frontend device status Live/Online by continuously updating transformer last_seen and triggering websocket broadcasts via backend.

## Features

- baseline_noise mode: realistic baseline with bounded random variation
- scenario_profiles mode: timeline transitions across conditions
- csv_replay mode: replay measurements from CSV
- hybrid target sourcing:
- Manual targets from config
- Optional API discovery of transformers (admin JWT)
- Safety controls for staging/production-like environments

## Requirements

- Python 3.10+
- Running backend at http://localhost:8000 (or your configured URL)

No additional Python package is required. The script uses only standard library modules.

## Quick start

1. Copy or edit config:

- tools/device_simulator/config.example.json

2. Validate config:

```powershell
python tools/device_simulator/simulate_device.py --config tools/device_simulator/config.example.json validate-config
```

3. Run readiness diagnostics (API/auth/WS target URLs):

```powershell
python tools/device_simulator/simulate_device.py --config tools/device_simulator/config.example.json health-check
```

4. Send one reading per target:

```powershell
python tools/device_simulator/simulate_device.py --config tools/device_simulator/config.example.json once
```

5. Run continuously:

```powershell
python tools/device_simulator/simulate_device.py --config tools/device_simulator/config.example.json run
```

6. Test without sending requests:

```powershell
python tools/device_simulator/simulate_device.py --config tools/device_simulator/config.example.json --dry-run run --max-iterations 5
```

## Health-check output

The health-check command validates:

- API reachability via GET /api/health/
- Auth token usability via GET /api/me/ (when token is configured)
- Transformer visibility (when token allows listing)
- Per-target expected websocket URL used by frontend clients

Use this first when run exits with code 1 so you can identify whether the failure is API, auth, transformer availability, or payload posting.

## Config notes

- backend_url: backend root (for example http://localhost:8000)
- auth.mode:
- hybrid: optional token/credentials (best default)
- admin_lookup: requires access token or admin credentials
- safety.allow_nonlocal:
- false: blocks non-localhost URLs
- true: allows staging/production-like hosts
- safety.nonlocal_min_interval_seconds:
- minimum interval enforced for non-local targets

- runtime.post_retries:
- number of retries for transient post failures (default: 2)

- runtime.retry_backoff_seconds:
- base backoff for retries (default: 0.8)

- runtime.exit_nonzero_on_send_failures:
- when false (default), simulator keeps exit code 0 even if some sends failed during a run
- set true if you want CI/automation to fail fast on any send error

### Modes

1. baseline_noise

- Use baseline and noise sections
- Condition is auto-derived from loading level

2. scenario_profiles

- Define scenario.steps with condition and duration_seconds
- Optional per-step overrides: voltage, current, power_factor, frequency, oil_temp, noise_scale

3. csv_replay

- Set csv.path to a CSV file
- Required columns: voltage, current
- Optional columns: apparent_power, real_power, power_factor, frequency, oil_temp, energy_kwh, condition

## Making dashboard go Live/Online

- Pick a valid transformer_id that exists and is_active=true.
- Run simulator at interval <= 60 seconds. Recommended 5 to 10 seconds.
- Open dashboard and select that transformer.
- Status should become Live while messages arrive.
- Stop simulator and status should become Offline/Stale after timeout window.

## Troubleshooting

- HTTP 400: payload validation error. Check numeric fields and condition values.
- HTTP 403 with deactivated message: transformer is inactive.
- Websocket not updating despite 201 responses:
- Verify Channels + Redis backend setup.
- Verify frontend VITE_WS_URL points to backend websocket host.

## Example staged safety usage

To run against non-local backend:

- Set safety.allow_nonlocal=true
- Keep interval conservative (at least nonlocal_min_interval_seconds)
- Prefer a dedicated test transformer ID
