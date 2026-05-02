#!/usr/bin/env python3
"""
Raspberry Pi 5 — autonomous mode (default): dedicated threads on core 2 (sensors + actuators + logic),
core 3 (WebSocket TX), core 4 (WebSocket RX). Starts directly without the menu.

Optional test menu:  python3 hardware_apex_team.py --menu

Dependencies: pip install gpiozero websockets
  + adafruit-blinka, adafruit-circuitpython-dht, bmp280, mpu6050
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import os
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from gpiozero import Button, OutputDevice, PWMLED, PWMOutputDevice, Servo

try:
    import board
    import busio
except ImportError:
    board = None
    busio = None

try:
    import adafruit_dht
except Exception:
    adafruit_dht = None

try:
    import adafruit_bmp280
except Exception:
    adafruit_bmp280 = None

try:
    import adafruit_mpu6050
except Exception:
    adafruit_mpu6050 = None

try:
    import websockets
except Exception:
    websockets = None
try:
    from websockets.protocol import State as WSConnState
except Exception:
    WSConnState = None  # type: ignore[assignment, misc]

def _format_ws_log_payload(message) -> str:
    """String for serial logs (text or binary WebSocket frame)."""
    if isinstance(message, str):
        return message
    if isinstance(message, (bytes, bytearray, memoryview)):
        b = bytes(message)
        if len(b) <= 400:
            return repr(b)
        return repr(b[:400]) + f"... ({len(b)} bytes total)"
    return repr(message)


def _ws_incoming_to_str(message) -> str:
    """Use command text the same as WebSocket text frames; decode UTF-8 if bytes."""
    if isinstance(message, str):
        return message
    if isinstance(message, (bytes, bytearray, memoryview)):
        return bytes(message).decode("utf-8", errors="replace")
    return str(message)


# AUTONOMY:THROTTLE_BOOST:1.10 or THROTTLE_LIMIT:0.95 etc.
_THROTTLE_CMD_RE = re.compile(
    r"^(?:AUTONOMY:)?(THROTTLE_(?:BOOST|LIMIT|SCALE)):(\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)


def _resolve_core_id(preferred_core: int) -> tuple[int, str | None]:
    """
    Accept either 0-based or 1-based core indices.
    Example on a 4-core CPU:
      - valid 0-based: 0..3
      - valid 1-based: 1..4 (auto-converted to 0..3)
    """
    cpu_count = os.cpu_count() or 1
    max_zero_based = cpu_count - 1
    warning = None

    if 0 <= preferred_core <= max_zero_based:
        return preferred_core, None

    # If user passed human-friendly 1-based index, convert it.
    if 1 <= preferred_core <= cpu_count:
        converted = preferred_core - 1
        warning = f"Core {preferred_core} interpreted as 1-based, mapped to CPU {converted}."
        return converted, warning

    # Fallback to last available CPU.
    warning = f"Requested core {preferred_core} is out of range. Using CPU {max_zero_based}."
    return max_zero_based, warning


def _pin_current_process_to_core(preferred_core: int, role_name: str) -> None:
    core_id, warning = _resolve_core_id(preferred_core)
    if warning:
        print(f"[AFFINITY] {warning}")
    try:
        os.sched_setaffinity(0, {core_id})
        print(f"[AFFINITY] {role_name} pinned to CPU {core_id}.")
    except Exception as exc:
        print(f"[AFFINITY] Could not pin {role_name} to CPU {core_id}: {exc}")


def _sensor_process_loop(shared_state, command_state, stop_event, params: dict) -> None:
    _pin_current_process_to_core(int(params.get("core_sensor", 2)), "sensor-process")

    # Local imports inside child process.
    from gpiozero import Button, OutputDevice, PWMLED, PWMOutputDevice, Servo

    button1 = button2 = ldr = mq135 = fan = buzzer = led_r = led_g = led_b = servo = None
    dht22 = None
    i2c = bmp280 = mpu6050 = None
    i2c_last_try = 0.0
    i2c_retry_sec = 3.0

    def _open(name: str, factory):
        try:
            return factory()
        except Exception as exc:
            print(f"[SENSOR] GPIO init {name} failed: {exc}")
            return None

    button1 = _open("button1", lambda: Button(17, pull_up=True))
    button2 = _open("button2", lambda: Button(27, pull_up=True))
    ldr = _open("ldr", lambda: Button(23, pull_up=True))
    mq135 = _open("mq135", lambda: Button(22, pull_up=True))
    fan = _open("fan", lambda: OutputDevice(24, active_high=True, initial_value=False))
    buzzer = _open("buzzer", lambda: PWMOutputDevice(25, active_high=True, initial_value=0.0))
    # RPi5: GPIO 12 and 18 share hardware PWM. PWMLED(12) in the main loop re-takes
    # the peripheral every time set_led runs and silently breaks Servo(18) — use
    # digital GPIO for blue so pin 12 never uses the PWM block at runtime.
    _servo_bcm = int(params.get("servo_bcm", 18))
    servo = _open("servo", lambda b=_servo_bcm: Servo(b))
    if servo is not None:
        servo.value = None
        print(
            f"[SENSOR] Servo ready on GPIO{_servo_bcm} (blue LED on 12 = digital, no PWM).",
            flush=True,
        )
    else:
        print(
            f"[SENSOR] Servo failed to init (GPIO {_servo_bcm}); AERO/* WebSocket commands will not move the motor.",
            flush=True,
        )
    led_r = _open("led_r", lambda: PWMLED(5))
    led_g = _open("led_g", lambda: PWMLED(6))
    led_b = _open(
        "led_b",
        lambda: OutputDevice(12, active_high=True, initial_value=False),
    )

    if board is not None and adafruit_dht is not None:
        try:
            dht22 = adafruit_dht.DHT22(board.D4, use_pulseio=False)
            print("[SENSOR] DHT22 initialized in sensor-process.")
        except Exception as exc:
            print(f"[SENSOR] DHT22 init warning: {exc}")

    ldr_low_means_light = bool(params.get("ldr_low_means_light", True))
    mq_low_means_gas = bool(params.get("mq135_low_means_gas", True))
    sample_interval = float(params.get("ws_interval_sec", 0.5))
    debug_ws_state = bool(params.get("debug_ws_state", False))
    # 1.0 = DC (silent on most passive piezos); 0.3–0.6 often works for passive PWM; active buzzers are loud.
    buzzer_safety_hold_duty = max(0.0, min(1.0, float(params.get("buzzer_safety_hold_duty", 0.5))))

    led_mode = "connection"  # connection -> white (no WS) / red (WS connected)
    # cmd_seq (Manager dict) bumps every RX message so identical strings re-apply (servo, etc.)
    last_applied_cmd_seq: int = -1
    ramp_lock = threading.Lock()
    # 0 = button released; 10..100 in steps of 10 while button1 held (reaches 100 after 5 s)
    throttle_ramp = 0
    throttle_factor: float | None = None
    throttle_mode: str | None = None  # BOOST | LIMIT | SCALE (from last WS command)
    RAMP_MAX_SEC = 5.0
    # nine +10 steps (10→20→…→90) in [0,5s); 100 for t>=5s while held
    RAMP_STEP_SEC = RAMP_MAX_SEC / 9.0
    # Track WS client presence to detect 0->1: resume connection LED (white/red) after
    # ENGINE/SHUTDOWN commands left led_mode in a forced state without a new text message.
    prev_any_ws = False

    def set_led_color(r: float, g: float, b: float) -> None:
        if led_r is not None:
            led_r.value = r
        if led_g is not None:
            led_g.value = g
        if led_b is not None:
            led_b.value = 1.0 if b > 0.0 else 0.0

    def servo_set_degrees(deg: int) -> None:
        if servo is None:
            print("[SENSOR] Servo not initialized (GPIO); AERO/servo command skipped", flush=True)
            return

        def _move_task():
            clamped = max(0, min(180, int(deg)))
            if clamped == 0:
                servo.min()
            elif clamped == 180:
                servo.max()
            elif clamped == 90:
                servo.mid()
            else:
                servo.value = (clamped / 90.0) - 1.0
            
            # Allow 1.5s for the servo to reach the position physically.
            time.sleep(1.5)
            # Drop PWM like the hardware test menu to avoid stalling.
            servo.value = None

        # Run motion in a separate thread so DHT22/BMP280 reads are not delayed.
        threading.Thread(target=_move_task, daemon=True).start()

    # Initial LED status: white while no websocket device connected.
    set_led_color(1.0, 1.0, 1.0)

    def _apply_text_command(msg: str) -> None:
        nonlocal led_mode, throttle_mode, throttle_factor
        clean = msg.strip(" \t\n\r\"'")
        n = clean.upper()
        # Human / informal aliases (e.g. chat text)
        if "JUM" in n and "SERV" in n:
            n = "AUTONOMY:REDUCE_AERO_LOAD"
        tmatch = _THROTTLE_CMD_RE.match(clean)
        if tmatch:
            # THROTTLE_BOOST, THROTTLE_LIMIT, or THROTTLE_SCALE
            raw_kind = tmatch.group(1).upper()
            if raw_kind == "THROTTLE_BOOST":
                throttle_mode = "BOOST"
            elif raw_kind == "THROTTLE_LIMIT":
                throttle_mode = "LIMIT"
            else:
                throttle_mode = "SCALE"
            try:
                throttle_factor = float(tmatch.group(2))
            except ValueError:
                throttle_factor = None
            print(
                f"[SENSOR] Command -> THROTTLE {throttle_mode} factor={throttle_factor}",
                flush=True,
            )
            return
        if n in (
            "AUTONOMY:AERO_CLOSE",
            "AERO_CLOSE",
        ):
            before = servo is not None
            servo_set_degrees(0)
            if before:
                print("[SENSOR] Command -> servo 0 deg (AERO_CLOSE)", flush=True)
        elif n in (
            "AUTONOMY:AERO_OPEN",
            "AERO_OPEN",
        ):
            before = servo is not None
            servo_set_degrees(180)
            if before:
                print("[SENSOR] Command -> servo 180 deg (AERO_OPEN)", flush=True)
        elif n in (
            "AUTONOMY:REDUCE_AERO_LOAD",
            "REDUCE_AERO_LOAD",
            "AERO_MID",
            "AUTONOMY:AERO_MID",
            "AERO_HALF",
            "AUTONOMY:AERO_HALF",
            "AERO_90",
            "SERVO_MID",
            "SERVOMOTOR LA JUMA",
            "SERVOMOTOR LA JUMATATE",
            "SERVOMOTOR LA JUMĂTATE",
        ):
            before = servo is not None
            servo_set_degrees(90)
            if before:
                print(
                    "[SENSOR] Command -> servo 90 deg (REDUCE / mid / half)",
                    flush=True,
                )
        elif n in ("AUTONOMY:FAN_OFF", "FAN_OFF"):
            if fan is not None:
                fan.off()
            print("[SENSOR] Command -> fan OFF")
        elif n in ("AUTONOMY:FAN_ON", "FAN_ON"):
            if fan is not None:
                fan.on()
            print("[SENSOR] Command -> fan ON")
        elif n in ("AUTONOMY:ENGINE_CLEAN_BURN", "ENGINE_CLEAN_BURN"):
            led_mode = "forced_blue"
            set_led_color(0.0, 0.0, 1.0)
            print("[SENSOR] Command -> LED BLUE (ENGINE_CLEAN_BURN)")
        elif n in ("AUTONOMY:ENGINE_SAFETY_HOLD", "ENGINE_SAFETY_HOLD"):
            led_mode = "forced_green"
            if buzzer is not None:
                buzzer.value = buzzer_safety_hold_duty
            set_led_color(0.0, 1.0, 0.0)
            print(
                f"[SENSOR] Command -> buzzer ON (duty={buzzer_safety_hold_duty}) + LED GREEN",
                flush=True,
            )
        elif n in (
            "AUTONOMY:ENGINE_PERFORMANCE_HOLD",
            "ENGINE_PERFORMANCE_HOLD",
        ):
            led_mode = "forced_red"
            set_led_color(1.0, 0.0, 0.0)
            if buzzer is not None:
                buzzer.value = 0.0
            print(
                "[SENSOR] Command -> ENGINE_PERFORMANCE_HOLD (red LED, buzzer OFF)",
                flush=True,
            )
        elif n in ("AUTONOMY:SHUTDOWN_NONCRITICAL", "SHUTDOWN_NONCRITICAL"):
            led_mode = "forced_white"
            set_led_color(1.0, 1.0, 1.0)
            print("[SENSOR] Command -> SHUTDOWN_NONCRITICAL (white LED)")
        elif n in (
            "AUTONOMY:LED_CONNECTION",
            "LED_CONNECTION",
            "LED_CONNECTION_MODE",
        ):
            led_mode = "connection"
            print("[SENSOR] Command -> LED connection mode (white = no WS, red = WS)")
        elif n in ("AUTONOMY:SAFE_RESET", "SAFE_RESET"):
            # Red LED, fan/buzzer off, servo 0°; throttle back to defaults (no BOOST/LIMIT).
            led_mode = "forced_red"
            set_led_color(1.0, 0.0, 0.0)
            if fan is not None:
                fan.off()
            if buzzer is not None:
                buzzer.value = 0.0
            before = servo is not None
            servo_set_degrees(0)
            throttle_mode = None
            throttle_factor = None
            if before:
                print(
                    "[SENSOR] Command -> SAFE_RESET (red LED, fan OFF, buzzer OFF, servo 0°)",
                    flush=True,
                )
            else:
                print(
                    "[SENSOR] Command -> SAFE_RESET (red LED, fan OFF, buzzer OFF; no servo)",
                    flush=True,
                )
        elif n in ("AUTONOMY:BUZZER_OFF", "BUZZER_OFF"):
            if buzzer is not None:
                buzzer.value = 0.0
            print("[SENSOR] Command -> buzzer OFF")

    def _button1_ramp_thread() -> None:
        nonlocal throttle_ramp
        t_start: float | None = None
        while not stop_event.is_set():
            time.sleep(0.01)
            if not button1:
                continue
            if not button1.is_pressed:
                t_start = None
                with ramp_lock:
                    throttle_ramp = 0
                continue
            now = time.monotonic()
            if t_start is None:
                t_start = now
            elapsed = now - t_start
            with ramp_lock:
                if elapsed >= RAMP_MAX_SEC:
                    throttle_ramp = 100
                else:
                    step_idx = int(elapsed // RAMP_STEP_SEC)
                    if step_idx > 8:
                        step_idx = 8
                    throttle_ramp = 10 + step_idx * 10

    ramp_thread = threading.Thread(target=_button1_ramp_thread, name="button1_ramp", daemon=True)
    ramp_thread.start()

    while not stop_event.is_set():
        # Read RX (8766) and TX (8765) separately: ESP32 / browsers often use TX-only for telemetry;
        # ws_connected alone would stay False and leave the LED white.
        any_ws_rx = bool(command_state.get("ws_connected", False))
        any_ws_tx = bool(command_state.get("ws_tx_connected", False))
        any_ws = any_ws_rx or any_ws_tx
        if debug_ws_state:
            print(
                f"[SENSOR] DEBUG: ws_connected(RX)={any_ws_rx}, ws_tx_connected(TX)={any_ws_tx}",
                flush=True,
            )
        seq = int(command_state.get("cmd_seq", 0) or 0)
        if seq != last_applied_cmd_seq:
            last_applied_cmd_seq = seq
            raw_cmd = command_state.get("last_message")
            msg = _ws_incoming_to_str(raw_cmd).strip() if raw_cmd is not None else ""
            if msg:
                _apply_text_command(msg)

        # New WS client: restore connection-style LED (red when connected) even if
        # last_message still holds an old ENGINE/SHUTDOWN and is not re-applied.
        if any_ws and not prev_any_ws:
            led_mode = "connection"
            set_led_color(1.0, 0.0, 0.0)
        prev_any_ws = any_ws

        # Connection-based LED state when no forced LED mode is active.
        if led_mode == "connection":
            if any_ws:
                set_led_color(1.0, 0.0, 0.0)
            else:
                set_led_color(1.0, 1.0, 1.0)
        elif led_mode == "forced_white":
            set_led_color(1.0, 1.0, 1.0)
        elif led_mode == "forced_blue":
            set_led_color(0.0, 0.0, 1.0)
        elif led_mode == "forced_green":
            set_led_color(0.0, 1.0, 0.0)
        elif led_mode == "forced_red":
            set_led_color(1.0, 0.0, 0.0)

        with ramp_lock:
            count_for_throttle = throttle_ramp
        tmode = throttle_mode
        # No THROTTLE_* command yet -> multiply by 1.0 (per spec).
        effective_factor = float(throttle_factor) if throttle_factor is not None else 1.0
        throttle_value = round(float(count_for_throttle) * effective_factor, 4)

        snapshot = {
            "dht22": {"temperature_c": None, "humidity_pct": None},
            "bmp280": {"pressure_hpa": None, "altitude_m": None},
            "mq135": {"air_quality": "unknown"},
            "ldr": {"state": "OFF"},
            "mpu6050": {"accelerometer_m_s2": None, "gyroscope_rad_s": None},
            "servo": {"on": False},
            "buzzer": {"on": False},
            "fan": {"on": False},
            "buttons": {"button1": "OFF", "button2": "OFF"},
            "throttle": {
                "value": throttle_value,
                "count": count_for_throttle,
                "factor": effective_factor,
                "mode": tmode,
            },
            "THROTTLE": throttle_value,
        }

        # Buttons
        if button1 and button2:
            snapshot["buttons"] = {
                "button1": "ON" if bool(button1.is_pressed) else "OFF",
                "button2": "ON" if bool(button2.is_pressed) else "OFF",
            }

        # LDR + MQ135
        if ldr and mq135:
            ldr_pin_low = bool(ldr.is_pressed)
            mq_pin_low = bool(mq135.is_pressed)
            ldr_is_light = ldr_pin_low if ldr_low_means_light else (not ldr_pin_low)
            mq_is_gas = mq_pin_low if mq_low_means_gas else (not mq_pin_low)
            snapshot["ldr"]["state"] = "ON" if ldr_is_light else "OFF"
            snapshot["mq135"]["air_quality"] = "GAS detected" if mq_is_gas else "Clean air"

        # Output status
        if fan is not None:
            snapshot["fan"]["on"] = bool(fan.value > 0)
        if buzzer is not None:
            snapshot["buzzer"]["on"] = bool(buzzer.value > 0)

        if servo is not None:
            snapshot["servo"]["on"] = bool(servo.value is not None)

        # DHT22
        if dht22 is not None:
            try:
                snapshot["dht22"]["temperature_c"] = dht22.temperature
                snapshot["dht22"]["humidity_pct"] = dht22.humidity
            except Exception:
                pass

        # I2C sensors with controlled retry to reduce log spam.
        now = time.monotonic()
        if (bmp280 is None or mpu6050 is None) and now - i2c_last_try >= i2c_retry_sec:
            i2c_last_try = now
            if board is not None and busio is not None and adafruit_bmp280 is not None and adafruit_mpu6050 is not None:
                if i2c is not None:
                    try:
                        i2c.deinit()
                    except Exception:
                        pass
                    i2c = None
                bmp280 = None
                mpu6050 = None
                try:
                    i2c = busio.I2C(board.SCL, board.SDA)
                    for bmp_addr in (0x76, 0x77):
                        try:
                            bmp280 = adafruit_bmp280.Adafruit_BMP280_I2C(i2c, address=bmp_addr)
                            print(f"[SENSOR] BMP280 found at 0x{bmp_addr:02X}")
                            break
                        except Exception:
                            bmp280 = None
                    if bmp280 is not None:
                        mpu6050 = adafruit_mpu6050.MPU6050(i2c)
                        print("[SENSOR] MPU6050 initialized.")
                    else:
                        try:
                            i2c.deinit()
                        except Exception:
                            pass
                        i2c = None
                except Exception as exc:
                    if i2c is not None:
                        try:
                            i2c.deinit()
                        except Exception:
                            pass
                        i2c = None
                    bmp280 = None
                    mpu6050 = None
                    print(f"[SENSOR] I2C init warning: {exc}", flush=True)

        if bmp280 is not None:
            try:
                snapshot["bmp280"]["pressure_hpa"] = bmp280.pressure
                snapshot["bmp280"]["altitude_m"] = bmp280.altitude
            except Exception:
                pass

        if mpu6050 is not None:
            try:
                accel = mpu6050.acceleration
                gyro = mpu6050.gyro
                snapshot["mpu6050"]["accelerometer_m_s2"] = list(accel)
                snapshot["mpu6050"]["gyroscope_rad_s"] = list(gyro)
            except Exception:
                pass

        shared_state["snapshot"] = snapshot
        time.sleep(sample_interval)

    ramp_thread.join(timeout=1.0)

    if dht22 is not None:
        try:
            dht22.exit()
        except Exception:
            pass

    mpu6050 = None
    bmp280 = None
    if i2c is not None:
        try:
            i2c.deinit()
        except Exception:
            pass
        i2c = None

    for dev in (servo, led_b, led_g, led_r, buzzer, fan, mq135, ldr, button2, button1):
        if dev is not None:
            try:
                dev.close()
            except Exception:
                pass
    print("[SENSOR] GPIO released from sensor-process.", flush=True)


def _ws_tx_process_loop(shared_state, command_state, stop_event, params: dict) -> None:
    _pin_current_process_to_core(int(params.get("core_ws_tx", 3)), "websocket-tx-process")
    if websockets is None:
        print("[WS-TX] websockets module missing. Install with: pip install websockets")
        return

    host = str(params.get("ws_host", "0.0.0.0"))
    port = int(params.get("ws_port", 8765))
    interval = float(params.get("ws_interval_sec", 0.5))
    ws_log_traffic = bool(params.get("ws_log_traffic", True))
    print(f"[WS-TX] Starting TX server on ws://{host}:{port} (telemetry every {interval}s)")
    try:
        command_state["ws_tx_connected"] = False
    except Exception:
        pass

    async def main() -> None:
        clients = set()

        async def handler(websocket):
            clients.add(websocket)
            try:
                # Direct bool assignment to Manager dict proxy (reliable for other processes).
                command_state["ws_tx_connected"] = True
            except Exception:
                pass
            remote = getattr(websocket, "remote_address", None)
            print(f"[WS-TX] Client connected: {remote} | active_clients={len(clients)}")
            try:
                await websocket.wait_closed()
            finally:
                clients.discard(websocket)
                try:
                    command_state["ws_tx_connected"] = len(clients) > 0
                except Exception:
                    pass
                print(f"[WS-TX] Client disconnected: {remote} | active_clients={len(clients)}")

        async with websockets.serve(handler, host, port):
            while not stop_event.is_set():
                try:
                    command_state["ws_tx_connected"] = len(clients) > 0
                except Exception:
                    pass
                snapshot = shared_state.get("snapshot", {"status": "waiting_for_sensor_data"})
                message = json.dumps(snapshot)
                websockets.broadcast(clients, message)
                if ws_log_traffic and clients:
                    print(
                        f"[WS-TX] OUT> {len(clients)} client(s) | {len(message)} bytes: {message}",
                        flush=True,
                    )
                if WSConnState is not None:
                    for c in list(clients):
                        if c.state is not WSConnState.OPEN:
                            clients.discard(c)
                try:
                    command_state["ws_tx_connected"] = len(clients) > 0
                except Exception:
                    pass
                await asyncio.sleep(interval)

    asyncio.run(main())


def _ws_rx_process_loop(command_state, stop_event, params: dict) -> None:
    _pin_current_process_to_core(int(params.get("core_ws_rx", 4)), "websocket-rx-process")
    if websockets is None:
        print("[WS-RX] websockets module missing. Install with: pip install websockets", flush=True)
        return

    host = str(params.get("ws_host", "0.0.0.0"))
    rx_port = int(params.get("ws_rx_port", 8766))
    ws_log_traffic = bool(params.get("ws_log_traffic", True))
    print(f"[WS-RX] Starting RX server on ws://{host}:{rx_port}", flush=True)

    async def main() -> None:
        last_heartbeat = time.monotonic()
        connected_clients = 0
        command_state["ws_connected"] = False

        async def handler(websocket):
            nonlocal connected_clients
            remote = getattr(websocket, "remote_address", None)
            connected_clients += 1
            try:
                command_state["ws_connected"] = True
            except Exception:
                pass
            print(f"[WS-RX] Client connected: {remote}", flush=True)
            try:
                async for message in websocket:
                    text = _ws_incoming_to_str(message)
                    try:
                        prev = int(command_state.get("cmd_seq", 0) or 0)
                    except (TypeError, ValueError):
                        prev = 0
                    # last_message / timestamp before cmd_seq so the sensor process never
                    # sees a bumped seq with stale (or empty) last_message.
                    command_state["last_message"] = text
                    command_state["last_timestamp"] = datetime.now().isoformat(timespec="seconds")
                    command_state["cmd_seq"] = prev + 1
                    line = _format_ws_log_payload(text)
                    if not ws_log_traffic and len(line) > 120:
                        line = line[:120] + "..."
                    print("", flush=True)
                    print(f"[WS-RX] IN< from {remote}: {line}", flush=True)
                    print("", flush=True)
            except (ConnectionResetError, BrokenPipeError) as e:
                print(f"[WS-RX] read reset ({remote}): {e}", flush=True)
            except OSError as e:
                if e.errno == 104:  # ECONNRESET
                    print(f"[WS-RX] read reset ({remote}): {e}", flush=True)
                else:
                    raise
            except Exception as e:
                if "ConnectionClosed" in e.__class__.__name__ or "no close frame" in str(
                    e
                ).lower():
                    print(f"[WS-RX] client closed ({remote}): {e}", flush=True)
                else:
                    raise
            finally:
                connected_clients = max(0, connected_clients - 1)
                command_state["ws_connected"] = connected_clients > 0
                print(f"[WS-RX] Client disconnected: {remote}", flush=True)

        async with websockets.serve(handler, host, rx_port):
            while not stop_event.is_set():
                now = time.monotonic()
                if now - last_heartbeat >= 5.0:
                    print("[WS-RX] Waiting for incoming websocket messages...", flush=True)
                    last_heartbeat = now
                await asyncio.sleep(0.2)

    asyncio.run(main())


class HardwareTestMenu:
    def __init__(self, init_gpio: bool = False) -> None:
        self.base_dir = Path(__file__).resolve().parent
        self.params_path = self.base_dir / "test_params.json"
        self.results_path = self.base_dir / "test_results.json"
        self.params = self.load_params()

        # GPIO devices: only for --menu in parent process. In autonomous mode, GPIO lives
        # exclusively in the sensor child process (core 2) to avoid lgpio "GPIO busy".
        self.button1 = None
        self.button2 = None
        self.ldr = None
        self.mq135 = None
        self.fan = None
        self.buzzer = None
        self.servo = None
        self.led_r = None
        self.led_g = None
        self.led_b = None
        if init_gpio:
            self._init_gpio_devices()

        # Sensor handles initialized lazily
        self.i2c = None
        self.bmp280 = None
        self.mpu6050 = None
        self.dht22 = None

    def _init_gpio_devices(self) -> None:
        # Input buttons (pull-up, active low)
        self.button1 = Button(17, pull_up=True)
        self.button2 = Button(27, pull_up=True)

        # Digital "analog-hack" sensors (pull-up, active low on detection/light)
        self.ldr = Button(23, pull_up=True)
        self.mq135 = Button(22, pull_up=True)

        # Output actuators
        self.fan = OutputDevice(24, active_high=True, initial_value=False)
        self.buzzer = PWMOutputDevice(25, active_high=True, initial_value=0.0)
        # RPi5: GPIO 12+18 share HW PWM; never use PWMLED(12) with Servo(18) — blue is digital.
        sb = int(self.params.get("servo_bcm", 18))
        self.servo = Servo(sb)
        self.servo.value = None

        # RGB LED (common cathode) — R/G PWM, B digital for servo-friendly PWM on 18.
        self.led_r = PWMLED(5)
        self.led_g = PWMLED(6)
        self.led_b = OutputDevice(12, active_high=True, initial_value=False)
        self._stabilize_outputs_on_startup()

    def _stabilize_outputs_on_startup(self) -> None:
        """
        Reduce startup glitches on transistor-driven outputs by forcing OFF state
        twice with a short settle delay.
        """
        try:
            self.fan.off()
            self.buzzer.value = 0.0
            self.led_r.off()
            self.led_g.off()
            self.led_b.off()
            self.servo.value = None
            time.sleep(0.05)
            self.fan.off()
            self.buzzer.value = 0.0
        except Exception:
            # Keep startup robust even if one device is unavailable.
            pass

    def _release_gpio_devices(self) -> None:
        # Ensure outputs are off before releasing pins.
        self.all_outputs_off()
        for attr in ("button1", "button2", "ldr", "mq135", "fan", "buzzer", "servo", "led_r", "led_g", "led_b"):
            dev = getattr(self, attr, None)
            if dev is not None:
                try:
                    dev.close()
                except Exception:
                    pass
            setattr(self, attr, None)

        if self.dht22 is not None:
            try:
                self.dht22.exit()
            except Exception:
                pass
            self.dht22 = None

        self.bmp280 = None
        self.mpu6050 = None
        if self.i2c is not None:
            try:
                self.i2c.deinit()
            except Exception:
                pass
            self.i2c = None

    @staticmethod
    def default_params() -> dict:
        return {
            "sensor_samples": 5,
            "sensor_interval_sec": 1.0,
            "actuator_on_sec": 2.0,
            "led_color_hold_sec": 1.0,
            "servo_return_to_mid": True,
            "ldr_low_means_light": True,
            "mq135_low_means_gas": True,
            "ws_host": "0.0.0.0",
            "ws_port": 8765,
            "ws_rx_port": 8766,
            "ws_interval_sec": 0.5,
            "core_sensor": 2,
            "core_ws_tx": 3,
            # On a 4-core Pi, use 0-3. Value 4 is treated as 1-based "4th core" -> CPU 3 (same as TX if core_ws_tx is 3).
            "core_ws_rx": 0,
            # If true, sensor loop logs RX/TX connection flags (multiprocess visibility check).
            "debug_ws_state": False,
            # ENGINE_SAFETY_HOLD buzzer duty (0..1). Use e.g. 0.4–0.6 for passive piezos; 1.0 is DC (often silent on passive).
            "buzzer_safety_hold_duty": 0.5,
            # If true, serial prints: [WS-TX] OUT> telemetry sent to clients; [WS-RX] IN< commands received.
            "ws_log_traffic": True,
            # BCM pin for Servo; 18 = same as test_final.py. Use 19 if you must avoid PWM ch0 with LED on GPIO 12.
            "servo_bcm": 18,
        }

    def load_params(self) -> dict:
        if not self.params_path.exists():
            params = self.default_params()
            self.save_params(params)
            return params

        try:
            with self.params_path.open("r", encoding="utf-8") as fp:
                loaded = json.load(fp)
            params = self.default_params()
            params.update(loaded)
            return params
        except Exception:
            params = self.default_params()
            self.save_params(params)
            return params

    def save_params(self, params: dict | None = None) -> None:
        payload = self.params if params is None else params
        with self.params_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    def append_result(self, test_name: str, data: dict) -> None:
        entry = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "data": data,
        }

        existing = {}
        if self.results_path.exists():
            try:
                with self.results_path.open("r", encoding="utf-8") as fp:
                    existing = json.load(fp)
                    if not isinstance(existing, dict):
                        existing = {}
            except Exception:
                existing = {}

        existing[test_name] = entry
        existing["_meta"] = {
            "last_update": datetime.now().isoformat(timespec="seconds"),
            "format": "current_state",
        }
        with self.results_path.open("w", encoding="utf-8") as fp:
            json.dump(existing, fp, indent=2)

    def init_i2c_sensors(self) -> bool:
        if self.bmp280 and self.mpu6050:
            return True

        if board is None or busio is None:
            print("I2C support libraries missing: install adafruit-blinka.")
            return False
        if adafruit_bmp280 is None:
            print("BMP280 library missing: pip install adafruit-circuitpython-bmp280")
            return False
        if adafruit_mpu6050 is None:
            print("MPU6050 library missing: pip install adafruit-circuitpython-mpu6050")
            return False

        try:
            self.i2c = busio.I2C(board.SCL, board.SDA)

            # Helpful diagnostics: show all detected I2C addresses.
            addresses = []
            while not self.i2c.try_lock():
                time.sleep(0.01)
            try:
                addresses = [f"0x{addr:02X}" for addr in self.i2c.scan()]
            finally:
                self.i2c.unlock()
            print(f"I2C scan detected: {addresses if addresses else 'none'}")

            # BMP280 boards are usually at 0x76 or 0x77.
            bmp_error = None
            for bmp_addr in (0x76, 0x77):
                try:
                    self.bmp280 = adafruit_bmp280.Adafruit_BMP280_I2C(self.i2c, address=bmp_addr)
                    print(f"BMP280 found at 0x{bmp_addr:02X}")
                    break
                except Exception as exc:
                    bmp_error = exc
                    self.bmp280 = None
            if self.bmp280 is None:
                raise RuntimeError(f"BMP280 not found on 0x76/0x77 ({bmp_error})")

            self.mpu6050 = adafruit_mpu6050.MPU6050(self.i2c)  # default 0x68
            print("MPU6050 initialized (expected address 0x68).")
            print("I2C sensors initialized successfully.")
            return True
        except Exception as exc:
            print(f"I2C init failed: {exc}")
            return False

    def init_dht22(self) -> bool:
        if self.dht22 is not None:
            return True

        if board is None:
            print("Board library missing: install adafruit-blinka.")
            return False
        if adafruit_dht is None:
            print("DHT library missing: pip install adafruit-circuitpython-dht")
            return False

        try:
            self.dht22 = adafruit_dht.DHT22(board.D4, use_pulseio=False)
            print("DHT22 initialized successfully.")
            return True
        except Exception as exc:
            print(f"DHT22 init failed: {exc}")
            return False

    def all_outputs_off(self) -> None:
        if self.fan is not None:
            self.fan.off()
        if self.buzzer is not None:
            self.buzzer.off()
        if self.led_r is not None:
            self.led_r.off()
        if self.led_g is not None:
            self.led_g.off()
        if self.led_b is not None:
            self.led_b.off()
        if self.servo is not None:
            self.servo.value = None  # Detach pulse

    def test_rgb_led(self) -> None:
        print("\n[RGB LED TEST] Cycling colors...")
        hold_sec = float(self.params["led_color_hold_sec"])
        colors = [
            ("Red", 1.0, 0.0, 0.0),
            ("Green", 0.0, 1.0, 0.0),
            ("Blue", 0.0, 0.0, 1.0),
            ("Yellow", 1.0, 1.0, 0.0),
            ("Purple", 1.0, 0.0, 1.0),
            ("Cyan", 0.0, 1.0, 1.0),
            ("White", 1.0, 1.0, 1.0),
        ]
        for name, r, g, b in colors:
            print(f"  -> {name}")
            self.led_r.value = r
            self.led_g.value = g
            self.led_b.value = 1.0 if b > 0.0 else 0.0
            time.sleep(hold_sec)
        self.led_r.off()
        self.led_g.off()
        self.led_b.off()
        self.append_result("rgb_led", {"status": "ok", "colors_count": len(colors), "hold_sec": hold_sec})
        print("[RGB LED TEST] Complete.\n")

    def test_dht22(self) -> None:
        samples = int(self.params["sensor_samples"])
        interval = float(self.params["sensor_interval_sec"])
        print(f"\n[DHT22 TEST] Reading {samples} samples...")
        if not self.init_dht22():
            self.append_result("dht22", {"status": "init_failed"})
            return

        last_reading = None
        for i in range(1, samples + 1):
            try:
                temp_c = self.dht22.temperature
                humidity = self.dht22.humidity
                print(f"  Sample {i}: Temp={temp_c:.1f} C  Humidity={humidity:.1f}%")
                last_reading = {"sample": i, "temp_c": temp_c, "humidity_pct": humidity, "ok": True}
            except Exception as exc:
                print(f"  Sample {i}: read failed ({exc})")
                last_reading = {"sample": i, "ok": False, "error": str(exc)}
            time.sleep(interval)
        self.append_result("dht22", {"status": "ok", "current": last_reading, "samples_count": samples})
        print("[DHT22 TEST] Complete.\n")

    def test_i2c_sensors(self) -> None:
        samples = int(self.params["sensor_samples"])
        interval = float(self.params["sensor_interval_sec"])
        print(f"\n[I2C SENSOR TEST] BMP280 + MPU6050, {samples} samples...")
        if not self.init_i2c_sensors():
            self.append_result("i2c_sensors", {"status": "init_failed"})
            return

        last_reading = None
        for i in range(1, samples + 1):
            try:
                temp_c = self.bmp280.temperature
                pressure = self.bmp280.pressure
                altitude = self.bmp280.altitude
                accel = self.mpu6050.acceleration
                gyro = self.mpu6050.gyro

                print(f"  Sample {i}:")
                print(f"    BMP280 -> Temp={temp_c:.2f} C  Pressure={pressure:.2f} hPa  Alt={altitude:.2f} m")
                print(f"    MPU6050 -> Accel(x,y,z)={accel} m/s^2")
                print(f"               Gyro(x,y,z)={gyro} rad/s")
                last_reading = (
                    {
                        "sample": i,
                        "bmp280": {"temp_c": temp_c, "pressure_hpa": pressure, "altitude_m": altitude},
                        "mpu6050": {"accel_m_s2": list(accel), "gyro_rad_s": list(gyro)},
                        "ok": True,
                    }
                )
            except Exception as exc:
                print(f"  Sample {i}: read failed ({exc})")
                last_reading = {"sample": i, "ok": False, "error": str(exc)}
            time.sleep(interval)
        self.append_result("i2c_sensors", {"status": "ok", "current": last_reading, "samples_count": samples})
        print("[I2C SENSOR TEST] Complete.\n")

    def test_buttons(self) -> None:
        samples = int(self.params["sensor_samples"])
        interval = float(self.params["sensor_interval_sec"])
        print(f"\n[BUTTON TEST] Reading states {samples} times...")
        last_reading = None
        for i in range(1, samples + 1):
            b1_pressed = self.button1.is_pressed  # Active-low button
            b2_pressed = self.button2.is_pressed
            print(f"  Sample {i}: Button1={'PRESSED' if b1_pressed else 'released'}  "
                  f"Button2={'PRESSED' if b2_pressed else 'released'}")
            last_reading = {"sample": i, "button1_pressed": b1_pressed, "button2_pressed": b2_pressed}
            time.sleep(interval)
        self.append_result("buttons", {"status": "ok", "current": last_reading, "samples_count": samples})
        print("[BUTTON TEST] Complete.\n")

    def test_fan(self) -> None:
        on_sec = float(self.params["actuator_on_sec"])
        print(f"\n[FAN TEST] ON for {on_sec} seconds...")
        self.fan.on()
        time.sleep(on_sec)
        self.fan.off()
        self.append_result("fan", {"status": "ok", "on_sec": on_sec})
        print("[FAN TEST] OFF.\n")

    def test_buzzer(self) -> None:
        on_sec = float(self.params["actuator_on_sec"])
        # 0.1 means 10% duty cycle (reduced volume).
        low_volume = 0.1

        print(f"\n[BUZZER TEST] ON at {low_volume * 100}% volume for {on_sec} seconds...")

        self.buzzer.value = low_volume
        time.sleep(on_sec)
        self.buzzer.value = 0  # Off

        self.append_result("buzzer", {"status": "ok", "on_sec": on_sec, "volume": low_volume})
        print("[BUZZER TEST] OFF.\n")

    def test_servo(self) -> None:
        on_sec = float(self.params["actuator_on_sec"])
        return_to_mid = bool(self.params["servo_return_to_mid"])
        print(f"\n[SERVO TEST] Move to max position for {on_sec} seconds...")
        self.servo.max()
        time.sleep(on_sec)
        if return_to_mid:
            self.servo.mid()
        self.servo.value = None  # stop pulses
        self.append_result("servo", {"status": "ok", "on_sec": on_sec, "return_to_mid": return_to_mid})
        print("[SERVO TEST] Returned to center and detached.\n")

    def test_ldr_mq135(self) -> None:
        samples = int(self.params["sensor_samples"])
        interval = float(self.params["sensor_interval_sec"])
        ldr_low_means_light = bool(self.params["ldr_low_means_light"])
        mq_low_means_gas = bool(self.params["mq135_low_means_gas"])
        print(f"\n[LDR + MQ135 TEST] Reading digital states {samples} times...")
        print(f"  LDR logic config: LOW={'light' if ldr_low_means_light else 'dark'}")
        print(f"  MQ135 logic config: LOW={'gas' if mq_low_means_gas else 'clean'}")
        last_reading = None
        for i in range(1, samples + 1):
            # For pull-up inputs with gpiozero Button:
            # is_pressed == True means pin is LOW.
            ldr_pin_low = self.ldr.is_pressed
            mq_pin_low = self.mq135.is_pressed

            ldr_is_light = ldr_pin_low if ldr_low_means_light else (not ldr_pin_low)
            mq_is_gas = mq_pin_low if mq_low_means_gas else (not mq_pin_low)

            ldr_level = "LOW" if ldr_pin_low else "HIGH"
            mq_level = "LOW" if mq_pin_low else "HIGH"
            ldr_state = "LIGHT" if ldr_is_light else "DARK"
            mq_state = "GAS detected" if mq_is_gas else "Clean air"

            print(f"  Sample {i}: LDR={ldr_state} (raw={ldr_level}) | MQ135={mq_state} (raw={mq_level})")
            last_reading = (
                {
                    "sample": i,
                    "ldr_state": ldr_state,
                    "mq135_state": mq_state,
                    "ldr_pin_level": ldr_level,
                    "mq135_pin_level": mq_level,
                    "ldr_low_means_light": ldr_low_means_light,
                    "mq135_low_means_gas": mq_low_means_gas,
                }
            )
            time.sleep(interval)
        self.append_result("ldr_mq135", {"status": "ok", "current": last_reading, "samples_count": samples})
        print("[LDR + MQ135 TEST] Complete.\n")

    def show_params(self) -> None:
        print("\n[PARAMS] Current test parameters:")
        print(json.dumps(self.params, indent=2))
        print(f"Saved in: {self.params_path}\n")

    def update_params_interactive(self) -> None:
        print("\n[PARAMS UPDATE] Press Enter to keep existing value.")
        new_params = dict(self.params)
        prompts = [
            ("sensor_samples", int),
            ("sensor_interval_sec", float),
            ("actuator_on_sec", float),
            ("led_color_hold_sec", float),
            ("servo_return_to_mid", lambda x: x.lower() in ("1", "true", "yes", "y")),
            ("ldr_low_means_light", lambda x: x.lower() in ("1", "true", "yes", "y")),
            ("mq135_low_means_gas", lambda x: x.lower() in ("1", "true", "yes", "y")),
            ("ws_host", str),
            ("ws_port", int),
            ("ws_rx_port", int),
            ("ws_interval_sec", float),
            ("core_sensor", int),
            ("core_ws_tx", int),
            ("core_ws_rx", int),
            ("servo_bcm", int),
        ]
        for key, caster in prompts:
            raw = input(f"{key} [{new_params[key]}]: ").strip()
            if not raw:
                continue
            try:
                new_params[key] = caster(raw)
            except Exception:
                print(f"  Invalid value for {key}, keeping {new_params[key]}")

        self.params = new_params
        self.save_params()
        self.append_result("params_update", {"status": "ok", "params": self.params})
        print(f"Parameters saved to {self.params_path}\n")

    def show_last_results(self) -> None:
        print("\n[RESULTS] Current state snapshot:")
        if not self.results_path.exists():
            print("No results file yet.\n")
            return
        try:
            with self.results_path.open("r", encoding="utf-8") as fp:
                entries = json.load(fp)
            if not isinstance(entries, dict) or not entries:
                print("Results file is empty.\n")
                return
            print(json.dumps(entries, indent=2))
            print(f"Full results file: {self.results_path}\n")
        except Exception as exc:
            print(f"Failed to read results: {exc}\n")

    def collect_live_snapshot(self) -> dict:
        ldr_pin_low = self.ldr.is_pressed
        mq_pin_low = self.mq135.is_pressed
        ldr_low_means_light = bool(self.params["ldr_low_means_light"])
        mq_low_means_gas = bool(self.params["mq135_low_means_gas"])

        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "buttons": {
                "button1_pressed": self.button1.is_pressed,
                "button2_pressed": self.button2.is_pressed,
            },
            "digital_inputs": {
                "ldr": {
                    "raw_level": "LOW" if ldr_pin_low else "HIGH",
                    "state": "LIGHT" if (ldr_pin_low if ldr_low_means_light else (not ldr_pin_low)) else "DARK",
                },
                "mq135": {
                    "raw_level": "LOW" if mq_pin_low else "HIGH",
                    "state": "GAS detected" if (mq_pin_low if mq_low_means_gas else (not mq_pin_low)) else "Clean air",
                },
            },
            "sensors": {},
            "outputs": {
                "fan_on": self.fan.value > 0,
                "buzzer_level": self.buzzer.value,
            },
        }

        # DHT22
        if self.init_dht22():
            try:
                payload["sensors"]["dht22"] = {
                    "temp_c": self.dht22.temperature,
                    "humidity_pct": self.dht22.humidity,
                    "ok": True,
                }
            except Exception as exc:
                payload["sensors"]["dht22"] = {"ok": False, "error": str(exc)}
        else:
            payload["sensors"]["dht22"] = {"ok": False, "error": "init_failed"}

        # BMP280 + MPU6050
        if self.init_i2c_sensors():
            try:
                payload["sensors"]["bmp280"] = {
                    "temp_c": self.bmp280.temperature,
                    "pressure_hpa": self.bmp280.pressure,
                    "altitude_m": self.bmp280.altitude,
                    "ok": True,
                }
            except Exception as exc:
                payload["sensors"]["bmp280"] = {"ok": False, "error": str(exc)}
            try:
                accel = self.mpu6050.acceleration
                gyro = self.mpu6050.gyro
                payload["sensors"]["mpu6050"] = {
                    "accel_m_s2": list(accel),
                    "gyro_rad_s": list(gyro),
                    "ok": True,
                }
            except Exception as exc:
                payload["sensors"]["mpu6050"] = {"ok": False, "error": str(exc)}
        else:
            payload["sensors"]["bmp280"] = {"ok": False, "error": "init_failed"}
            payload["sensors"]["mpu6050"] = {"ok": False, "error": "init_failed"}

        return payload

    async def _websocket_stream_loop(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets module missing. Install with: pip install websockets")

        clients = set()
        host = str(self.params["ws_host"])
        port = int(self.params["ws_port"])
        interval = float(self.params.get("ws_interval_sec", 0.5))

        async def handler(websocket):
            clients.add(websocket)
            remote = getattr(websocket, "remote_address", None)
            ts = datetime.now().isoformat(timespec="seconds")
            print(f"[{ts}] [WEBSOCKET] Client connected: {remote} | active_clients={len(clients)}")
            try:
                await websocket.wait_closed()
            finally:
                clients.discard(websocket)
                ts = datetime.now().isoformat(timespec="seconds")
                print(f"[{ts}] [WEBSOCKET] Client disconnected: {remote} | active_clients={len(clients)}")

        log_traffic = bool(self.params.get("ws_log_traffic", True))
        async with websockets.serve(handler, host, port):
            print(f"\n[WEBSOCKET] Server started on ws://{host}:{port}")
            print("[WEBSOCKET] Press Ctrl+C to stop streaming and return to menu.")
            while True:
                snapshot = self.collect_live_snapshot()
                message = json.dumps(snapshot)
                websockets.broadcast(clients, message)
                if log_traffic and clients:
                    print(
                        f"[WS-TX] OUT> {len(clients)} client(s) | {len(message)} bytes: {message}",
                        flush=True,
                    )
                if WSConnState is not None:
                    for c in list(clients):
                        if c.state is not WSConnState.OPEN:
                            clients.discard(c)
                await asyncio.sleep(interval)

    def start_websocket_stream(self) -> None:
        print("\n[WEBSOCKET] Starting live telemetry server...")
        try:
            asyncio.run(self._websocket_stream_loop())
        except KeyboardInterrupt:
            print("\n[WEBSOCKET] Streaming stopped by user.\n")
        except Exception as exc:
            print(f"\n[WEBSOCKET] Failed to start: {exc}\n")
            self.append_result("websocket_error", {"status": "failed", "error": str(exc)})

    def start_multicore_runtime(self) -> None:
        print("\n[MULTICORE] Starting 3 dedicated processes...")
        print(f"  - Sensor process core preference: {self.params['core_sensor']}")
        print(f"  - WS TX process core preference: {self.params['core_ws_tx']}")
        print(f"  - WS RX process core preference: {self.params['core_ws_rx']}")
        print(f"  - WS TX endpoint: ws://{self.params['ws_host']}:{self.params['ws_port']}")
        print(f"  - WS RX endpoint: ws://{self.params['ws_host']}:{self.params['ws_rx_port']}")
        print(
            f"  - WS TX interval (core {self.params['core_ws_tx']}): "
            f"{float(self.params.get('ws_interval_sec', 0.5))} s",
        )
        print("[MULTICORE] Running autonomously. Press Ctrl+C to stop.")

        # Release GPIO in parent process to avoid "GPIO busy" in sensor child.
        self._release_gpio_devices()
        time.sleep(0.4)

        manager = mp.Manager()
        shared_state = manager.dict()
        command_state = manager.dict()
        stop_event = mp.Event()

        sensor_proc = mp.Process(
            target=_sensor_process_loop,
            args=(shared_state, command_state, stop_event, dict(self.params)),
            daemon=True,
        )
        tx_proc = mp.Process(
            target=_ws_tx_process_loop,
            args=(shared_state, command_state, stop_event, dict(self.params)),
            daemon=True,
        )
        rx_proc = mp.Process(
            target=_ws_rx_process_loop,
            args=(command_state, stop_event, dict(self.params)),
            daemon=True,
        )

        sensor_proc.start()
        time.sleep(0.35)
        tx_proc.start()
        rx_proc.start()

        self.append_result(
            "multicore_runtime",
            {
                "status": "started",
                "pids": {"sensor": sensor_proc.pid, "ws_tx": tx_proc.pid, "ws_rx": rx_proc.pid},
                "cores": {
                    "sensor": self.params["core_sensor"],
                    "ws_tx": self.params["core_ws_tx"],
                    "ws_rx": self.params["core_ws_rx"],
                },
            },
        )

        try:
            while True:
                if not sensor_proc.is_alive() or not tx_proc.is_alive() or not rx_proc.is_alive():
                    print("[MULTICORE] One process exited unexpectedly. Stopping runtime...")
                    break
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n[MULTICORE] Stop requested by user.")
        finally:
            stop_event.set()
            for proc in (sensor_proc, tx_proc, rx_proc):
                proc.join(timeout=8)
                if proc.is_alive():
                    proc.terminate()
                    proc.join(timeout=2)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=1)
            time.sleep(0.6)
            try:
                self._init_gpio_devices()
            except Exception as exc:
                print(f"[MULTICORE] GPIO re-init failed (retry): {exc}")
                time.sleep(0.5)
                self._init_gpio_devices()
            print("[MULTICORE] All child processes stopped.\n")

    def print_menu(self) -> None:
        print("========== RPi 5 Hardware Test Menu ==========")
        print("1. Test RGB LED")
        print("2. Test DHT22 (GPIO4)")
        print("3. Test I2C Sensors (BMP280 + MPU6050)")
        print("4. Test Buttons (GPIO17, GPIO27)")
        print("5. Test Fan (GPIO24)")
        print("6. Test Buzzer (GPIO25)")
        print("7. Test Servo SG90 (see servo_bcm in test_params; default 18 = test_final.py)")
        print("8. Test LDR + MQ135 digital inputs (GPIO23, GPIO22)")
        print("9. Turn all outputs OFF now")
        print("10. View current parameters JSON")
        print("11. Edit parameters JSON (interactive)")
        print("12. View current results JSON")
        print("13. Start WebSocket telemetry server (for ESP32)")
        print("14. Start multi-core runtime (sensor + ws tx + ws rx)")
        print("0. Exit")
        print("==============================================")

    def run(self) -> None:
        while True:
            self.print_menu()
            choice = input("Select option: ").strip()

            if choice == "1":
                self.test_rgb_led()
            elif choice == "2":
                self.test_dht22()
            elif choice == "3":
                self.test_i2c_sensors()
            elif choice == "4":
                self.test_buttons()
            elif choice == "5":
                self.test_fan()
            elif choice == "6":
                self.test_buzzer()
            elif choice == "7":
                self.test_servo()
            elif choice == "8":
                self.test_ldr_mq135()
            elif choice == "9":
                print("\nTurning everything OFF...\n")
                self.all_outputs_off()
                self.append_result("all_outputs_off", {"status": "ok"})
            elif choice == "10":
                self.show_params()
            elif choice == "11":
                self.update_params_interactive()
            elif choice == "12":
                self.show_last_results()
            elif choice == "13":
                self.start_websocket_stream()
            elif choice == "14":
                self.start_multicore_runtime()
            elif choice == "0":
                print("\nExiting test menu.")
                break
            else:
                print("\nInvalid selection. Please try again.\n")


def main() -> int:
    if "--help" in sys.argv or "-h" in sys.argv:
        print(
            "Raspberry Pi 5 hardware (autonomous)\n\n"
            "  (default)  Core 2 = sensors + logic, Core 3 = WebSocket TX, Core 4 = WebSocket RX\n"
            "  no menu; starts the multicore + WebSocket runtime immediately.\n\n"
            "  python3 hardware_apex_team.py        autonomous mode (recommended)\n"
            "  python3 hardware_apex_team.py --menu  interactive test menu\n"
        )
        return 0

    use_menu = "--menu" in sys.argv
    menu = HardwareTestMenu(init_gpio=use_menu)
    try:
        if use_menu:
            menu.run()
        else:
            print(
                "========== Autonomous mode (no menu) ==========\n"
                f"  Core 2 = sensors, white/red LED (connection), throttle, RX commands\n"
                f"  Core 3 = WS TX: ws://{menu.params['ws_host']}:{menu.params['ws_port']}\n"
                f"  Core 4 = WS RX: ws://{menu.params['ws_host']}:{menu.params['ws_rx_port']}\n"
                "  Ctrl+C = stop\n"
                "==============================================\n"
            )
            menu.start_multicore_runtime()
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received, shutting down safely...")
    finally:
        menu.all_outputs_off()
        if menu.dht22 is not None:
            try:
                menu.dht22.exit()
            except Exception:
                pass
        print("Exiting. Goodbye.")
    return 0


if __name__ == "__main__":
    # Force Python to spawn fresh processes instead of forked clones.
    # This fixes the PWM connection for the servo motor on Pi 5.
    import multiprocessing as mp
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass
        
    sys.exit(main())
