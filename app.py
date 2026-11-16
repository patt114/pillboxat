#!/usr/bin/env python3
"""
Smart Pillbox - Flask + SocketIO + Raw WebSocket (/ws)
Quản lý 4 ngăn thuốc cố định: A, B, C, D (Đã fix lỗi SQLite database is locked)
"""
import sqlite3
import json
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from flask_sock import Sock

DB_FILE = "pillbox.db"
app = Flask(__name__, template_folder="templates")
app.config["SECRET_KEY"] = "smartpillbox-secret-key-2026"

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")
sock = Sock(app)

esp_clients = set()
esp_lock = threading.Lock()
db_lock = threading.Lock()  # Khóa đồng bộ truy cập SQLite giữa các luồng


# --- Database ---
def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    with db_lock:
        conn = get_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boxes (
                id TEXT PRIMARY KEY,
                med_name TEXT NOT NULL,
                dosage TEXT DEFAULT '',
                sched_time TEXT NOT NULL,
                note TEXT DEFAULT '',
                taken INTEGER DEFAULT 0,
                is_due INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                box_id TEXT,
                payload TEXT,
                timestamp REAL DEFAULT (strftime('%s','now'))
            )
        """)

        default_boxes = [
            ("A", "Omeprazole", "20mg", "07:30", "Trước ăn", 1, 0),
            ("B", "Paracetamol", "500mg", "11:30", "Sau ăn", 0, 0),
            ("C", "Vitamin C", "1000mg", "17:30", "Sau ăn", 0, 0),
            ("D", "Amlodipine", "5mg", "21:00", "Trước khi ngủ", 0, 0),
        ]
        for b in default_boxes:
            conn.execute(
                "INSERT OR IGNORE INTO boxes (id, med_name, dosage, sched_time, note, taken, is_due) VALUES (?, ?, ?, ?, ?, ?, ?)",
                b,
            )
        conn.commit()
        conn.close()


init_db()


def build_state_payload():
    """Tạo gói tin JSON chuẩn đồng bộ cho cả Web và ESP32-C3"""
    with db_lock:
        conn = get_db()
        rows = conn.execute("SELECT * FROM boxes ORDER BY id ASC").fetchall()
        conn.close()

    now = datetime.now()
    boxes_list = []
    active_box = -1

    for idx, r in enumerate(rows):
        is_due = bool(r["is_due"]) and (r["taken"] == 0)
        if is_due and active_box == -1:
            active_box = idx
        boxes_list.append({
            "id": r["id"],
            "med": r["med_name"],
            "dosage": r["dosage"],
            "time": r["sched_time"],
            "note": r["note"],
            "taken": int(r["taken"]),
            "due": is_due,
        })

    return {
        "date": now.strftime("%d/%m/%Y"),
        "time": now.strftime("%H:%M"),
        "active_box": active_box,
        "esp_online": len(esp_clients) > 0,
        "boxes": boxes_list,
    }


def broadcast_all():
    """Gửi trạng thái mới nhất tới tất cả Web Browser và ESP32-C3"""
    payload = build_state_payload()
    msg_str = json.dumps(payload, ensure_ascii=False)

    socketio.emit("state_update", payload)

    with esp_lock:
        dead = []
        for ws in esp_clients:
            try:
                ws.send(msg_str)
            except Exception:
                dead.append(ws)
        for ws in dead:
            esp_clients.discard(ws)


def log_event(event_type, box_id, detail=""):
    with db_lock:
        conn = get_db()
        conn.execute(
            "INSERT INTO events (event_type, box_id, payload) VALUES (?, ?, ?)",
            (event_type, box_id, detail),
        )
        conn.commit()
        conn.close()

    socketio.emit("new_event", {
        "time": datetime.now().strftime("%H:%M:%S"),
        "msg": f"[{box_id}] {detail}" if box_id else detail,
    })


# --- Luồng kiểm tra giờ uống thuốc tự động ---
def scheduler_loop():
    last_minute = ""
    while True:
        now = datetime.now()
        cur_hm = now.strftime("%H:%M")

        if cur_hm != last_minute:
            last_minute = cur_hm
            events_to_log = []

            with db_lock:
                conn = get_db()
                rows = conn.execute("SELECT id, sched_time, taken, is_due FROM boxes").fetchall()

                if cur_hm == "00:00":
                    conn.execute("UPDATE boxes SET taken = 0, is_due = 0")
                    events_to_log.append(("RESET", "ALL", "Sang ngày mới - Đã đặt lại 4 ngăn về 0"))

                for r in rows:
                    if r["sched_time"] == cur_hm and r["taken"] == 0 and r["is_due"] == 0:
                        conn.execute("UPDATE boxes SET is_due = 1 WHERE id = ?", (r["id"],))
                        events_to_log.append(("DUE", r["id"], f"Tới giờ uống thuốc ngăn {r['id']} ({cur_hm})"))

                conn.commit()
                conn.close()

            for ev in events_to_log:
                log_event(*ev)

            broadcast_all()

        time.sleep(1)


threading.Thread(target=scheduler_loop, daemon=True).start()


# --- Routes HTTP ---
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    return jsonify(build_state_payload())


@app.route("/api/box/<box_id>", methods=["POST"])
def api_update_box(box_id):
    box_id = box_id.upper()
    if box_id not in ["A", "B", "C", "D"]:
        return jsonify({"ok": False, "error": "Ngăn không hợp lệ"}), 400

    data = request.get_json(silent=True) or {}

    with db_lock:
        conn = get_db()
        if "med" in data:
            conn.execute("UPDATE boxes SET med_name = ? WHERE id = ?", (data["med"], box_id))
        if "dosage" in data:
            conn.execute("UPDATE boxes SET dosage = ? WHERE id = ?", (data["dosage"], box_id))
        if "time" in data:
            conn.execute("UPDATE boxes SET sched_time = ?, is_due = 0 WHERE id = ?", (data["time"], box_id))
        if "note" in data:
            conn.execute("UPDATE boxes SET note = ? WHERE id = ?", (data["note"], box_id))
        if "taken" in data:
            taken_val = 1 if int(data["taken"]) == 1 else 0
            is_due_val = 0 if taken_val == 1 else 0
            conn.execute("UPDATE boxes SET taken = ?, is_due = ? WHERE id = ?", (taken_val, is_due_val, box_id))
        conn.commit()
        conn.close()

    # Gọi log_event SAU KHI đã đóng conn để không bị tự khóa database
    if "taken" in data:
        taken_val = 1 if int(data["taken"]) == 1 else 0
        log_event("WEB_TAKEN", box_id, f"Web cập nhật trạng thái: {'Đã uống (1)' if taken_val else 'Chưa uống (0)'}")
    else:
        log_event("CONFIG", box_id, f"Cập nhật lịch ngăn {box_id}: {data.get('med', '')} lúc {data.get('time', '')}")

    broadcast_all()
    return jsonify({"ok": True})


@app.route("/api/trigger", methods=["POST"])
def api_trigger():
    data = request.get_json(silent=True) or {}
    action = data.get("action", "buzz")
    box_id = str(data.get("box", "A")).upper()

    with db_lock:
        conn = get_db()
        if action == "buzz" and box_id in ["A", "B", "C", "D"]:
            conn.execute("UPDATE boxes SET is_due = 1, taken = 0 WHERE id = ?", (box_id,))
        elif action == "reset_all":
            conn.execute("UPDATE boxes SET taken = 0, is_due = 0")
        conn.commit()
        conn.close()

    if action == "buzz" and box_id in ["A", "B", "C", "D"]:
        log_event("BUZZ", box_id, f"Gửi lệnh nhắc nhấp nháy & còi tới ngăn {box_id}")
    elif action == "reset_all":
        log_event("RESET", "ALL", "Đặt lại tất cả 4 ngăn về chưa uống (0)")

    broadcast_all()
    return jsonify({"ok": True})


# --- Raw WebSocket Endpoint cho ESP32-C3 (/ws) ---
@sock.route("/ws")
def esp32_ws(ws):
    print("[ESP32-WS] Mạch Lolin C3 Mini đã kết nối!")
    with esp_lock:
        esp_clients.add(ws)
    log_event("DEVICE", "ESP32", "Mạch ESP32-C3 đã kết nối")
    broadcast_all()

    try:
        while True:
            raw_msg = ws.receive()
            if raw_msg is None:
                break
            print("[ESP32-WS] Nhận từ mạch:", raw_msg)
            try:
                data = json.loads(raw_msg)
                msg_type = data.get("type")

                if msg_type == "pill_taken":
                    box_id = str(data.get("box", "")).upper()
                    if box_id in ["A", "B", "C", "D"]:
                        with db_lock:
                            conn = get_db()
                            conn.execute("UPDATE boxes SET taken = 1, is_due = 0 WHERE id = ?", (box_id,))
                            conn.commit()
                            conn.close()
                        log_event("ESP_BTN", box_id, f"Bấm nút trên mạch: Đã uống ngăn {box_id} (1)")
                        broadcast_all()

                elif msg_type == "manual_confirm":
                    log_event("ESP_BTN", "MANUAL", "Bấm nút xác nhận thủ công trên mạch")

            except Exception as e:
                print("[ESP32-WS] Lỗi xử lý JSON:", e)
    finally:
        with esp_lock:
            esp_clients.discard(ws)
        print("[ESP32-WS] Mạch ESP32-C3 đã ngắt kết nối!")
        log_event("DEVICE", "ESP32", "Mạch ESP32-C3 ngắt kết nối")
        broadcast_all()


# --- SocketIO cho Web Browser ---
@socketio.on("connect")
def on_browser_connect():
    socketio.emit("state_update", build_state_payload())


if __name__ == "__main__":
    print("=== Smart Pillbox Server (4 Ngăn A-B-C-D) ===")
    print("Dashboard Web : http://localhost:5000")
    print("ESP32 Raw WS  : ws://<IP_MAY_TINH>:5000/ws")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)