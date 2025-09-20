from flask import Flask, request, jsonify
import sqlite3
import datetime

app = Flask(__name__)
DB_FILE = "daydream_sydney.db"

# --- Database init with schema ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Audit logs
    c.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            table_name TEXT NOT NULL,
            details TEXT,
            timestamp TEXT NOT NULL
        )
    ''')

    # Users table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Trigger for users.updated_at
    c.execute('''
        CREATE TRIGGER IF NOT EXISTS users_updated_at
        AFTER UPDATE ON users
        BEGIN
            UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
    ''')

    # Stars table
    c.execute('''
        CREATE TABLE IF NOT EXISTS stars (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')

    # NFC Tags table
    c.execute('''
        CREATE TABLE IF NOT EXISTS nfc_tags (
            tag_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')

    # Trigger for nfc_tags.updated_at
    c.execute('''
        CREATE TRIGGER IF NOT EXISTS nfc_tags_updated_at
        AFTER UPDATE ON nfc_tags
        BEGIN
            UPDATE nfc_tags SET updated_at = CURRENT_TIMESTAMP WHERE tag_id = NEW.tag_id;
        END;
    ''')

    # Indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_stars_user_created ON stars(user_id, created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_nfc_tags_user ON nfc_tags(user_id)')

    conn.commit()
    conn.close()

def log_action(action, table, details=""):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO audit_logs (action, table_name, details, timestamp) VALUES (?, ?, ?, ?)",
              (action, table, details, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

init_db()

# --- USERS ---
@app.route("/users", methods=["POST"])
def create_user():
    data = request.json
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                  (data["id"], data["name"], data["email"]))
        conn.commit()
        conn.close()
        log_action("INSERT", "users", f"User {data['id']} created")
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route("/users", methods=["GET"])
def list_users():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, name, email, created_at, updated_at FROM users")
    rows = c.fetchall()
    conn.close()
    return jsonify([{"id": r[0], "name": r[1], "email": r[2], "created_at": r[3], "updated_at": r[4]} for r in rows])

# --- STARS ---
@app.route("/stars", methods=["POST"])
def create_star():
    data = request.json
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO stars (id, user_id) VALUES (?, ?)", (data["id"], data["user_id"]))
        conn.commit()
        conn.close()
        log_action("INSERT", "stars", f"Star {data['id']} for user {data['user_id']}")
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route("/stars/<user_id>", methods=["GET"])
def list_stars(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, user_id, created_at FROM stars WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return jsonify([{"id": r[0], "user_id": r[1], "created_at": r[2]} for r in rows])

# --- NFC TAGS ---
@app.route("/nfc", methods=["POST"])
def create_nfc():
    data = request.json
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO nfc_tags (tag_id, user_id) VALUES (?, ?)", (data["tag_id"], data["user_id"]))
        conn.commit()
        conn.close()
        log_action("INSERT", "nfc_tags", f"Tag {data['tag_id']} for user {data['user_id']}")
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route("/nfc/<user_id>", methods=["GET"])
def list_nfc(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT tag_id, user_id, created_at, updated_at FROM nfc_tags WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return jsonify([{"tag_id": r[0], "user_id": r[1], "created_at": r[2], "updated_at": r[3]} for r in rows])

# --- AUDIT LOGS ---
@app.route("/audit", methods=["GET"])
def audit():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, action, table_name, details, timestamp FROM audit_logs ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    return jsonify([{"id": r[0], "action": r[1], "table": r[2], "details": r[3], "timestamp": r[4]} for r in rows])

if __name__ == "__main__":
    app.run(debug=True, port=5000)
