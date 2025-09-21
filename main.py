from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import sqlite3
import datetime
import traceback

app = Flask(__name__)
# Enable CORS for all routes to allow access from iOS app
CORS(app)
DB_FILE = "daydream_sydney.db"

# Global error handling
@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Resource not found"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"status": "error", "message": "Server error", "error": str(error)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    print(f"Unhandled exception: {e}")
    print(traceback.format_exc())
    return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500

# --- Initialize DB with schema ---
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

@app.route("/users/<user_id>", methods=["GET"])
def get_user(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, name, email, created_at, updated_at FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"status": "error", "message": "User not found"}), 404
    return jsonify({"id": row[0], "name": row[1], "email": row[2], "created_at": row[3], "updated_at": row[4]})

@app.route("/users/<user_id>", methods=["PUT"])
def update_user(user_id):
    data = request.json
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET name=?, email=? WHERE id=?", 
                 (data["name"], data["email"], user_id))
        if c.rowcount == 0:
            conn.close()
            return jsonify({"status": "error", "message": "User not found"}), 404
        conn.commit()
        
        # Fetch the updated user
        c.execute("SELECT id, name, email, created_at, updated_at FROM users WHERE id=?", (user_id,))
        row = c.fetchone()
        conn.close()
        
        log_action("UPDATE", "users", f"User {user_id} updated")
        return jsonify({"id": row[0], "name": row[1], "email": row[2], "created_at": row[3], "updated_at": row[4]})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

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

@app.route("/users/<user_id>/stars", methods=["GET"])
def list_user_stars(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, user_id, created_at FROM stars WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return jsonify([{"id": r[0], "user_id": r[1], "created_at": r[2]} for r in rows])

@app.route("/stars/<user_id>", methods=["GET"])
def list_stars(user_id):
    # Keep the old endpoint for backward compatibility
    return list_user_stars(user_id)

@app.route("/stars/<star_id>", methods=["DELETE"])
def delete_star(star_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM stars WHERE id=?", (star_id,))
    if c.rowcount == 0:
        conn.close()
        return jsonify({"status": "error", "message": "Star not found"}), 404
    conn.commit()
    conn.close()
    log_action("DELETE", "stars", f"Star {star_id} deleted")
    return jsonify({"status": "ok"})

# Add endpoint to get a specific star
@app.route("/stars/<star_id>", methods=["GET"])
def get_star(star_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, user_id, created_at FROM stars WHERE id=?", (star_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"status": "error", "message": "Star not found"}), 404
    return jsonify({"id": row[0], "user_id": row[1], "created_at": row[2]})

@app.route("/users/<user_id>/stars", methods=["DELETE"])
def delete_user_stars(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM stars WHERE user_id=?", (user_id,))
    deleted_count = c.rowcount
    conn.commit()
    conn.close()
    log_action("DELETE", "stars", f"{deleted_count} stars deleted for user {user_id}")
    return jsonify({"status": "ok", "deleted": deleted_count})

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

@app.route("/users/<user_id>/nfc", methods=["GET"])
def list_user_nfc(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT tag_id, user_id, created_at, updated_at FROM nfc_tags WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return jsonify([{"tag_id": r[0], "user_id": r[1], "created_at": r[2], "updated_at": r[3]} for r in rows])

@app.route("/nfc/<user_id>", methods=["GET"])
def list_nfc(user_id):
    # Keep the old endpoint for backward compatibility
    return list_user_nfc(user_id)

@app.route("/nfc/<tag_id>", methods=["DELETE"])
def unlink_nfc(tag_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM nfc_tags WHERE tag_id=?", (tag_id,))
    if c.rowcount == 0:
        conn.close()
        return jsonify({"status": "error", "message": "Tag not found"}), 404
    conn.commit()
    conn.close()
    log_action("DELETE", "nfc_tags", f"Tag {tag_id} unlinked")
    return jsonify({"status": "ok"})

# Add endpoint to get a specific NFC tag
@app.route("/nfc/<tag_id>", methods=["GET"])
def get_nfc_tag(tag_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT tag_id, user_id, created_at, updated_at FROM nfc_tags WHERE tag_id=?", (tag_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"status": "error", "message": "Tag not found"}), 404
    return jsonify({"tag_id": row[0], "user_id": row[1], "created_at": row[2], "updated_at": row[3]})

@app.route("/nfc/<tag_id>/user", methods=["GET"])
def get_user_by_nfc(tag_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id FROM nfc_tags WHERE tag_id=?", (tag_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"status": "error", "message": "Tag not found"}), 404
    user_id = row[0]
    c.execute("SELECT id, name, email, created_at, updated_at FROM users WHERE id=?", (user_id,))
    user = c.fetchone()
    conn.close()
    if not user:
        return jsonify({"status": "error", "message": "User not found"}), 404
    return jsonify({"id": user[0], "name": user[1], "email": user[2], "created_at": user[3], "updated_at": user[4]})

# --- AUDIT LOGS ---
@app.route("/audit", methods=["GET"])
def audit():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, action, table_name, details, timestamp FROM audit_logs ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    return jsonify([{"id": r[0], "action": r[1], "table": r[2], "details": r[3], "timestamp": r[4]} for r in rows])

# Add DELETE endpoint for user
@app.route("/users/<user_id>", methods=["DELETE"])
def delete_user(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # First delete dependent records (stars and nfc tags will be deleted by foreign key constraints)
        c.execute("DELETE FROM users WHERE id=?", (user_id,))
        if c.rowcount == 0:
            conn.close()
            return jsonify({"status": "error", "message": "User not found"}), 404
            
        conn.commit()
        conn.close()
        log_action("DELETE", "users", f"User {user_id} deleted")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=1234)
