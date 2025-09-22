from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import sqlite3
import datetime
import traceback
import os
import time
import threading

app = Flask(__name__)

# Database connection with retry logic and timeout
def get_db_connection(timeout=30.0, retries=3):
    """Get a database connection with retry logic and prope@app.route("/stars/<star_id>", methods=["DELETE"])
def delete_star(star_id):
    try:
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("DELETE FROM stars WHERE id=?", (star_id,))
            if c.rowcount == 0:
                conn.close()
                return jsonify({"status": "error", "message": "Star not found"}), 404
            conn.commit()
            conn.close()
        
        log_action("DELETE", "stars", f"Star {star_id} deleted")
        return jsonify({"status": "ok"})
    except Exception as e:
        print(f"Error deleting star: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500ndling."""
    for attempt in range(retries):
        try:
            conn = sqlite3.connect(DB_FILE, timeout=timeout)
            # Enable WAL mode for better concurrent access
            conn.execute('PRAGMA journal_mode=WAL;')
            # Set busy timeout
            conn.execute(f'PRAGMA busy_timeout={int(timeout * 1000)};')
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower() and attempt < retries - 1:
                print(f"Database locked, retrying in {0.1 * (attempt + 1)} seconds... (attempt {attempt + 1}/{retries})")
                time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                continue
            raise e
    raise sqlite3.OperationalError("Failed to get database connection after retries")

# Thread lock for critical database operations
db_lock = threading.Lock()

# Configure CORS more explicitly
cors = CORS(app, resources={
    r"/*": {
        "origins": "*",  # Allow all origins (development mode)
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "Accept"]
    }
})

# Add CORS headers to all responses
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Accept'
    return response

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
    with db_lock:
        conn = get_db_connection()
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
    """Thread-safe logging function that uses the same connection when possible."""
    try:
        with db_lock:
            conn = get_db_connection(timeout=10.0)
            c = conn.cursor()
            c.execute("INSERT INTO audit_logs (action, table_name, details, timestamp) VALUES (?, ?, ?, ?)",
                      (action, table, details, datetime.datetime.utcnow().isoformat()))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Warning: Failed to log action {action} on {table}: {e}")
        # Don't fail the main operation if logging fails

# Function to add sample data if tables are empty
def populate_sample_data():
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if users table is empty
        c.execute("SELECT COUNT(*) FROM users")
        user_count = c.fetchone()[0]
        
        if user_count == 0:
            print("Adding sample users...")
            # Add a sample user
            sample_user_id = "8472A92D-C6D5-4014-82FE-9D47348DAE24"  # This matches the ID in your logs
            c.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                      (sample_user_id, "Sample User", "sample@example.com"))
            
            # Add some sample stars for this user
            for i in range(5):
                star_id = f"star_{i}_{sample_user_id[:8]}"
                c.execute("INSERT INTO stars (id, user_id) VALUES (?, ?)",
                          (star_id, sample_user_id))
            
            # Add a sample NFC tag linked to this user
            c.execute("INSERT INTO nfc_tags (tag_id, user_id) VALUES (?, ?)",
                      ("04BC777A7B1190", sample_user_id))  # This matches the NFC tag ID in your logs
            
            conn.commit()
            print("Sample data added successfully")
        
        conn.close()

init_db()
populate_sample_data()

# --- USERS ---
@app.route("/users", methods=["POST"])
def create_user():
    data = request.json
    try:
        # Log the incoming data for debugging
        print(f"Create user request received: {data}")
        
        # Check if all required fields are present
        required_fields = ["id", "name", "email"]
        for field in required_fields:
            if field not in data:
                error_msg = f"Missing required field: {field}"
                print(error_msg)
                return jsonify({"status": "error", "message": error_msg}), 400
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Check if email already exists
        c.execute("SELECT id FROM users WHERE email=?", (data["email"],))
        existing_user = c.fetchone()
        if existing_user:
            conn.close()
            error_msg = f"Email already exists: {data['email']}"
            print(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 400
        
        # Insert the new user
        c.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                  (data["id"], data["name"], data["email"]))
        conn.commit()
        
        # Fetch the created user to return
        c.execute("SELECT id, name, email, created_at, updated_at FROM users WHERE id=?", (data["id"],))
        user = c.fetchone()
        conn.close()
        
        log_action("INSERT", "users", f"User {data['id']} created")
        print(f"User created successfully: {data['id']}")
        
        # Return the created user object
        return jsonify({"id": user[0], "name": user[1], "email": user[2], 
                        "created_at": user[3], "updated_at": user[4]}), 201
    except Exception as e:
        print(f"Error creating user: {str(e)}")
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
        print(f"Creating star: {data}")
        
        # Validate required fields
        if not data or "id" not in data or "user_id" not in data:
            error_msg = "Missing required fields: id and user_id"
            print(f"Error: {error_msg}")
            return jsonify({"status": "error", "message": error_msg}), 400
        
        star_id = data["id"]
        user_id = data["user_id"]
        
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Check if user exists first
            c.execute("SELECT id FROM users WHERE id=?", (user_id,))
            if not c.fetchone():
                conn.close()
                error_msg = f"User not found: {user_id}"
                print(f"Error: {error_msg}")
                return jsonify({"status": "error", "message": error_msg}), 400
            
            # Check if star already exists
            c.execute("SELECT id FROM stars WHERE id=?", (star_id,))
            if c.fetchone():
                conn.close()
                print(f"Star {star_id} already exists")
                return jsonify({"status": "ok", "message": "Star already exists"}), 200
            
            # Insert the star
            c.execute("INSERT INTO stars (id, user_id) VALUES (?, ?)", (star_id, user_id))
            conn.commit()
            conn.close()
            
        # Log action (separate connection with error handling)
        log_action("INSERT", "stars", f"Star {star_id} for user {user_id}")
        print(f"Successfully created star {star_id} for user {user_id}")
        return jsonify({"status": "ok"}), 201
        
    except sqlite3.IntegrityError as e:
        error_msg = f"Database constraint violation: {str(e)}"
        print(f"Error: {error_msg}")
        return jsonify({"status": "error", "message": error_msg}), 400
    except Exception as e:
        error_msg = f"Unexpected error creating star: {str(e)}"
        print(f"Error: {error_msg}")
        return jsonify({"status": "error", "message": error_msg}), 500

@app.route("/users/<user_id>/stars", methods=["GET"])
def list_user_stars(user_id):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id, user_id, created_at FROM stars WHERE user_id=?", (user_id,))
        rows = c.fetchall()
        conn.close()
        return jsonify([{"id": r[0], "user_id": r[1], "created_at": r[2]} for r in rows])
    except Exception as e:
        print(f"Error listing user stars: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

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
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id, user_id, created_at FROM stars WHERE id=?", (star_id,))
        row = c.fetchone()
        conn.close()
        if not row:
            return jsonify({"status": "error", "message": "Star not found"}), 404
        return jsonify({"id": row[0], "user_id": row[1], "created_at": row[2]})
    except Exception as e:
        print(f"Error getting star: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/users/<user_id>/stars", methods=["DELETE"])
def delete_user_stars(user_id):
    try:
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("DELETE FROM stars WHERE user_id=?", (user_id,))
            deleted_count = c.rowcount
            conn.commit()
            conn.close()
        
        log_action("DELETE", "stars", f"{deleted_count} stars deleted for user {user_id}")
        return jsonify({"status": "ok", "deleted": deleted_count})
    except Exception as e:
        print(f"Error deleting user stars: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- NFC TAGS ---
@app.route("/nfc", methods=["POST"])
def create_nfc():
    data = request.json
    try:
        # Log the incoming request for debugging
        print(f"NFC link request received: {data}")
        
        # Validate the request data
        if not data:
            error_msg = "No JSON data provided"
            print(f"Error: {error_msg}")
            return jsonify({"status": "error", "message": error_msg}), 400
        
        # Check for required fields
        if "tag_id" not in data or not data["tag_id"]:
            error_msg = "Missing or empty tag_id field"
            print(f"Error: {error_msg}")
            return jsonify({"status": "error", "message": error_msg}), 400
            
        if "user_id" not in data or not data["user_id"]:
            error_msg = "Missing or empty user_id field"
            print(f"Error: {error_msg}")
            return jsonify({"status": "error", "message": error_msg}), 400
        
        tag_id = data["tag_id"]
        user_id = data["user_id"]
        
        print(f"Attempting to link tag '{tag_id}' to user '{user_id}'")
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Check if user exists
        c.execute("SELECT id FROM users WHERE id=?", (user_id,))
        if not c.fetchone():
            conn.close()
            error_msg = f"User not found: {user_id}"
            print(f"Error: {error_msg}")
            return jsonify({"status": "error", "message": error_msg}), 400
        
        # Check if tag is already linked to another user
        c.execute("SELECT user_id FROM nfc_tags WHERE tag_id=?", (tag_id,))
        existing_link = c.fetchone()
        if existing_link:
            if existing_link[0] == user_id:
                conn.close()
                print(f"Tag {tag_id} is already linked to user {user_id}")
                return jsonify({"status": "ok", "message": "Tag already linked to this user"}), 200
            else:
                conn.close()
                error_msg = f"Tag {tag_id} is already linked to another user"
                print(f"Error: {error_msg}")
                return jsonify({"status": "error", "message": error_msg}), 400
        
        # Insert the new NFC tag link
        c.execute("INSERT INTO nfc_tags (tag_id, user_id) VALUES (?, ?)", (tag_id, user_id))
        conn.commit()
        conn.close()
        
        log_action("INSERT", "nfc_tags", f"Tag {tag_id} for user {user_id}")
        print(f"Successfully linked tag {tag_id} to user {user_id}")
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        print(f"Error linking NFC tag: {str(e)}")
        print(f"Request data was: {data}")
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
    
    print(f"Looking up user for NFC tag: {tag_id}")
    
    c.execute("SELECT user_id FROM nfc_tags WHERE tag_id=?", (tag_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        print(f"NFC tag not found: {tag_id}")
        return jsonify({"status": "error", "message": "Tag not found"}), 404
        
    user_id = row[0]
    print(f"Found user_id: {user_id} for tag: {tag_id}")
    
    c.execute("SELECT id, name, email, created_at, updated_at FROM users WHERE id=?", (user_id,))
    user = c.fetchone()
    conn.close()
    
    if not user:
        print(f"User not found with ID: {user_id}")
        return jsonify({"status": "error", "message": "User not found"}), 404
        
    print(f"Returning user details for ID: {user_id}")
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

# Simple health check endpoint
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "api_version": "1.0.0"
    })

if __name__ == "__main__":
    print("Starting Daydream Sydney API server...")
    print(f"Database: {os.path.abspath(DB_FILE)}")
    app.run(host='0.0.0.0', port=1234, debug=True)
