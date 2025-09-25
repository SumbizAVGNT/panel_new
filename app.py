from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import sqlite3
import os
from werkzeug.security import generate_password_hash, check_password_hash
import requests
from functools import wraps

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production!

# Discord OAuth2 configuration
DISCORD_CLIENT_ID = 'your-discord-client-id'
DISCORD_CLIENT_SECRET = 'your-discord-client-secret'
DISCORD_REDIRECT_URI = 'http://localhost:5000/discord/callback'
DISCORD_AUTH_URL = f'https://discord.com/api/oauth2/authorize?client_id={DISCORD_CLIENT_ID}&redirect_uri={DISCORD_REDIRECT_URI}&response_type=code&scope=identify'


# Database setup
def init_db():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()

    # Create users table
    c.execute('''
              CREATE TABLE IF NOT EXISTS users
              (
                  id
                  INTEGER
                  PRIMARY
                  KEY
                  AUTOINCREMENT,
                  username
                  TEXT
                  UNIQUE
                  NOT
                  NULL,
                  password_hash
                  TEXT
                  NOT
                  NULL,
                  discord_id
                  TEXT
                  UNIQUE,
                  is_superadmin
                  BOOLEAN
                  DEFAULT
                  FALSE,
                  created_at
                  TIMESTAMP
                  DEFAULT
                  CURRENT_TIMESTAMP
              )
              ''')

    # Create default superadmin if not exists
    c.execute("SELECT * FROM users WHERE is_superadmin = TRUE")
    if not c.fetchone():
        password_hash = generate_password_hash('admin123')  # Change this default password!
        c.execute(
            "INSERT INTO users (username, password_hash, is_superadmin) VALUES (?, ?, ?)",
            ('superadmin', password_hash, True)
        )

    conn.commit()
    conn.close()


# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated_function


# Superadmin required decorator
def superadmin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))

        conn = sqlite3.connect('database.db')
        c = conn.cursor()
        c.execute("SELECT is_superadmin FROM users WHERE id = ?", (session['user_id'],))
        user = c.fetchone()
        conn.close()

        if not user or not user[0]:
            flash('Superadmin privileges required', 'error')
            return redirect(url_for('dashboard'))

        return f(*args, **kwargs)

    return decorated_function


@app.route('/')
@login_required
def dashboard():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("SELECT username, is_superadmin FROM users WHERE id = ?", (session['user_id'],))
    user = c.fetchone()
    conn.close()

    stats = {
        'sales': '97.6K',
        'avg_sessions': '2.7k',
        'sessions_change': '5.2',
        'cost': '$100000',
        'users': '100K',
        'retention': '90%',
        'duration': '1yr',
        'tickets': '16.3',
        'new_tickets': '29',
        'cricket_received': '97.5K',
        'completed_tickets': '83%',
        'response_time': '89 y less'
    }

    return render_template('dashboard.html', stats=stats, username=user[0], is_superadmin=user[1])


@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = sqlite3.connect('database.db')
        c = conn.cursor()
        c.execute("SELECT id, password_hash FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user[1], password):
            session['user_id'] = user[0]
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid credentials', 'error')

    return render_template('login.html')


@app.route('/discord/login')
def discord_login():
    return redirect(DISCORD_AUTH_URL)


@app.route('/discord/callback')
def discord_callback():
    code = request.args.get('code')
    if not code:
        flash('Discord authentication failed', 'error')
        return redirect(url_for('login'))

    # Exchange code for access token
    data = {
        'client_id': DISCORD_CLIENT_ID,
        'client_secret': DISCORD_CLIENT_SECRET,
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': DISCORD_REDIRECT_URI,
        'scope': 'identify'
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post('https://discord.com/api/oauth2/token', data=data, headers=headers)
    if response.status_code != 200:
        flash('Discord authentication failed', 'error')
        return redirect(url_for('login'))

    access_token = response.json()['access_token']

    # Get user info from Discord
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    user_response = requests.get('https://discord.com/api/users/@me', headers=headers)
    if user_response.status_code != 200:
        flash('Failed to get user info from Discord', 'error')
        return redirect(url_for('login'))

    discord_user = user_response.json()
    discord_id = discord_user['id']

    # Check if user exists in database
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE discord_id = ?", (discord_id,))
    user = c.fetchone()

    if user:
        session['user_id'] = user[0]
        conn.close()
        flash('Login successful!', 'success')
        return redirect(url_for('dashboard'))
    else:
        conn.close()
        flash('No account linked to this Discord ID', 'error')
        return redirect(url_for('login'))


@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out successfully', 'success')
    return redirect(url_for('login'))


@app.route('/admin/users')
@superadmin_required
def admin_users():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("SELECT id, username, discord_id, is_superadmin, created_at FROM users")
    users = c.fetchall()
    conn.close()

    return render_template('admin_users.html', users=users)


@app.route('/admin/users/add', methods=['POST'])
@superadmin_required
def add_user():
    username = request.form['username']
    password = request.form['password']
    discord_id = request.form.get('discord_id', '')
    is_superadmin = 'is_superadmin' in request.form

    password_hash = generate_password_hash(password)

    try:
        conn = sqlite3.connect('database.db')
        c = conn.cursor()
        c.execute(
            "INSERT INTO users (username, password_hash, discord_id, is_superadmin) VALUES (?, ?, ?, ?)",
            (username, password_hash, discord_id if discord_id else None, is_superadmin)
        )
        conn.commit()
        conn.close()
        flash('User added successfully', 'success')
    except sqlite3.IntegrityError:
        flash('Username or Discord ID already exists', 'error')

    return redirect(url_for('admin_users'))


@app.route('/admin/users/delete/<int:user_id>')
@superadmin_required
def delete_user(user_id):
    if user_id == session['user_id']:
        flash('Cannot delete your own account', 'error')
        return redirect(url_for('admin_users'))

    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    flash('User deleted successfully', 'success')
    return redirect(url_for('admin_users'))


if __name__ == '__main__':
    init_db()
    app.run(debug=True)