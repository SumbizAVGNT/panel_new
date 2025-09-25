# init_db.py — запускать из корня проекта
from pathlib import Path

# ✅ Явно загружаем .env (на случай, если запускается не через Flask)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
except Exception:
    pass

from app.database import get_db_connection, init_db

conn = get_db_connection(None)
init_db(conn)
conn.close()
print("✅ Database schema ensured in MySQL (ENV-based connection).")
