#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
MySQL -> текстовый дамп (DDL + данные) для анализа в чате.
"""

import sys
import os
import time
import argparse
from typing import List, Optional

import pymysql
from pymysql.cursors import SSCursor, SSDictCursor

SYSTEM_SCHEMAS = {"mysql", "information_schema", "performance_schema", "sys"}

def connect(host: str, port: int, user: str, password: str, db: Optional[str] = None):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=SSDictCursor if db else SSCursor,
        read_timeout=60,
        write_timeout=60,
    )

def _dict_get(row: dict, *candidates: str):
    # Безопасно достаём значение по одному из ключей (с учётом регистра);
    # если не нашли — берём первое значение.
    for k in candidates:
        if k in row:
            return row[k]
    lower_map = {k.lower(): k for k in row.keys()}
    for k in candidates:
        lk = k.lower()
        if lk in lower_map:
            return row[lower_map[lk]]
    # fallback
    return next(iter(row.values())) if row else None

def list_databases(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("SHOW DATABASES")
        rows = cur.fetchall()
    if rows and isinstance(rows[0], dict):
        return [next(iter(r.values())) for r in rows if next(iter(r.values())) not in SYSTEM_SCHEMAS]
    else:
        return [r[0] for r in rows if r[0] not in SYSTEM_SCHEMAS]

def list_tables(conn, db: str) -> List[str]:
    with conn.cursor() as cur:
        # Явно алиасим, чтобы ключ был "table_name" независимо от регистра
        cur.execute(
            "SELECT table_name AS table_name "
            "FROM information_schema.tables "
            "WHERE table_schema=%s ORDER BY table_name",
            (db,),
        )
        rows = cur.fetchall()
    if rows and isinstance(rows[0], dict):
        return [_dict_get(r, "table_name", "TABLE_NAME") for r in rows]
    else:
        return [r[0] for r in rows]

def get_create_table(conn, db: str, table: str) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SHOW CREATE TABLE `{db}`.`{table}`")
        row = cur.fetchone()
    if isinstance(row, dict):
        for k in row:
            if k.lower().startswith("create"):
                return row[k]
        return list(row.values())[-1]
    return row[1]

def get_row_count(conn, db: str, table: str) -> Optional[int]:
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM `{db}`.`{table}`")
            row = cur.fetchone()
        return int(_dict_get(row, "cnt", "COUNT(*)", "count(*)")) if isinstance(row, dict) else int(row[0])
    except Exception:
        return None

def get_columns(conn, db: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        # Тоже алиасим
        cur.execute(
            "SELECT column_name AS column_name "
            "FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s "
            "ORDER BY ordinal_position",
            (db, table),
        )
        rows = cur.fetchall()
    if rows and isinstance(rows[0], dict):
        return [_dict_get(r, "column_name", "COLUMN_NAME") for r in rows]
    else:
        return [r[0] for r in rows]

def iter_rows(conn, db: str, table: str, columns: List[str], row_limit: Optional[int], batch: int = 5000):
    col_list = ", ".join(f"`{c}`" for c in columns)
    sql = f"SELECT {col_list} FROM `{db}`.`{table}`"
    if row_limit is not None:
        sql += f" LIMIT {int(row_limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch)
            if not rows:
                break
            for r in rows:
                if isinstance(r, dict):
                    yield [r.get(c) for c in columns]
                else:
                    yield list(r)

def escape_tsv_value(v) -> str:
    r"""
    None -> \N; \t \n \r заменяем на пробелы; bytes -> hex.
    """
    if v is None:
        return r"\N"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "0x" + bytes(v).hex()
    s = str(v)
    if ("\n" in s) or ("\r" in s) or ("\t" in s):
        s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return s

def dump_database(host: str, port: int, user: str, password: str,
                  target_db: Optional[str], row_limit: Optional[int],
                  warn_threshold: Optional[int], outfile: str):
    root_conn = connect(host, port, user, password, None)
    databases = [target_db] if target_db else list_databases(root_conn)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    with open(outfile, "w", encoding="utf-8") as out:
        out.write(f"# MySQL TEXT DUMP @ {ts}\n")
        out.write(f"# Host={host} Port={port} User={user}\n")
        out.write(f"# Databases: {', '.join(databases)}\n\n")

        for db in databases:
            out.write(f"\n\n### DATABASE: `{db}`\n")
            try:
                conn = connect(host, port, user, password, db)
            except Exception as e:
                out.write(f"!! ERROR: cannot connect to `{db}`: {e}\n")
                continue

            try:
                tables = list_tables(conn, db)
            except Exception as e:
                out.write(f"!! ERROR: cannot list tables for `{db}`: {e}\n")
                conn.close()
                continue

            if not tables:
                out.write("-- (no tables)\n")
                conn.close()
                continue

            for t in tables:
                out.write(f"\n### TABLE: `{t}`\n")
                # DDL
                try:
                    ddl = get_create_table(conn, db, t)
                    out.write("-- CREATE TABLE:\n")
                    out.write(ddl + "\n\n")
                except Exception as e:
                    out.write(f"-- CREATE TABLE: ERROR: {e}\n\n")

                # Столбцы
                try:
                    columns = get_columns(conn, db, t)
                except Exception as e:
                    out.write(f"-- COLUMNS: ERROR: {e}\n")
                    columns = []

                # Кол-во строк
                row_count = get_row_count(conn, db, t)
                if row_count is not None:
                    out.write(f"-- ROW COUNT (approx/exact): {row_count}\n")
                    if warn_threshold is not None and row_limit is None and row_count > warn_threshold:
                        out.write(f"-- WARN: table is big (> {warn_threshold}). "
                                  f"Consider using --row-limit to keep file reasonable.\n")

                # Данные
                if columns:
                    out.write(f"-- COLUMNS ORDER: {', '.join(f'`{c}`' for c in columns)}\n")
                    out.write("-- ROWS:\n")
                    try:
                        for row in iter_rows(conn, db, t, columns, row_limit=row_limit, batch=5000):
                            line = "\t".join(escape_tsv_value(v) for v in row)
                            out.write(line + "\n")
                    except Exception as e:
                        out.write(f"-- DATA READ ERROR: {e}\n")
                else:
                    out.write("-- (no readable columns)\n")

            conn.close()

    print(f"[OK] Dump written to: {outfile}")

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MySQL -> текстовый дамп (DDL + данные)")
    p.add_argument("--host", default=os.getenv("DB_HOST", "79.174.89.178"))
    p.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "16844")))
    p.add_argument("--user", default=os.getenv("DB_USER", "HBusiwshu9whsd"))
    p.add_argument("--password", default=os.getenv("DB_PASSWORD", "NIUhbsuhwSU*GB0w87ygs08"))
    p.add_argument("--db", help="Конкретная БД (по умолчанию — все, кроме системных)")
    p.add_argument("--row-limit", type=int, help="Лимит строк на таблицу (по умолчанию — без лимита)")
    p.add_argument("--warn-threshold", type=int, default=500_000,
                   help="Предупреждать, если таблица больше этого порога (по умолчанию 500k)")
    p.add_argument("--out", help="Имя выходного файла (по умолчанию формируется автоматически)")
    return p.parse_args(argv)

def main(argv: List[str]) -> int:
    args = parse_args(argv)
    ts = time.strftime("%Y%m%d-%H%M%S")
    outfile = args.out or f"dump_{args.host.replace('.', '-')}_{ts}.txt"
    try:
        dump_database(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            target_db=args.db,
            row_limit=args.row_limit,
            warn_threshold=args.warn_threshold,
            outfile=outfile,
        )
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
        return 130
    except Exception as e:
        print(f"[ERROR] {e}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
