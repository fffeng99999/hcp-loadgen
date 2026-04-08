import argparse
import os
import sys
import uuid

import psycopg2


TARGET_DATABASE = "hcp_server"
TARGET_SCHEMA = "loadgendata"
DEFAULT_DSN = f"postgres://user_rbc3B8:password_DfA4Pw@192.168.58.102:5432/{TARGET_DATABASE}?sslmode=disable"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="向 loadgen 数据库写入一个用户账户")
    parser.add_argument("--username", default=f"user_{uuid.uuid4().hex[:8]}")
    parser.add_argument("--address", default=f"0x{uuid.uuid4().hex[:40]}")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dsn = os.getenv("LOADGEN_DB_DSN", DEFAULT_DSN)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            current_database = cur.fetchone()[0]
            if current_database != TARGET_DATABASE:
                raise RuntimeError(
                    f"当前连接数据库为 {current_database}，与目标数据库 {TARGET_DATABASE} 不一致"
                )

            cur.execute(
                """
                INSERT INTO loadgendata.accounts (address, username)
                VALUES (%s, %s)
                ON CONFLICT (address) DO UPDATE SET username = EXCLUDED.username
                RETURNING account_id, address, username, created_at;
                """,
                (args.address, args.username),
            )
            account_id, address, username, created_at = cur.fetchone()
            print("写入成功")
            print(f"database: {TARGET_DATABASE}")
            print(f"schema: {TARGET_SCHEMA}")
            print(f"account_id: {account_id}")
            print(f"address: {address}")
            print(f"username: {username}")
            print(f"created_at: {created_at}")
    except Exception as exc:
        print("写入失败")
        print(f"错误信息: {exc}")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
