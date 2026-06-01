import os
import sys
from typing import Dict, List, Tuple

import psycopg2
from psycopg2 import sql


TARGET_DATABASE = "hcap_server"
DEFAULT_DSN = f"postgres://user_rbc3B8:password_DfA4Pw@192.168.58.102:5432/{TARGET_DATABASE}?sslmode=disable"
TARGET_SCHEMA = "loadgendata"
TARGET_TABLES = ("accounts", "balances", "orders", "trades")
TARGET_INDEXES = ("idx_trades_tx_hash", "idx_balances_account_id")


def list_schema_tables(cur) -> List[str]:
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = %s
        ORDER BY tablename;
        """,
        (TARGET_SCHEMA,),
    )
    return [row[0] for row in cur.fetchall()]


def fetch_sample_rows(cur, table_name: str, limit: int = 3) -> List[Dict[str, object]]:
    cur.execute(
        sql.SQL("SELECT * FROM {}.{} LIMIT %s;").format(
            sql.Identifier(TARGET_SCHEMA),
            sql.Identifier(table_name),
        ),
        (limit,),
    )
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def detect_data(cur) -> Dict[str, List[Dict[str, object]]]:
    samples: Dict[str, List[Dict[str, object]]] = {}
    for table_name in list_schema_tables(cur):
        cur.execute(
            sql.SQL("SELECT EXISTS (SELECT 1 FROM {}.{} LIMIT 1);").format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(table_name),
            )
        )
        has_rows = cur.fetchone()[0]
        if has_rows:
            samples[table_name] = fetch_sample_rows(cur, table_name, 3)
    return samples


def drop_all_tables(cur) -> None:
    tables = list_schema_tables(cur)
    if not tables:
        return
    identifiers = [
        sql.SQL("{}.{}").format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(name))
        for name in tables
    ]
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.SQL(", ").join(identifiers)))


def create_target_schema_objects(cur) -> Tuple[List[str], List[str]]:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
    cur.execute(f"SET search_path TO {TARGET_SCHEMA}, public;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_id BIGSERIAL PRIMARY KEY,
            address VARCHAR(64) NOT NULL UNIQUE,
            username VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balances (
            account_id BIGINT REFERENCES accounts(account_id),
            asset_symbol VARCHAR(10) NOT NULL,
            available DECIMAL(20, 8) DEFAULT 0,
            frozen DECIMAL(20, 8) DEFAULT 0,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (account_id, asset_symbol)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGSERIAL PRIMARY KEY,
            account_id BIGINT REFERENCES accounts(account_id),
            side VARCHAR(10) CHECK (side IN ('BUY', 'SELL')),
            price DECIMAL(20, 8) NOT NULL,
            quantity DECIMAL(20, 8) NOT NULL,
            filled_qty DECIMAL(20, 8) DEFAULT 0,
            status VARCHAR(20) DEFAULT 'PENDING',
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id BIGSERIAL PRIMARY KEY,
            buy_order_id BIGINT,
            sell_order_id BIGINT,
            price DECIMAL(20, 8) NOT NULL,
            quantity DECIMAL(20, 8) NOT NULL,
            tx_hash VARCHAR(128),
            latency_ms INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_tx_hash ON trades(tx_hash);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_balances_account_id ON balances(account_id);")
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = %s
        AND tablename IN ('accounts', 'balances', 'orders', 'trades')
        ORDER BY tablename;
        """,
        (TARGET_SCHEMA,),
    )
    created_tables = [row[0] for row in cur.fetchall()]
    cur.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = %s
        AND indexname IN ('idx_trades_tx_hash', 'idx_balances_account_id')
        ORDER BY indexname;
        """,
        (TARGET_SCHEMA,),
    )
    created_indexes = [row[0] for row in cur.fetchall()]
    return created_tables, created_indexes


def ensure_database(dsn: str) -> Tuple[List[str], List[str], bool]:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    created_tables: List[str] = []
    created_indexes: List[str] = []
    existed_data = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            current_database = cur.fetchone()[0]
            if current_database != TARGET_DATABASE:
                raise RuntimeError(
                    f"当前连接数据库为 {current_database}，与目标数据库 {TARGET_DATABASE} 不一致"
                )

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
            samples = detect_data(cur)
            existed_data = bool(samples)
            if existed_data:
                print("检测到数据库已有数据")
                for table_name, rows in samples.items():
                    print(f"表 {TARGET_SCHEMA}.{table_name} 样例数据（最多3条）:")
                    for row in rows:
                        print(f"- {row}")
                answer = input("输入 yes 删除全部表并重新初始化，输入 no 停止执行: ").strip().lower()
                if answer != "yes":
                    print("用户选择 no，脚本已停止执行")
                    return [], [], existed_data
            drop_all_tables(cur)
            created_tables, created_indexes = create_target_schema_objects(cur)
    finally:
        conn.close()
    return created_tables, created_indexes, existed_data


def main() -> int:
    dsn = os.getenv("LOADGEN_DB_DSN", DEFAULT_DSN)
    try:
        tables, indexes, existed_data = ensure_database(dsn)
    except Exception as exc:
        print("数据库初始化失败")
        print(f"错误信息: {exc}")
        return 1

    if not tables and not indexes and existed_data:
        return 0

    print("数据库连接与初始化成功")
    print(f"database: {TARGET_DATABASE}")
    print(f"schema: {TARGET_SCHEMA}")
    print("已确认的表:")
    for name in tables:
        print(f"- {name}")
    print("已确认的索引:")
    for name in indexes:
        print(f"- {name}")

    missing_tables = sorted(set(TARGET_TABLES) - set(tables))
    missing_indexes = sorted(set(TARGET_INDEXES) - set(indexes))
    if missing_tables or missing_indexes:
        print("存在未确认对象")
        if missing_tables:
            print("缺失表: " + ", ".join(missing_tables))
        if missing_indexes:
            print("缺失索引: " + ", ".join(missing_indexes))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
