import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Dict, List

import psycopg2


TARGET_DATABASE = "hcap_server"
TARGET_SCHEMA = "loadgendata"
USER_COUNT = 10
DEFAULT_DSN = f"postgres://user_rbc3B8:password_DfA4Pw@192.168.58.102:5432/{TARGET_DATABASE}?sslmode=disable"


def run_init_script() -> None:
    script_dir = Path(__file__).resolve().parent
    init_script = script_dir / "init_loadgen_db.py"
    env = os.environ.copy()
    if "LOADGEN_DB_DSN" not in env:
        env["LOADGEN_DB_DSN"] = DEFAULT_DSN
    result = subprocess.run(
        [sys.executable, str(init_script)],
        input="yes\n",
        text=True,
        capture_output=True,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"初始化脚本执行失败\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    print(result.stdout.strip())


def build_users() -> List[Dict[str, str]]:
    users: List[Dict[str, str]] = []
    for idx in range(1, USER_COUNT + 1):
        username = f"user{idx:03d}"
        address = f"0x{idx:040x}"[:42]
        users.append({"username": username, "address": address})
    return users


def seed_data(dsn: str) -> Dict[str, int]:
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

            users = build_users()
            account_ids: List[int] = []
            for user in users:
                cur.execute(
                    """
                    INSERT INTO loadgendata.accounts (address, username)
                    VALUES (%s, %s)
                    RETURNING account_id;
                    """,
                    (user["address"], user["username"]),
                )
                account_ids.append(cur.fetchone()[0])

            for idx, account_id in enumerate(account_ids, start=1):
                available_hcap = Decimal("100000.00000000") + Decimal(idx * 1000)
                available_usdt = Decimal("50000.00000000") + Decimal(idx * 500)
                cur.execute(
                    """
                    INSERT INTO loadgendata.balances (account_id, asset_symbol, available, frozen)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (account_id, "HCP", available_hcap, Decimal("0.00000000")),
                )
                cur.execute(
                    """
                    INSERT INTO loadgendata.balances (account_id, asset_symbol, available, frozen)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (account_id, "USDT", available_usdt, Decimal("0.00000000")),
                )

            buy_order_ids: List[int] = []
            sell_order_ids: List[int] = []
            statuses = ("PENDING", "PARTIAL", "FILLED")
            for idx, account_id in enumerate(account_ids, start=1):
                buy_price = Decimal("100.00000000") + Decimal(idx)
                sell_price = Decimal("101.00000000") + Decimal(idx)
                qty = Decimal("10.00000000") + Decimal(idx) / Decimal("10")
                buy_filled = qty if idx % 2 == 0 else qty / Decimal("2")
                sell_filled = qty if idx % 3 == 0 else qty / Decimal("3")
                cur.execute(
                    """
                    INSERT INTO loadgendata.orders (account_id, side, price, quantity, filled_qty, status)
                    VALUES (%s, 'BUY', %s, %s, %s, %s)
                    RETURNING order_id;
                    """,
                    (account_id, buy_price, qty, buy_filled, statuses[idx % len(statuses)]),
                )
                buy_order_ids.append(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO loadgendata.orders (account_id, side, price, quantity, filled_qty, status)
                    VALUES (%s, 'SELL', %s, %s, %s, %s)
                    RETURNING order_id;
                    """,
                    (
                        account_ids[(idx % len(account_ids))],
                        sell_price,
                        qty,
                        sell_filled,
                        statuses[(idx + 1) % len(statuses)],
                    ),
                )
                sell_order_ids.append(cur.fetchone()[0])

            for idx in range(USER_COUNT):
                trade_price = Decimal("100.50000000") + Decimal(idx) / Decimal("10")
                trade_qty = Decimal("5.00000000") + Decimal(idx) / Decimal("20")
                tx_hash = f"tx_{idx + 1:04d}_{buy_order_ids[idx]}_{sell_order_ids[idx]}"
                latency_ms = 10 + idx * 3
                cur.execute(
                    """
                    INSERT INTO loadgendata.trades (buy_order_id, sell_order_id, price, quantity, tx_hash, latency_ms)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        buy_order_ids[idx],
                        sell_order_ids[idx],
                        trade_price,
                        trade_qty,
                        tx_hash,
                        latency_ms,
                    ),
                )

            cur.execute("SELECT COUNT(*) FROM loadgendata.accounts;")
            accounts_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM loadgendata.balances;")
            balances_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM loadgendata.orders;")
            orders_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM loadgendata.trades;")
            trades_count = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "accounts": accounts_count,
        "balances": balances_count,
        "orders": orders_count,
        "trades": trades_count,
    }


def main() -> int:
    dsn = os.getenv("LOADGEN_DB_DSN", DEFAULT_DSN)
    try:
        run_init_script()
        counts = seed_data(dsn)
    except Exception as exc:
        print("造数失败")
        print(f"错误信息: {exc}")
        return 1

    print("造数成功")
    print(f"database: {TARGET_DATABASE}")
    print(f"schema: {TARGET_SCHEMA}")
    for table_name, count in counts.items():
        print(f"{table_name}: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
