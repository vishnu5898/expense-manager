import datetime
import sqlite3

from sqlite3 import Connection, Cursor
from typing import Tuple, List


def create_sqlite_conn() -> Tuple[Connection, Cursor]:
    """Create a sqlite connection."""

    conn = sqlite3.connect("expense_manager.db")
    cur = conn.cursor()

    return conn, cur


def create_table(conn: Connection, cur: Cursor) -> None:
    """Create required table for the application."""

    table_statement = (
        f"CREATE TABLE expense_records ("
        f"transaction_id INTEGER,"
        f"category TEXT,"
        f"description TEXT,"
        f"amount REAL,"
        f"expense_date DATE,"
        f"updated_at TIMESTAMP,"
        f"CONSTRAINT unique_columns UNIQUE(transaction_id))"
    )

    try:
        cur.execute(table_statement)
        conn.commit()
        print("Required table created successfully")
    except sqlite3.OperationalError:
        pass
    except Exception:
        raise


def get_all_the_expenses(cur: Cursor) -> List[dict]:
    """Get all the expenses from the database."""

    select_statement = (
        f"SELECT * from expense_records"
    )

    try:
        cur.execute(select_statement)
    except Exception:
        raise

    all_rows = []
    for row in cur.fetchall():
        data_dict = {}
        for i, col in enumerate(cur.description):
            data_dict[col[0]] = row[i]
        all_rows.append(data_dict)

    return all_rows


def get_max_expense_id(cur: Cursor) -> int:
    """Get the maximum transaction_id from the table."""

    select_statement = (
        f"SELECT MAX(transaction_id) FROM expense_records"
    )

    try:
        cur.execute(select_statement)
    except Exception:
        raise

    return cur.fetchone()[0]


def insert_expense_to_table(conn: Connection, cur: Cursor, data_dict: dict) -> None:
    """Insert the expense data to the table."""

    columns = data_dict.keys()
    columns_skel = ", ".join(columns)
    columns_placeholder_skel = ", ".join([f":{col}" for col in columns])
    insert_statement = (
        f"INSERT INTO expense_records ({columns_skel}) "
        f"VALUES ({columns_placeholder_skel})"
    )

    try:
        cur.execute(insert_statement, data_dict)
        conn.commit()
        print("Successfully saved the expense")
    except Exception:
        raise


def remove_transaction_id(conn: Connection, cur: Cursor, transaction_id: str) -> None:
    """Remove the transaction_id from the table."""

    remove_statement = (
        "DELETE FROM expense_records WHERE "
        "transaction_id = :transaction_id"
    )

    try:
        cur.execute(remove_statement, {"transaction_id": transaction_id})
        conn.commit()
        print(f"Successfully removed transaction_id: {transaction_id}")
    except Exception:
        raise


def get_total_expense(cur: Cursor) -> int:
    """Get total expense from the table."""

    select_statement = (
        f"SELECT SUM(amount) FROM expense_records"
    )

    try:
        cur.execute(select_statement)
    except Exception:
        raise

    return cur.fetchone()[0]


def main():
    conn, cur = create_sqlite_conn()
    create_table(conn, cur)
    while True:
        print("1) View expenses")
        print("2) Add expense")
        print("3) Remove expense")
        print("4) Total expenses")
        print("Press q for exiting the program")
        user_command = input()
        if user_command == "q":
            break
        elif user_command == "1":
            all_rows = get_all_the_expenses(cur)
            if not all_rows:
                print("="*10)
                print("No expenses added")
                print("="*10)
                continue
            print("="*10)
            for row in all_rows:
                print(row)
            print("="*10)
        elif user_command == "2":
            max_transaction_id = get_max_expense_id(cur)
            if not max_transaction_id:
                max_transaction_id = "1"
            else:
                max_transaction_id = str(int(max_transaction_id) + 1)
            category = input("Enter category: ")
            description = input("Enter description: ")
            amount = input("Enter amount spend (in Rs): ")
            expense_date = input("Enter the spend date (DD/MM/YYYY}: ")
            
            expense_date = datetime.datetime.strptime(expense_date, "%d/%m/%Y")

            data_dict = {
                "transaction_id": max_transaction_id,
                "category": category,
                "description": description,
                "amount": amount,
                "expense_date": expense_date,
                "updated_at": datetime.datetime.now()
            }

            insert_expense_to_table(conn, cur, data_dict)
        elif user_command == "3":
            transaction_id_to_be_removed = input("Enter transaction_id to be removed: ")
            remove_transaction_id(conn, cur, transaction_id_to_be_removed)
        elif user_command == "4":
            print("="*10)
            print(f"Total expense: {get_total_expense(cur)}")
            print("="*10)


            




if __name__ == "__main__":
    main()