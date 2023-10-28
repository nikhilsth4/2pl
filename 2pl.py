import re
from collections import deque


def begin_transaction(transaction_id, transaction_table, global_timestamp):
    if transaction_id not in transaction_table:
        transaction = {
            "transaction_state": "active",
            "locked_items": set(),
            "timestamp": global_timestamp,
        }
        transaction_table[transaction_id] = transaction
        print(f"Transaction {transaction_id} begins at TS({global_timestamp}).")


def read_item(transaction_id, item_id, transaction_table, lock_table, global_timestamp):
    current_transaction = transaction_table[transaction_id]
    if item_id in lock_table:
        current_lock = lock_table[item_id]
        if current_lock["lock_state"] == "read":
            if current_lock["holding_transaction"] == transaction_id:
                print(f"Transaction {transaction_id} reads {item_id}.")
            else:
                if current_lock["holding_transaction"] is not None:
                    if (
                        current_transaction["timestamp"]
                        < transaction_table[current_lock["holding_transaction"]][
                            "timestamp"
                        ]
                    ):
                        current_transaction["transaction_state"] = "waiting"
                        current_lock["waiting_transactions"].append(transaction_id)
                        print(
                            f"Transaction {transaction_id} is younger and waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                        )
                    else:
                        current_transaction["transaction_state"] = "aborted"
                        print(
                            f"Transaction {transaction_id} is older and aborted due to conflict with Transaction {current_lock['holding_transaction']} on {item_id}."
                        )
                else:
                    current_transaction["transaction_state"] = "aborted"
                    print(
                        f"Transaction {transaction_id} is aborted due to conflict with no holding transaction on {item_id}."
                    )
        else:
            if current_lock["holding_transaction"] == transaction_id:
                print(f"Transaction {transaction_id} reads {item_id}.")
            else:
                if current_lock["holding_transaction"] is not None:
                    if (
                        current_transaction["timestamp"]
                        < transaction_table[current_lock["holding_transaction"]][
                            "timestamp"
                        ]
                    ):
                        current_transaction["transaction_state"] = "aborted"
                        print(
                            f"Transaction {transaction_id} is younger and aborted due to conflict with Transaction {current_lock['holding_transaction']} on {item_id}."
                        )
                    else:
                        current_transaction["transaction_state"] = "waiting"
                        current_lock["waiting_transactions"].append(transaction_id)
                        print(
                            f"Transaction {transaction_id} is older and waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                        )
                else:
                    current_transaction["transaction_state"] = "aborted"
                    print(
                        f"Transaction {transaction_id} is aborted due to conflict with no holding transaction on {item_id}."
                    )
    else:
        lock_table[item_id] = {
            "lock_state": "read",
            "holding_transaction": transaction_id,
            "waiting_transactions": deque(),
        }
        current_transaction["locked_items"].add(item_id)
        print(f"Transaction {transaction_id} reads {item_id}.")


def write_item(
    transaction_id, item_id, transaction_table, lock_table, global_timestamp
):
    current_transaction = transaction_table[transaction_id]
    if item_id in lock_table:
        current_lock = lock_table[item_id]
        if current_lock["lock_state"] == "read":
            if current_lock["holding_transaction"] == transaction_id:
                current_lock["lock_state"] = "write"
                print(
                    f"Transaction {transaction_id} writes {item_id}."
                )
            else:
                if current_lock["holding_transaction"] is not None:
                    if (
                        current_transaction["timestamp"]
                        < transaction_table[current_lock["holding_transaction"]][
                            "timestamp"
                        ]
                    ):
                        current_transaction["transaction_state"] = "waiting"
                        current_lock["waiting_transactions"].append(transaction_id)
                        print(
                            f"Transaction {transaction_id} is younger and waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                        )
                    else:
                        current_transaction["transaction_state"] = "aborted"
                        print(
                            f"Transaction {transaction_id} is older and aborted due to conflict with Transaction {current_lock['holding_transaction']} on {item_id}."
                        )
                else:
                    current_transaction["transaction_state"] = "aborted"
                    print(
                        f"Transaction {transaction_id} is aborted due to conflict with no holding transaction on {item_id}."
                    )
        else:
            if current_lock["holding_transaction"] == transaction_id:
                current_lock["lock_state"] = "write"
                print(
                    f"Transaction {transaction_id} writes {item_id}."
                )
            else:
                if current_lock["holding_transaction"] is not None:
                    if (
                        current_transaction["timestamp"]
                        < transaction_table[current_lock["holding_transaction"]][
                            "timestamp"
                        ]
                    ):
                        current_transaction["transaction_state"] = "aborted"
                        print(
                            f"Transaction {transaction_id} is younger and aborted due to conflict with Transaction {current_lock['holding_transaction']} on {item_id}."
                        )
                    else:
                        current_transaction["transaction_state"] = "waiting"
                        current_lock["waiting_transactions"].append(transaction_id)
                        print(
                            f"Transaction {transaction_id} is older and waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                        )
                else:
                    current_transaction["transaction_state"] = "aborted"
                    print(
                        f"Transaction {transaction_id} is aborted due to conflict with no holding transaction on {item_id}."
                    )
    else:
        lock_table[item_id] = {
            "lock_state": "write",
            "holding_transaction": transaction_id,
            "waiting_transactions": deque(),
        }
        current_transaction["locked_items"].add(item_id)
        print(f"Transaction {transaction_id} writes {item_id}.")


def end_transaction(transaction_id, transaction_table, lock_table):
    current_transaction = transaction_table[transaction_id]
    if current_transaction["transaction_state"] == "active":
        for item_id in current_transaction["locked_items"]:
            unlock_item(item_id, lock_table, transaction_table)
        print(f"Transaction {transaction_id} commits.")
    else:
        print(f"Transaction {transaction_id} cannot be committed.")


def unlock_item(item_id, lock_table, transaction_table):
    current_lock = lock_table[item_id]
    if current_lock:
        current_lock["lock_state"] = "unlocked"
        current_lock["holding_transaction"] = None
        grant_lock(item_id, lock_table, transaction_table)


def grant_lock(item_id, lock_table, transaction_table):
    current_lock = lock_table[item_id]
    if current_lock and current_lock["waiting_transactions"]:
        waiting_transactions = list(current_lock["waiting_transactions"])
        waiting_transactions.sort(key=lambda t: transaction_table[t]["timestamp"])
        for next_transaction in waiting_transactions:
            next_transaction_entry = transaction_table[next_transaction]
            if next_transaction_entry["transaction_state"] == "waiting":
                if current_lock["lock_state"] == "read":
                    read_item(
                        next_transaction,
                        item_id,
                        transaction_table,
                        lock_table,
                        next_transaction_entry["timestamp"],
                    )
                else:
                    write_item(
                        next_transaction,
                        item_id,
                        transaction_table,
                        lock_table,
                        next_transaction_entry["timestamp"],
                    )


def parse_operation(line):
    match = re.match(r"([brwe])(\d+)\s*(\((\w+)\))?;", line)
    if match:
        op = match.group(1)
        tid = int(match.group(2))
        item = match.group(4)
        return op, tid, item
    return None


def simulate_schedule(schedule_file):
    transaction_table = {}
    lock_table = {}
    global_timestamp = 1

    with open(schedule_file, "r") as file:
        for line in file:
            line = line.strip()
            op, tid, *rest = parse_operation(line)

            if op == "b":
                begin_transaction(tid, transaction_table, global_timestamp)
            elif op == "r":
                item = rest[0]
                read_item(tid, item, transaction_table, lock_table, global_timestamp)
            elif op == "w":
                item = rest[0]
                write_item(tid, item, transaction_table, lock_table, global_timestamp)
            elif op == "e":
                end_transaction(tid, transaction_table, lock_table)

            global_timestamp += 1


if __name__ == "__main__":
    schedule_file = "input.txt"  # Replace with the path to your input file
    simulate_schedule(schedule_file)
