import re
from collections import deque

# waiting_transactions={3:[r,w,e]}
transaction_table = {}
lock_table = {}
aborted_set = set()


def begin_transaction(transaction_id, transaction_table, global_timestamp):
    if transaction_id not in transaction_table:
        transaction = {
            "transaction_state": "active",
            "locked_items": set(),
            "timestamp": global_timestamp,
        }
        transaction_table[transaction_id] = transaction
        print(f"Transaction {transaction_id} begins at TS({global_timestamp}).")


def read_item(
    transaction_id,
    item_id,
    transaction_table,
    lock_table,
    aborted_set,
):
    current_transaction = transaction_table[transaction_id]
    # print(transaction_id, current_transaction["transaction_state"])

    if current_transaction["transaction_state"] == "waiting":
        print(f"Transaction {transaction_id} is waiting and cannot read {item_id}.")
        return

    if item_id in lock_table:
        current_lock = lock_table[item_id]
        if current_lock["holding_transaction"] != transaction_id:
            holding_transaction = transaction_table[current_lock["holding_transaction"]]
            if current_transaction["timestamp"] > holding_transaction["timestamp"]:
                # Abort the younger transaction.
                current_transaction["transaction_state"] = "aborted"
                print(
                    f"Transaction {transaction_id} is aborted as it cannot read {item_id} held by {current_lock['holding_transaction']}"
                )
                for item_id in current_transaction["locked_items"]:
                    unlock_item(item_id, lock_table)
                if len(current_lock["waiting_transactions"]) != 0:
                    for waiting_transaction_id in current_lock["waiting_transactions"]:
                        waiting_transaction = transaction_table[waiting_transaction_id]
                        waiting_transaction["transaction_state"] = "active"
                aborted_set.add(transaction_id)
                return
            else:
                current_transaction["transaction_state"] = "waiting"
                current_lock["waiting_transactions"].append(transaction_id)
                print(
                    f"Transaction {transaction_id} waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                )
                # waiting_transactions[transaction_id] = "waiting"
                return
        else:
            print(f"Transaction {transaction_id} reads {item_id}.")
    else:
        lock_table[item_id] = {
            "lock_state": "read",
            "holding_transaction": transaction_id,
            "waiting_transactions": deque(),
        }
        current_transaction["locked_items"].add(item_id)
        print(f"Transaction {transaction_id} reads {item_id}.")


def write_item(
    transaction_id,
    item_id,
    transaction_table,
    lock_table,
    aborted_set,
):
    current_transaction = transaction_table[transaction_id]
    # print(transaction_id, current_transaction["transaction_state"])
    if current_transaction["transaction_state"] == "waiting":
        print(f"Transaction {transaction_id} is waiting and cannot write {item_id}.")
        return
    if item_id in lock_table:
        current_lock = lock_table[item_id]
        holding_transaction = transaction_table[current_lock["holding_transaction"]]
        if (
            current_lock["holding_transaction"] != transaction_id
            and current_transaction["timestamp"] > holding_transaction["timestamp"]
        ):
            # Abort the younger transaction.
            current_transaction["transaction_state"] = "aborted"
            print(
                f"Transaction {transaction_id} is aborted as it cannot write {item_id} held by {current_lock['holding_transaction']}"
            )
            for item_id in current_transaction["locked_items"]:
                unlock_item(item_id, lock_table)

            # Wake up any transactions that were waiting for the aborted transaction to release locks.
            for waiting_transaction_id in current_lock["waiting_transactions"]:
                waiting_transaction = transaction_table[waiting_transaction_id]
                waiting_transaction["transaction_state"] = "active"
            aborted_set.add(transaction_id)
            return
        else:
            current_lock["lock_state"] = "write"
            print(f"Transaction {transaction_id} writes {item_id}.")
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
    if (
        current_transaction["transaction_state"] == "aborted"
        or current_transaction["transaction_state"] == "waiting"
    ):
        return
    for item_id in current_transaction["locked_items"]:
        unlock_item(item_id, lock_table)
    print(f"Transaction {transaction_id} commits.")
    # print_lock_table(lock_table)


def unlock_item(item_id, lock_table):
    if item_id in lock_table:
        del lock_table[item_id]


def parse_operation(line):
    match = re.match(r"([brwe])(\d+)\s*(\((\w+)\))?;", line)
    if match:
        op = match.group(1)
        tid = int(match.group(2))
        item = match.group(4)
        return op, tid, item
    return None


def print_lock_table(lock_table):
    print("Lock table:")
    for item_id in lock_table:
        print(f"{item_id}: {lock_table[item_id]}")


def simulate_schedule(schedule_file):
    global_timestamp = 0

    with open(schedule_file, "r") as file:
        for line in file:
            # print(lock_table)
            line = line.strip()
            op, tid, *rest = parse_operation(line)
            global_timestamp += 1

            if tid in aborted_set:
                continue
            elif op == "b":
                begin_transaction(tid, transaction_table, global_timestamp)
            else:
                sim_ops(op, tid, rest[0])


def sim_ops(op, tid, item_id):
    if op == "r":
        item = item_id
        read_item(
            tid,
            item,
            transaction_table,
            lock_table,
            aborted_set,
        )
    elif op == "w":
        item = item_id
        write_item(
            tid,
            item,
            transaction_table,
            lock_table,
            aborted_set,
        )
    elif op == "e":
        end_transaction(tid, transaction_table, lock_table)


if __name__ == "__main__":
    schedule_file = "input5.txt"  # Replace with the path to your input file
    simulate_schedule(schedule_file)
