import re
from collections import deque

transaction_table = {}
lock_table = {}
aborted_set = set()
waiting_transactions = {}


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
                    unlock_item(item_id, lock_table, transaction_table, transaction_id)
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
                unlock_item(item_id, lock_table, transaction_table, transaction_id)

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
    # print(current_transaction, transaction_id)
    for item_id in current_transaction["locked_items"]:
        unlock_item(item_id, lock_table, transaction_table, transaction_id)

    transaction_table[transaction_id]["transaction_state"] = "committed"

    print(f"Transaction {transaction_id} commits.")
    # print_lock_table(lock_table)
    # print(transaction_table)


def unlock_item(item_id, lock_table, transaction_table, transaction_id):
    if item_id in lock_table and len(lock_table[item_id]["waiting_transactions"]) == 0:
        del lock_table[item_id]

    elif item_id in lock_table and len(lock_table[item_id]["waiting_transactions"]) > 0:
        lock_item = lock_table[item_id]
        waiting_item = lock_item["waiting_transactions"].popleft()
        lock_table[item_id]["holding_transaction"] = waiting_item
        if waiting_item in transaction_table:
            transaction_table[waiting_item]["transaction_state"] = "active"

        waiting_transaction = transaction_table[waiting_item]
        for item in waiting_transaction["locked_items"]:
            if lock_table.get(item, {}).get("holding_transaction") == waiting_item:
                # This item was locked by the waiting transaction
                if lock_table[item]["lock_state"] == "read":
                    print(f"Transaction {waiting_item} reads {item}.")
                elif lock_table[item]["lock_state"] == "write":
                    print(f"Transaction {waiting_item} writes {item}.")


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
    # List to track waiting transactions that need to be reprocessed
    waiting_list = []
    global_timestamp = 0
    with open(schedule_file, "r") as file:
        for line in file:
            # print(lock_table)
            line = line.strip()
            op, tid, *rest = parse_operation(line)
            global_timestamp = global_timestamp + 1
            if tid in aborted_set:
                continue
            if op == "b":
                begin_transaction(tid, transaction_table, global_timestamp)
            else:
                simulate_op(op, tid, rest[0])


def simulate_op(op, tid, item_id):
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
    schedule_file = "input3.txt"  # Replace with the path to your input file
    simulate_schedule(schedule_file)
