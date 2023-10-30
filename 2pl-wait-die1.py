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


def read_item(
    transaction_id,
    item_id,
    transaction_table,
    lock_table,
    aborted_set,
    waiting_transactions,
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
                        waiting_transactions[waiting_transaction_id] = "active"
                aborted_set.add(transaction_id)
                return
            else:
                current_transaction["transaction_state"] = "waiting"
                current_lock["waiting_transactions"].append(transaction_id)
                print(
                    f"Transaction {transaction_id} waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                )
                waiting_transactions[transaction_id] = "waiting"
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
    waiting_transactions,
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
            if len(current_lock["waiting_transactions"]) != 0:
                for waiting_transaction_id in current_lock["waiting_transactions"]:
                    waiting_transactions[waiting_transaction_id] = "active"
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


def end_transaction(
    transaction_id, transaction_table, lock_table, waiting_transactions
):
    current_transaction = transaction_table[transaction_id]
    if current_transaction["transaction_state"] == "aborted":
        return
    if current_transaction["transaction_state"] == "waiting":
        return
    for item_id in current_transaction["locked_items"]:
        unlock_item(item_id, lock_table)
    print(f"Transaction {transaction_id} commits.")
    del transaction_table[transaction_id]
    if transaction_id in waiting_transactions:
        del waiting_transactions[transaction_id]


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
    transaction_table = {}
    lock_table = {}
    global_timestamp = 0
    aborted_set = set()
    waiting_transactions = {}

    # List to track waiting transactions that need to be reprocessed
    waiting_list = []

    with open(schedule_file, "r") as file:
        for line in file:
            line = line.strip()
            op, tid, *rest = parse_operation(line)
            global_timestamp += 1

            if tid in aborted_set:
                continue
            if tid in waiting_transactions and waiting_transactions[tid] == "waiting":
                waiting_transactions[tid] = "active"

            if op == "b":
                begin_transaction(tid, transaction_table, global_timestamp)
            elif op == "r":
                item = rest[0]
                read_item(
                    tid,
                    item,
                    transaction_table,
                    lock_table,
                    aborted_set,
                    waiting_transactions,
                )
            elif op == "w":
                item = rest[0]
                write_item(
                    tid,
                    item,
                    transaction_table,
                    lock_table,
                    aborted_set,
                    waiting_transactions,
                )
            elif op == "e":
                end_transaction(
                    tid, transaction_table, lock_table, waiting_transactions
                )
                if (
                    tid in waiting_transactions
                    and waiting_transactions[tid] == "waiting"
                ):
                    waiting_list.append(tid)

            # Process waiting transactions in the order they were added to the list
            for waiting_tid in waiting_list:
                while waiting_tid in waiting_list:
                    if (
                        waiting_tid in waiting_transactions
                        and waiting_transactions[waiting_tid] == "active"
                    ):
                        # Process the now active waiting transaction
                        op, tid, *rest = parse_operation(line)
                        if op == "r":
                            item = rest[0]
                            read_item(
                                tid,
                                item,
                                transaction_table,
                                lock_table,
                                aborted_set,
                                waiting_transactions,
                            )
                        elif op == "w":
                            item = rest[0]
                            write_item(
                                tid,
                                item,
                                transaction_table,
                                lock_table,
                                aborted_set,
                                waiting_transactions,
                            )
                        waiting_list.remove(waiting_tid)


if __name__ == "__main__":
    schedule_file = "input4.txt"  # Replace with the path to your input file
    simulate_schedule(schedule_file)
