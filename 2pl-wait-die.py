import re
from collections import deque

# Initialize data structures to manage transactions and locks.

# transaction_table = {1:{“transaction_state”: "active", “locked_items”:["X"], “timestamp” : 2}
transaction_table = {}

# lock_table = {“Y”:{“lock_state”: “read”, “holding_transaction”: 1 , “waiting_transactions”:deque[2,3]},
lock_table = {}

aborted_set = set()

# waiting_transactions : { 1:{“read”, “write”, “end”}}
waiting_transactions = {}


# Initializes a new transaction with a given ID and timestamp.
# Sets the transaction's state to "active."
# Prints a message indicating the transaction has begun.
def begin_transaction(transaction_id, transaction_table, global_timestamp):
    if transaction_id not in transaction_table:
        transaction = {
            "transaction_state": "active",
            "locked_items": set(),
            "timestamp": global_timestamp,
        }
        transaction_table[transaction_id] = transaction
        print(f"Transaction {transaction_id} begins at TS({global_timestamp}).")


# Handles a read operation for a specific transaction and item.
# Checks the transaction's state and the item's lock status to determine the action.
# May lead to waiting, aborting, or performing the read operation
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
        waiting_transactions[transaction_id].append("r")
        print(f"Transaction {transaction_id} is waiting and cannot read {item_id}.")
        return

    if item_id in lock_table:
        current_lock = lock_table[item_id]
        if current_lock["holding_transaction"] != transaction_id:
            holding_transaction = transaction_table[current_lock["holding_transaction"]]
            # Check Wait-die for read
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
                # wait for the younger transaction
                current_transaction["transaction_state"] = "waiting"
                current_lock["waiting_transactions"].append(transaction_id)
                print(
                    f"Transaction {transaction_id} waits for Transaction {current_lock['holding_transaction']} to release {item_id}."
                )
                waiting_transactions[transaction_id] = deque(["r"])
                # waiting_transactions[transaction_id] = "waiting"
                return
        # read transaction
        else:
            print(f"Transaction {transaction_id} reads {item_id}.")
    else:
        # Create a new  read lock
        lock_table[item_id] = {
            "lock_state": "read",
            "holding_transaction": transaction_id,
            "waiting_transactions": deque(),
        }
        current_transaction["locked_items"].add(item_id)
        print(f"Transaction {transaction_id} reads {item_id}.")


# Handles a write operation for a specific transaction and item.
# Checks the transaction's state and the item's lock status to determine the action.
# May lead to waiting, aborting, or performing the write operation.
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
        waiting_transactions[transaction_id].append("w")

        print(f"Transaction {transaction_id} is waiting and cannot write {item_id}.")
        return
    if item_id in lock_table:
        current_lock = lock_table[item_id]
        holding_transaction = transaction_table[current_lock["holding_transaction"]]

        # Check Wait-die for write
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


# Ends a transaction, releasing all its locks.
# Handles potential waiting transactions and wakes them up.
def end_transaction(
    transaction_id, transaction_table, lock_table, waiting_transaction_running
):
    current_transaction = transaction_table[transaction_id]
    if transaction_id in waiting_transactions and not waiting_transaction_running:
        waiting_transactions[transaction_id].append("e")

        return

    if (
        current_transaction["transaction_state"] == "aborted"
        or current_transaction["transaction_state"] == "waiting"
    ):
        return

    for item_id in current_transaction["locked_items"]:
        unlock_item(item_id, lock_table)

    current_transaction["transaction_state"] = "committed"
    print(f"Transaction {transaction_id} commits.")

    if len(waiting_transactions) > 0:
        for tid in waiting_transactions:
            ops = waiting_transactions[tid]
            waiting_transaction_running = True
            while ops:
                sim_ops(ops.popleft(), tid, item_id, waiting_transaction_running)
    waiting_transaction_running = False
    # waiting_transactions.pop(tid)

    return


# Unlocks a data item and removes it from the lock table.
# If there are waiting transactions, it assigns the lock to the next waiting transaction.
def unlock_item(item_id, lock_table):
    if item_id in lock_table and len(lock_table[item_id]["waiting_transactions"]) == 0:
        del lock_table[item_id]

    elif item_id in lock_table and len(lock_table[item_id]["waiting_transactions"]) > 0:
        lock_item = lock_table[item_id]
        waiting_item = lock_item["waiting_transactions"].popleft()
        lock_table[item_id]["holding_transaction"] = waiting_item
        if waiting_item in transaction_table:
            transaction_table[waiting_item]["transaction_state"] = "active"


# Parses a line from the input file to extract the operation type, transaction ID, and item ID.
def parse_operation(line):
    match = re.match(r"([brwe])(\d+)\s*(\((\w+)\))?;", line)
    if match:
        op = match.group(1)
        tid = int(match.group(2))
        item = match.group(4)
        return op, tid, item
    return None


# Prints the current state of the lock table.
def print_lock_table(lock_table):
    print("Lock table:")
    for item_id in lock_table:
        print(f"{item_id}: {lock_table[item_id]}")


# Prints the current state of the transaction table.


def print_transaction_table(transaction_table):
    print("Transaction table:")
    # print(transaction_table)
    for item_id in transaction_table:
        print(f"{item_id}: {transaction_table[item_id]}")


# Simulates the transaction schedule based on an input file.
# Manages the global timestamp and waiting transactions.
def simulate_schedule(schedule_file):
    global_timestamp = 0
    waiting_transaction_running = False

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
                sim_ops(op, tid, rest[0], waiting_transaction_running)


# Executes a specific operation (read, write, end) for a transaction.
def sim_ops(op, tid, item_id, waiting_transaction_running):
    # Uncomment/comment the below code to check transaction table and lock table
    if tid not in aborted_set:
        print_lock_table(lock_table)
        print_transaction_table(transaction_table)
        # print(waiting_transactions)

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
        end_transaction(tid, transaction_table, lock_table, waiting_transaction_running)


if __name__ == "__main__":
    schedule_file = "input.txt"  # Replace with the path to the input file [input1,input2,input3,input4,input5.txt]
    simulate_schedule(schedule_file)
