def simulate_schedule(input_file):
    transactions = {}
    locks = {}
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            operation, *args = line.split()
            transaction_id = int(args[0][1])
            if operation == 'b':
                begin_transaction(transaction_id, transactions)
            elif operation == 'r':
                item_id = args[0][1]
                read_item(transaction_id, item_id, transactions, locks)
            elif operation == 'w':
                item_id = args[0][1]
                write_item(transaction_id, item_id, transactions, locks)
            elif operation == 'e':
                end_transaction(transaction_id, transactions, locks)

def begin_transaction(transaction_id, transactions):
    transaction = {"transaction_state": "active", "locked_items": [], "timestamp": transaction_id}
    transactions[transaction_id] = transaction
    print(f"Begin transaction T{transaction_id}")

def read_item(transaction_id, item_id, transactions, locks):
    transaction = transactions[transaction_id]
    if item_id in locks:
        lock = locks[item_id]
        if lock["lock_state"] == "write":
            if lock["holding_transaction"] == transaction_id:
                lock["lock_state"] = "read"
                print(f"Read item {item_id} by T{transaction_id}")
            else:
                block_transaction(transaction_id, lock, transactions)
        elif lock["lock_state"] == "read":
            lock["holding_transaction"].append(transaction_id)
            print(f"Read item {item_id} by T{transaction_id}")
    else:
        lock = {"item_id": item_id, "lock_state": "read", "holding_transaction": [transaction_id], "waiting_transactions": []}
        locks[item_id] = lock
        print(f"Read item {item_id} by T{transaction_id}")

def write_item(transaction_id, item_id, transactions, locks):
    transaction = transactions[transaction_id]
    if item_id in locks:
        lock = locks[item_id]
        if lock["lock_state"] == "read":
            if len(lock["holding_transaction"]) == 1 and lock["holding_transaction"][0] == transaction_id:
                lock["lock_state"] = "write"
                print(f"Write item {item_id} by T{transaction_id}")
            else:
                block_transaction(transaction_id, lock, transactions)
        elif lock["lock_state"] == "write":
            if lock["holding_transaction"] == transaction_id:
                pass
            else:
                block_transaction(transaction_id, lock, transactions)
    else:
        lock = {"item_id": item_id, "lock_state": "write", "holding_transaction": transaction_id, "waiting_transactions": []}
        locks[item_id] = lock
        print(f"Write item {item_id} by T{transaction_id}")

def block_transaction(transaction_id, lock, transactions):
    waiting_transactions = lock["waiting_transactions"]
    if waiting_transactions:
        max_waiting_transaction = max(waiting_transactions)
        if transaction_id < max_waiting_transaction:
            abort_transaction(transaction_id, transactions)
        else:
            waiting_transactions.append(transaction_id)
    else:
        waiting_transactions.append(transaction_id)

def end_transaction(transaction_id, transactions, locks):
    transaction = transactions[transaction_id]
    if transaction["transaction_state"] == "blocked":
        pass
    elif transaction["transaction_state"] == "aborted":
        release_locks(transaction_id, locks)
        del transactions[transaction_id]
    else:
        release_locks(transaction_id, locks)
        del transactions[transaction_id]
        print(f"End transaction T{transaction_id}")

def release_locks(transaction_id, locks):
    for lock in locks.values():
        if lock["lock_state"] == "read" and transaction_id in lock["holding_transaction"]:
            lock["holding_transaction"].remove(transaction_id)
        elif lock["lock_state"] == "write" and lock["holding_transaction"] == transaction_id:
            lock["lock_state"] = None
            lock["holding_transaction"] = None
            lock["waiting_transactions"] = []

# Example usage
simulate_schedule("input.doc")
