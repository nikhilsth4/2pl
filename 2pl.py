import re


def begin_transaction(transaction_id, transaction_table, global_timestamp):
    if transaction_id not in transaction_table:
        transaction = {
            "transaction_state": "active",
            "locked_items": [],
            "timestamp": global_timestamp,
        }
        transaction_table[transaction_id] = transaction
        print(
            f"Transaction T{transaction_id} is created and starts at TS({global_timestamp})."
        )


def read_item(transaction_id, item_id, transaction_table, lock_table, global_timestamp):
    current_transaction = transaction_table[transaction_id]
    if item_id in lock_table:
        current_lock = lock_table[item_id]
        if current_lock["lock_state"] == "read":
            if current_lock["holding_transaction"] == transaction_id:
                print(f"T{transaction_id} reads {item_id}.")
            else:
                if (
                    current_transaction["timestamp"]
                    < current_lock["holding_transaction"]["timestamp"]
                ):
                    current_transaction["transaction_state"] = "waiting"
                    current_lock["waiting_transactions"].append(transaction_id)
                    print(
                        f"T{transaction_id} is younger and waits for {current_lock['holding_transaction']} to release {item_id}."
                    )
                else:
                    current_transaction["transaction_state"] = "aborted"
                    print(
                        f"T{transaction_id} is older and aborted due to conflict with T{current_lock['holding_transaction']} on {item_id}."
                    )
        else:
            if (
                current_transaction["timestamp"]
                < current_lock["holding_transaction"]["timestamp"]
            ):
                current_transaction["transaction_state"] = "waiting"
                current_lock["waiting_transactions"].append(transaction_id)
                print(
                    f"T{transaction_id} is younger and waits for {current_lock['holding_transaction']} to release {item_id}."
                )
            else:
                current_transaction["transaction_state"] = "aborted"
                print(
                    f"T{transaction_id} is older and aborted due to conflict with T{current_lock['holding_transaction']} on {item_id}."
                )
    else:
        lock_table[item_id] = {
            "lock_state": "read",
            "holding_transaction": transaction_id,
            "waiting_transactions": [],
        }
        current_transaction["locked_items"].append(item_id)
        print(f"T{transaction_id} reads {item_id} (read-locks it).")


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
            print(op, tid, rest)

            if op == "b":
                begin_transaction(tid, transaction_table, global_timestamp)
                global_timestamp += 1
            elif op == "r":
                item = rest[0]
                print(tid, item, transaction_table, lock_table, global_timestamp)
                read_item(tid, item, transaction_table, lock_table, global_timestamp)
                global_timestamp += 1
            # elif op == 'w':
            #     item = rest[0]
            #     write_item(tid, item, transaction_table, lock_table, global_timestamp)
            #     global_timestamp += 1
            # elif op == 'e':
            #     end_transaction(tid, transaction_table, lock_table)


if __name__ == "__main__":
    schedule_file = "input.txt"  # Replace with the path to your input file
    simulate_schedule(schedule_file)
