# Two-Phase Locking with Wait-Die Protocol Simulator

This project implements a simplified Two-Phase Locking (2PL) protocol for managing concurrent transactions in a database system using Python. It simulates transaction operations—begin, read, write, and end—and employs the **wait-die** scheme to resolve conflicts and prevent deadlocks.

## Features

- **Transaction Management:**  
  Tracks transaction states such as active, waiting, aborted, and committed.

- **Lock Management:**  
  Maintains a lock table for data items with support for both read and write locks.  
  Each lock entry tracks the holding transaction and a queue of waiting transactions.

- **Wait-Die Protocol:**  
  Uses transaction timestamps to decide whether a transaction must wait or be aborted when a conflict occurs.

- **Schedule Simulation:**  
  Processes a schedule file containing a series of operations and simulates transaction execution.

- **Debugging Tools:**  
  Provides functions to print the current state of the lock table and transaction table for better traceability.

## Project Structure

- **`transaction_table`**  
  A dictionary that holds information for each transaction, including its state, a set of locked items, and a timestamp.

- **`lock_table`**  
  A dictionary that tracks the lock status for each data item. Each entry contains:
  - `lock_state` (read/write)
  - `holding_transaction`
  - `waiting_transactions` (a queue of transactions waiting for the lock)

- **`waiting_transactions`**  
  A dictionary that queues operations (read, write, end) for transactions that are in a waiting state.

- **`aborted_set`**  
  A set containing the IDs of transactions that have been aborted.

### Key Functions

- **`begin_transaction(transaction_id, transaction_table, global_timestamp)`**  
  Initializes a new transaction with an active state and assigns a timestamp.

- **`read_item(transaction_id, item_id, transaction_table, lock_table, aborted_set)`**  
  Handles read requests by checking lock status and applying the wait-die protocol to resolve conflicts.

- **`write_item(transaction_id, item_id, transaction_table, lock_table, aborted_set)`**  
  Manages write operations, updating lock states as needed and enforcing the wait-die scheme.

- **`end_transaction(transaction_id, transaction_table, lock_table, waiting_transaction_running)`**  
  Commits a transaction, releases all held locks, and reactivates any waiting transactions.

- **`unlock_item(item_id, lock_table)`**  
  Releases a lock on a data item and, if applicable, assigns the lock to the next waiting transaction.

- **`parse_operation(line)`**  
  Parses a line from the input schedule to extract the operation type, transaction ID, and data item.

- **`simulate_schedule(schedule_file)`**  
  Reads an input schedule file and sequentially simulates transaction operations.

## Getting Started

### Prerequisites

- Python 3.x

### Installation

Clone the repository:

```bash
git clone https://github.com/yourusername/2pl-simulator.git
cd 2pl-simulator
```

## Prerequisites

No additional libraries are required beyond Python's standard modules.

## Usage

### Prepare the Input File

Create an input file (e.g., `input.txt`) with operations formatted as follows:

```bash
b1;
r1(X);
w1(Y);
e1;
b2;
r2(Y);
e2;
```
## Input Format Commands

- **`b`** - Begin: Start a new transaction  
  Example: `b1;` starts Transaction 1  
- **`r`** - Read: Transaction reads a data item  
  Example: `r1(X);` reads item X for Transaction 1  
- **`w`** - Write: Transaction writes to a data item  
  Example: `w1(X);` writes to item X for Transaction 1  
- **`e`** - End: Commit and terminate a transaction  
  Example: `e1;` commits and ends Transaction 1  

## Run the Simulator

Execute the Python script with your input file:
```bash
python main.py input.txt
```
## Example
Given an input.txt file with the following operations:
```bash
b1;
r1(X);
w1(Y);
b2;
r2(Y);
e1;
e2;
```

## Running the Simulator

Running the simulator will:

- **Begin transactions 1 and 2.**
- **Process read and write operations while enforcing lock management.**
- **Resolve conflicts using the wait-die protocol** (aborting or making transactions wait as needed).
- **Commit transactions and release locks accordingly.**
- Throughout the simulation, the program prints the current state of the lock and transaction tables, providing a detailed trace of the operations.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements, bug fixes, or additional features.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgments

- Inspired by foundational database concurrency control concepts.
- Thanks to the open-source community for continuous improvements in transaction management techniques.
