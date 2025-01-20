
# Distributed Systems Paper Implementations

This repository contains implementations of two seminal distributed systems papers:

1. [**MapReduce: Simplified Data Processing on Large Clusters**](http://nil.csail.mit.edu/6.824/2020/papers/mapreduce.pdf)
2. [**Raft: In Search of an Understandable Consensus Algorithm**](http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf)

Both projects were developed as part of the 6.5840 (Distributed Systems) course in Spring 2024 at MIT.

---

## Repository Structure

```plaintext
/
├── Makefile
├── .check-build
├── src/
│   ├── labgob/       # Go encoder/decoder utilities
│   ├── labrpc/       # Go RPC framework for distributed communication
│   ├── main/         # Main programs for MapReduce coordination and testing
│   ├── models/       # Model files for the Key/Value store in Raft
│   ├── mr/           # MapReduce coordinator, worker, and RPC implementation
│   ├── mrapps/       # Applications (e.g., WordCount, Indexer) for MapReduce
│   ├── porcupine/    # Linearizability checker for testing Raft consistency
│   ├── raft/         # Raft implementation and tests
```

---

## MapReduce Implementation

### Description

The MapReduce implementation is based on the original Google paper, focusing on parallel processing with fault-tolerance using a coordinator-worker architecture. The project includes:

- A **coordinator** to distribute Map and Reduce tasks.
- **Workers** that execute tasks, handle intermediate files, and retry failed tasks.
- Applications such as **WordCount** and **Indexer** for testing.

### Features

- **Fault Tolerance:** Automatic re-assignment of tasks from failed workers.
- **Parallelism:** Multiple workers executing Map and Reduce tasks concurrently.
- **Tests:** Validation scripts to ensure correctness and performance.

### How to Run

1. Build the WordCount plugin:
   ```bash
   cd src/main
   go build -buildmode=plugin ../mrapps/wc.go
   ```

2. Start the coordinator:
   ```bash
   go run mrcoordinator.go pg-*.txt
   ```

3. Start one or more workers:
   ```bash
   go run mrworker.go wc.so
   ```

4. Validate results:
   ```bash
   cat mr-out-* | sort
   ```

For a complete test, run:
```bash
bash src/main/test-mr.sh
```

---

## Raft Implementation

### Description

The Raft implementation follows the extended Raft paper, focusing on leader election, log replication, and fault-tolerance. It is the foundation for building a distributed key-value store.

### Features

- **Leader Election:** Election of a leader within 5 seconds of a failure.
- **Log Replication:** Consistent replication of logs across all nodes.
- **Fault Tolerance:** Persistence of state to recover from crashes.
- **Snapshotting:** Reduces memory usage by discarding old logs.

### How to Run

1. Run tests for Part 3A (Leader Election):
   ```bash
   cd src/raft
   go test -run 3A
   ```

2. Run tests for Part 3B (Log Replication):
   ```bash
   go test -run 3B
   ```

3. Run tests for Part 3C (Persistence):
   ```bash
   go test -run 3C
   ```

4. Run tests for Part 3D (Snapshotting):
   ```bash
   go test -run 3D
   ```

### Example Output

A successful run of the 3A tests looks like this:
```plaintext
Test (3A): initial election ...
  ... Passed --   3.5  3   58   16840    0
Test (3A): election after network failure ...
  ... Passed --   5.4  3  118   25269    0
PASS
ok  	6.5840/raft	16.265s
```

---

## Notes

- These implementations were homework assignments for 6.5840 and adhere to the course’s collaboration policy.
- The provided tests simulate real-world distributed environments, including network delays, crashes, and restarts.
  
