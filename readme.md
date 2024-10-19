# Key-Value Store Implementation

This project implements a simple key-value store with different levels of functionality and concurrency support.

## Overview

The key-value store is implemented in three levels:

1. Basic Key-Value Store (L1)
2. Key-Value Store with Compaction (L2)
3. Thread-Safe Key-Value Store with Compaction (L3)

Each level builds upon the previous one, adding more features and robustness.

## Implementation Details

### L1: Basic Key-Value Store

The basic implementation provides fundamental key-value operations:

- Set: Store a key-value pair
- Get: Retrieve a value by key
- Update: Modify an existing key's value

Key features:
- Persistent storage using a file-based system
- Separate index file for quick lookups
- Basic error handling and data integrity checks

File: `l1_kv.py`

### L2: Key-Value Store with Compaction

This level introduces a compaction feature to optimize storage:

- All features from L1
- Compaction: Remove obsolete entries and reduce file size

Key improvements:
- Efficient storage management
- Reduced file sizes after multiple updates

File: `l2_kv.py`

### L3: Thread-Safe Key-Value Store with Compaction

The final level adds thread-safety for concurrent operations:

- All features from L2
- Thread-safe operations using locks
- Concurrent read/write support
- Background compaction

Key improvements:
- Safe for multi-threaded environments
- Improved performance for concurrent access

File: `l3_kv.py`

## Usage

To use the key-value store, create an instance of the desired implementation:

```python
from l3_kv import KeyValueStore

store = KeyValueStore('data.txt', 'index.txt')

# Set a value
store.set('key', 'value')

# Get a value
value = store.get('key')

# Update a value
store.update('key', 'new_value')

# Compact the store (L2 and L3 only)
store.compact()
```

## Testing

Each implementation level has its own test file:

- `test_l1_kv.py`
- `test_l2_kv.py`
- `test_l3_kv.py`

These tests cover various scenarios, including:
- Basic set/get operations
- Data persistence
- Compaction
- Concurrent access (L3 only)

To run the tests, use the Python unittest framework:

```
python -m unittest test_l1_kv.py
python -m unittest test_l2_kv.py
python -m unittest test_l3_kv.py
```

## Performance Considerations

- The L1 implementation is suitable for basic, single-threaded use cases.
- L2 adds compaction for better long-term storage efficiency.
- L3 is optimized for concurrent access in multi-threaded environments.

Choose the appropriate implementation based on your specific requirements for concurrency and storage optimization.


## Contributing

Contributions are welcome! Please submit pull requests with any improvements or bug fixes.

## License

This project is open-source and available under the MIT License.