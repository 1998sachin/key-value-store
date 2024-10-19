import unittest
from l2_kv import KeyValueStore
import os

class TestKeyValueStore(unittest.TestCase):
    def setUp(self):
        # File names for testing
        self.file_name = 'test_kv_store.txt'
        self.index_file_name = 'test_kv_index.txt'
        # Ensure a clean environment for each test
        self.clean_files()
        # Initialize the store
        self.kv_store = KeyValueStore(self.file_name, self.index_file_name)

    # def tearDown(self):
    #     # Clean up files after each test
    #     self.clean_files()

    def clean_files(self):
        """Remove test files if they exist."""
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
        if os.path.exists(self.index_file_name):
            os.remove(self.index_file_name)

    # def test_set_and_get(self):
    #     # Test setting and getting a value
    #     self.kv_store.set('name', 'Alice')
    #     self.assertEqual(self.kv_store.get('name'), 'Alice')

    # def test_update(self):
    #     # Test updating a value
    #     self.kv_store.set('name', 'Alice')
    #     self.kv_store.update('name', 'Bob')
    #     self.assertEqual(self.kv_store.get('name'), 'Bob')

    # def test_get_nonexistent_key(self):
    #     # Test getting a key that doesn't exist
    #     self.assertIsNone(self.kv_store.get('nonexistent'))

    # def test_persistence_after_restart(self):
    #     # Test data persistence after reinitialization
    #     self.kv_store.set('city', 'New York')
    #     # Simulate abrupt termination by reinitializing without cleanup
    #     self.kv_store = KeyValueStore(self.file_name, self.index_file_name)
    #     self.assertEqual(self.kv_store.get('city'), 'New York')

    # def test_multiple_keys(self):
    #     # Test setting multiple keys
    #     self.kv_store.set('name', 'Alice')
    #     self.kv_store.set('age', '30')
    #     self.kv_store.set('city', 'London')
    #     self.assertEqual(self.kv_store.get('name'), 'Alice')
    #     self.assertEqual(self.kv_store.get('age'), '30')
    #     self.assertEqual(self.kv_store.get('city'), 'London')

    # def test_overwrite_key(self):
    #     # Test overwriting an existing key
    #     self.kv_store.set('job', 'Engineer')
    #     self.kv_store.set('job', 'Teacher')
    #     self.assertEqual(self.kv_store.get('job'), 'Teacher')

    # def test_index_integrity(self):
    #     # Test that index matches the actual data positions
    #     self.kv_store.set('hobby', 'Reading')
    #     expected_offset = self.kv_store.index['hobby']
    #     with open(self.file_name, 'r') as f:
    #         f.seek(expected_offset)
    #         line = f.readline().strip()
    #         self.assertEqual(line, 'hobby Reading')

    # def test_display_index(self):
    #     # Test the display_index method
    #     self.kv_store.set('name', 'Alice')
    #     self.assertIn('name', self.kv_store.display_index())

    # def test_simulated_abrupt_termination(self):
    #     # Simulate abrupt termination and check data integrity
    #     self.kv_store.set('name', 'Alice')
    #     self.kv_store.set('age', '25')
    #     # Simulate abrupt termination by not calling any cleanup methods
    #     self.kv_store = KeyValueStore(self.file_name, self.index_file_name)
    #     self.assertEqual(self.kv_store.get('name'), 'Alice')
    #     self.assertEqual(self.kv_store.get('age'), '25')

    # def test_deterministic_operations(self):
    #     # Define a list of deterministic operations
    #     operations = [
    #         ('set', 'name', 'Alice'),
    #         ('set', 'age', '25'),
    #         ('set', 'city', 'New York'),
    #         ('update', 'age', '26'),
    #         ('set', 'job', 'Engineer'),
    #         ('update', 'city', 'San Francisco'),
    #         ('set', 'hobby', 'Reading'),
    #         ('update', 'name', 'Bob'),
    #         ('update', 'job', 'Developer'),
    #         ('update', 'hobby', 'Hiking'),
    #     ]

    #     # Perform the operations
    #     for op, key, value in operations:
    #         if op == 'set':
    #             self.kv_store.set(key, value)
    #         elif op == 'update':
    #             self.kv_store.update(key, value)

    #     # Simulate abrupt termination here
    #     # Re-initialize the store
    #     self.kv_store = KeyValueStore(self.file_name, self.index_file_name)

    #     # Define the expected values after the operations
    #     expected_values = {
    #         'name': 'Bob',
    #         'age': '26',
    #         'city': 'San Francisco',
    #         'job': 'Developer',
    #         'hobby': 'Hiking'
    #     }

    #     # Get values and test
    #     for key, expected_value in expected_values.items():
    #         value = self.kv_store.get(key)
    #         print(f"{key}: {value}")
    #         self.assertEqual(value, expected_value)

    # def tearDown(self):
    #     # Clean up the data files after the test
    #     if os.path.exists(self.data_file):
    #         os.remove(self.data_file)
    #     if os.path.exists(self.index_file):
    #         os.remove(self.index_file)

    def test_compaction(self):
        # Test the compaction functionality
        # Perform multiple set and update operations
        self.kv_store.set('name', 'Alice')
        self.kv_store.set('age', '25')
        self.kv_store.update('age', '26')
        self.kv_store.set('city', 'New York')
        self.kv_store.update('city', 'San Francisco')
        self.kv_store.set('job', 'Engineer')
        self.kv_store.update('job', 'Developer')

        # Get file sizes before compaction
        data_file_size_before = os.path.getsize(self.file_name)
        index_file_size_before = os.path.getsize(self.index_file_name)

        # Perform compaction
        self.kv_store.compact()

        # Get file sizes after compaction
        data_file_size_after = os.path.getsize(self.file_name)
        index_file_size_after = os.path.getsize(self.index_file_name)
        print(f"Data file size before: {data_file_size_before}")
        print(f"Index file size before: {index_file_size_before}")
        print(f"Data file size after: {data_file_size_after}")
        print(f"Index file size after: {index_file_size_after}")

        # Ensure that the file sizes have decreased
        self.assertLess(data_file_size_after, data_file_size_before)
        self.assertLess(index_file_size_after, index_file_size_before)

        # Ensure that data integrity is maintained after compaction
        self.assertEqual(self.kv_store.get('name'), 'Alice')
        self.assertEqual(self.kv_store.get('age'), '26')
        self.assertEqual(self.kv_store.get('city'), 'San Francisco')
        self.assertEqual(self.kv_store.get('job'), 'Developer')

        # Check that the data file contains only the latest entries
        with open(self.file_name, 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 4)  # Should contain only 4 entries

        # Check that the index file contains the correct offsets
        with open(self.index_file_name, 'r') as f:
            index_lines = f.readlines()
            self.assertEqual(len(index_lines), 4)  # Should contain only 4 entries

        # Verify that offsets in the index are correct
        offset = 0
        for line in lines:
            key_value = line.strip()
            key, value = key_value.split(' ', 1)
            self.assertEqual(self.kv_store.index[key], offset)
            offset += len(line)

    def test_compaction_with_no_updates(self):
        # Test compaction when there are no updates
        self.kv_store.set('key1', 'value1')
        self.kv_store.set('key2', 'value2')

        # Get file sizes before compaction
        data_file_size_before = os.path.getsize(self.file_name)
        index_file_size_before = os.path.getsize(self.index_file_name)

        # Perform compaction
        self.kv_store.compact()

        # Get file sizes after compaction
        data_file_size_after = os.path.getsize(self.file_name)
        index_file_size_after = os.path.getsize(self.index_file_name)

        # File sizes should remain the same
        self.assertEqual(data_file_size_after, data_file_size_before)
        self.assertEqual(index_file_size_after, index_file_size_before)

        # Data should remain intact
        self.assertEqual(self.kv_store.get('key1'), 'value1')
        self.assertEqual(self.kv_store.get('key2'), 'value2')


if __name__ == '__main__':
    unittest.main()
