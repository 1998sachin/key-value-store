import unittest
from l1_kv import KeyValueStore
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

    def test_deterministic_operations(self):
        # Define a list of deterministic operations
        operations = [
            ('set', 'name', 'Alice'),
            ('set', 'age', '25'),
            ('set', 'city', 'New York'),
            ('update', 'age', '26'),
            ('set', 'job', 'Engineer'),
            ('update', 'city', 'San Francisco'),
            ('set', 'hobby', 'Reading'),
            ('update', 'name', 'Bob'),
            ('update', 'job', 'Developer'),
            ('update', 'hobby', 'Hiking'),
        ]

        # Perform the operations
        for op, key, value in operations:
            if op == 'set':
                self.kv_store.set(key, value)
            elif op == 'update':
                self.kv_store.update(key, value)

        # Simulate abrupt termination here
        # Re-initialize the store
        self.kv_store = KeyValueStore(self.file_name, self.index_file_name)

        # Define the expected values after the operations
        expected_values = {
            'name': 'Bob',
            'age': '26',
            'city': 'San Francisco',
            'job': 'Developer',
            'hobby': 'Hiking'
        }

        # Get values and test
        for key, expected_value in expected_values.items():
            value = self.kv_store.get(key)
            print(f"{key}: {value}")
            self.assertEqual(value, expected_value)

    # def tearDown(self):
    #     # Clean up the data files after the test
    #     if os.path.exists(self.data_file):
    #         os.remove(self.data_file)
    #     if os.path.exists(self.index_file):
    #         os.remove(self.index_file)


if __name__ == '__main__':
    unittest.main()
