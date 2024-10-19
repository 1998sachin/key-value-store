import unittest
import os
import threading
import time
from l3_kv import KeyValueStore

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

    # def test_concurrent_access_during_compaction(self):
    #     """Test concurrent read/write operations during compaction."""
    #     self.kv_store.set('key', 'initial')

    #     def update_values(store):
    #         for i in range(50):
    #             store.update('key', f'value{i}')
    #             time.sleep(0.01)

    #     def read_values(store, results):
    #         for _ in range(50):
    #             value = store.get('key')
    #             results.append(value)
    #             time.sleep(0.01)

    #     # Start threads to update and read values
    #     results = []
    #     updater_thread = threading.Thread(target=update_values, args=(self.kv_store,))
    #     reader_thread = threading.Thread(target=read_values, args=(self.kv_store, results))

    #     updater_thread.start()
    #     reader_thread.start()

    #     # Start compaction while updates and reads are happening
    #     self.kv_store.start_compaction()

    #     updater_thread.join()
    #     reader_thread.join()
    #     self.kv_store.wait_for_compaction()

    #     # Verify that the last value is as expected
    #     final_value = self.kv_store.get('key')
    #     self.assertEqual(final_value, 'value49')

    #     # Verify that all read values are among the expected ones
    #     expected_values = {f'value{i}' for i in range(50)}
    #     expected_values.add('initial')
    #     for value in results:
    #         self.assertIn(value, expected_values)

    def test_data_integrity_after_compaction(self):
        """Test data integrity after compaction and concurrent operations."""
        keys = [f'key{i}' for i in range(5)]
        # Set initial values
        for key in keys:
            self.kv_store.set(key, 'initial')

        # Start multiple threads to update keys
        def update_key(store, key):
            for i in range(30):
                store.update(key, f'{key}_value{i}')
                time.sleep(0.01)

        threads = []
        for key in keys:
            thread = threading.Thread(target=update_key, args=(self.kv_store, key))
            threads.append(thread)
            thread.start()

        # Get the initial file size
        initial_data_size = os.path.getsize(self.file_name)
        initial_index_size = os.path.getsize(self.index_file_name)

        # Start compaction
        # self.kv_store.start_compaction()

        # Wait for all threads and compaction to finish
        for thread in threads:
            thread.join()
        # self.kv_store.wait_for_compaction()

        # Get the final file size
        final_data_size = os.path.getsize(self.file_name)
        final_index_size = os.path.getsize(self.index_file_name)

        # Verify data integrity
        for key in keys:
            value = self.kv_store.get(key)
            self.assertEqual(value, f'{key}_value29')

        # Check if compaction has actually reduced file sizes
        self.assertLess(final_data_size, initial_data_size, "Data file size should be reduced after compaction")
        self.assertLess(final_index_size, initial_index_size, "Index file size should be reduced after compaction")

        # Verify that each key appears only once in the data file
        with open(self.file_name, 'r') as f:
            content = f.read()
            for key in keys:
                self.assertEqual(content.count(key), 1, f"Key {key} should appear only once in the data file")

    # def test_concurrent_set_get_operations(self):
    #     """Test concurrent set and get operations from multiple threads."""
    #     def set_values(store, key_prefix):
    #         for i in range(50):
    #             store.set(f'{key_prefix}_{i}', f'value_{i}')
    #             time.sleep(0.005)

    #     def get_values(store, key_prefix, results):
    #         for i in range(50):
    #             value = store.get(f'{key_prefix}_{i}')
    #             results.append((f'{key_prefix}_{i}', value))
    #             time.sleep(0.005)

    #     threads = []
    #     results = []
    #     for i in range(3):
    #         key_prefix = f'key{i}'
    #         t_set = threading.Thread(target=set_values, args=(self.kv_store, key_prefix))
    #         t_get = threading.Thread(target=get_values, args=(self.kv_store, key_prefix, results))
    #         threads.extend([t_set, t_get])
    #         t_set.start()
    #         t_get.start()

    #     for thread in threads:
    #         thread.join()

    #     # Verify that all keys have the correct values
    #     for key, value in results:
    #         if value is not None:
    #             index = key.split('_')[-1]
    #             self.assertEqual(value, f'value_{index}')

    # def test_compaction_during_heavy_load(self):
    #     """Test compaction during heavy read/write operations."""
    #     def perform_operations(store, thread_id):
    #         for i in range(50):
    #             key = f'key_{thread_id}_{i}'
    #             store.set(key, f'value_{i}')
    #             value = store.get(key)
    #             self.assertEqual(value, f'value_{i}')
    #             time.sleep(0.005)

    #     threads = []
    #     for i in range(3):
    #         thread = threading.Thread(target=perform_operations, args=(self.kv_store, i))
    #         threads.append(thread)
    #         thread.start()

    #     # Start compaction while operations are happening
    #     self.kv_store.start_compaction()

    #     for thread in threads:
    #         thread.join()
    #     self.kv_store.wait_for_compaction()

    #     # Verify that data is correct after compaction
    #     for i in range(3):
    #         for j in range(50):
    #             key = f'key_{i}_{j}'
    #             value = self.kv_store.get(key)
    #             self.assertEqual(value, f'value_{j}')

    # def test_no_race_conditions(self):
        # """Test that no race conditions occur with concurrent access."""
        # def increment_counter(store):
        #     for _ in range(100):
        #         with store.lock:
        #             current_value = int(store.get('counter') or '0')
        #             new_value = current_value + 1
        #             store.set('counter', str(new_value))
        #         time.sleep(0.001)

        # # Initialize counter
        # self.kv_store.set('counter', '0')

        # # Start multiple threads to increment the counter
        # threads = []
        # for _ in range(5):
        #     thread = threading.Thread(target=increment_counter, args=(self.kv_store,))
        #     threads.append(thread)
        #     thread.start()

        # for thread in threads:
        #     thread.join()

        # # Verify that the counter is incremented correctly
        # final_value = int(self.kv_store.get('counter'))
        # self.assertEqual(final_value, 500)

if __name__ == '__main__':
    unittest.main()
