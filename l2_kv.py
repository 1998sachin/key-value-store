import os

class KeyValueStore:
    def __init__(self, file_name, index_file_name):
        self.file_name = file_name
        self.index_file_name = index_file_name
        self.index = self.load_index()
    
    def load_index(self):
        """Load the index from the separate index file."""
        index = {}
        if os.path.exists(self.index_file_name):
            with open(self.index_file_name, 'r') as index_file:
                for line in index_file:
                    key, offset = line.strip().split(' ')
                    index[key] = int(offset)
        return index

    def update_index(self, key, offset):
        """Update the index file only for the changed key."""
        # Open the index file in append mode
        with open(self.index_file_name, 'a') as index_file:
            index_file.write(f"{key} {offset}\n")
            index_file.flush()
            os.fsync(index_file.fileno())  # Ensure index is flushed to disk

    def set(self, key, value):
        """Set a key-value pair."""
        with open(self.file_name, 'a') as f:
            offset = f.tell()  # Get current file position
            f.write(f"{key} {value}\n")
            f.flush()
            os.fsync(f.fileno())  # Ensure data is flushed to disk

        # Update the in-memory index and the index file
        self.index[key] = offset
        self.update_index(key, offset)

    def get(self, key):
        """Get the value for a given key."""
        if key not in self.index:
            return None  # Key not found
        with open(self.file_name, 'r') as f:
            f.seek(self.index[key])  # Move to the position of the value
            line = f.readline()
            if not line:
                return None  # In case of incomplete writes
            _, value = line.strip().split(' ', 1)
            return value

    def update(self, key, new_value):
        """Update the value for a given key."""
        # Treat update as set operation
        self.set(key, new_value)

    def compact(self):
        """Compact the data and index files to remove obsolete entries."""
        # Temporary files for compaction
        temp_data_file = self.file_name + '.tmp'
        temp_index_file = self.index_file_name + '.tmp'

        # Dictionary to hold latest key-value pairs
        latest_entries = {}

        # First, read the latest entries from the in-memory index
        for key, offset in self.index.items():
            latest_entries[key] = (offset, None)  # We'll fill value later

        # Read the latest values from the data file
        with open(self.file_name, 'r') as data_file:
            for key in latest_entries:
                data_file.seek(latest_entries[key][0])
                line = data_file.readline()
                _, value = line.strip().split(' ', 1)
                latest_entries[key] = (latest_entries[key][0], value)

        # Write the latest entries to the new data file and rebuild index
        new_index = {}
        with open(temp_data_file, 'w') as new_data_file:
            offset = 0
            for key, (old_offset, value) in latest_entries.items():
                line = f"{key} {value}\n"
                new_data_file.write(line)
                new_index[key] = offset
                offset += len(line)

        # Replace the old data file with the new compacted file
        os.replace(temp_data_file, self.file_name)

        # Write the new index to the index file
        with open(temp_index_file, 'w') as new_index_file:
            for key, offset in new_index.items():
                new_index_file.write(f"{key} {offset}\n")

        # Replace the old index file with the new compacted index file
        os.replace(temp_index_file, self.index_file_name)

        # Update the in-memory index
        self.index = new_index

    def display_index(self):
        """Display the current in-memory index for debugging."""
        return self.index
