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
        # Even if the key doesn't exist, treat it as a set operation
        self.set(key, new_value)

    def display_index(self):
        """Display the current in-memory index for debugging."""
        return self.index

