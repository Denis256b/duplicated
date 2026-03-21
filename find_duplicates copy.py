import os
import sys
import hashlib
from pathlib import Path

def get_file_hash(file_path, block_size=65536):
    """Calculate the hash of a file"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(block_size), b""):
                hash_md5.update(chunk)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None
    return hash_md5.hexdigest()

def find_duplicates_in_folder(folder_path):
    """Find duplicate files in a folder based on their content hash"""
    file_hashes = {}
    duplicates = []
    
    try:
        for root, dirs, files in os.walk(folder_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                try:
                    file_hash = get_file_hash(file_path)
                    if file_hash is not None:
                        if file_hash in file_hashes:
                            # Found a duplicate
                            duplicates.append((file_hashes[file_hash], file_path))
                        else:
                            file_hashes[file_hash] = file_path
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
    except Exception as e:
        print(f"Error walking folder {folder_path}: {e}")
    
    return duplicates

def find_duplicates_between_folders(source_folder, search_folder):
    """Find files in source_folder that have duplicates in search_folder"""
    # First, get all files and their hashes from the search folder
    search_hashes = {}
    search_files = []
    
    try:
        for root, dirs, files in os.walk(search_folder):
            for filename in files:
                file_path = os.path.join(root, filename)
                file_hash = get_file_hash(file_path)
                if file_hash is not None:
                    search_hashes[file_hash] = file_path
                    search_files.append((file_path, file_hash))
    except Exception as e:
        print(f"Error walking search folder {search_folder}: {e}")
        return []
    
    # Now check files in source folder against search folder hashes
    duplicates = []
    
    try:
        for root, dirs, files in os.walk(source_folder):
            for filename in files:
                file_path = os.path.join(root, filename)
                file_hash = get_file_hash(file_path)
                if file_hash is not None and file_hash in search_hashes:
                    # Found a duplicate
                    duplicates.append((file_path, search_hashes[file_hash]))
    except Exception as e:
        print(f"Error walking source folder {source_folder}: {e}")
    
    return duplicates

def main():
    # Get command line arguments
    if len(sys.argv) < 3:
        # If no arguments provided, use current directory for both parameters
        source_folder = "."
        search_folder = "."
        print("No arguments provided. Searching for duplicates in current folder.")
    else:
        source_folder = sys.argv[1]
        search_folder = sys.argv[2]
        print(f"Searching for duplicates: Source folder '{source_folder}' -> Search folder '{search_folder}'")
    
    # Validate folders
    import os
    if not os.path.exists(source_folder):
        print(f"Error: Source folder '{source_folder}' does not exist.")
        return
    
    if not os.path.exists(search_folder):
        print(f"Error: Search folder '{search_folder}' does not exist.")
        return
    
    # Find duplicates
    duplicates = find_duplicates_between_folders(source_folder, search_folder)
    
    if duplicates:
        print(f"\nFound {len(duplicates)} duplicate files:")
        for source_file, duplicate_file in duplicates:
            print(f"  {source_file} -> {duplicate_file}")
    else:
        print("\nNo duplicates found.")

if __name__ == "__main__":
    main()