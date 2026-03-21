def find_duplicates_between_folders(source_folder, search_folder):
    """Find files in source_folder that have duplicates in search_folder"""
    # First, get all files and their hashes from the search folder
    search_hashes = {}
    search_files = []
    
    try:
        for root, dirs, files in os.walk(search_folder):
            hidden_dirs = []
            # Remove hidden directories from the walk
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            # Save their paths for report
            hidden_dirs.extend([os.path.join(root, d) for d in dirs if d.startswith('.')])

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
            hidden_dirs = [] # Reset hidden directories for each subfolder
            for filename in files:
                file_path = os.path.join(root, filename)
                file_hash = get_file_hash(file_path)
                if file_hash is not None and file_hash in search_hashes:
                    # Found a duplicate
                    duplicates.append((file_path, search_hashes[file_hash]))
    except Exception as e:
        print(f"Error walking source folder {source_folder}: {e}")

    # After traversing, show hidden directories
    if hidden_dirs:
        print("\nПропущенные (скрытые) папки:")
        for d in sorted(hidden_dirs):
            print(f"  {d}")

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
        print(f"\nНайденные дубликаты (всего {len(duplicates)}):")
        for dup_file, orig_file in duplicates:
            print(f"  Дубликат: {dup_file}")
            print(f"  Исходный:  {orig_file}")
    else:
        print("No duplicates found.")

if __name__ == "__main__":
    main()
