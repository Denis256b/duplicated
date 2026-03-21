import os
import sys
import hashlib
import json
import time

def get_file_hash(file_path, chunk_size=8192):
    """Calculate MD5 hash of a file"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def load_cache(cache_file):
    """Load hash cache from file"""
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading cache: {e}")
    return {}

def save_cache(cache_file, cache_data):
    """Save hash cache to file"""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)
    except Exception as e:
        print(f"Error saving cache: {e}")

def get_file_info(file_path):
    """Get file size and modification time for cache validation"""
    try:
        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'mtime': stat.st_mtime
        }
    except Exception as e:
        print(f"Error getting file info for {file_path}: {e}")
        return None

def find_duplicates_between_folders(source_folder, search_folder, use_cache=True):
    """Find files in source_folder that have duplicates in search_folder"""
    
    # Cache file path
    cache_file = f".duplicate_cache_{os.path.basename(search_folder)}"
    
    # Load existing cache
    cache = {}
    if use_cache:
        cache = load_cache(cache_file)
    
    # Dictionary to store search folder files and their hashes
    search_hashes = {}
    search_files = []
    
    # Cache for source folder files
    source_cache = {}
    
    try:
        # Process search folder files
        print(f"Scanning search folder: {search_folder}")
        start_time = time.time()
        
        for root, dirs, files in os.walk(search_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    # Check if we have cached data
                    file_key = os.path.relpath(file_path, search_folder)
                    cached_data = cache.get(file_key)
                    
                    if cached_data:
                        # Validate cache (check if file was modified)
                        file_info = get_file_info(file_path)
                        if file_info and file_info == cached_data.get('info'):
                            hash_value = cached_data['hash']
                            print(f"Using cached hash for {file_path}")
                        else:
                            # File was modified, recalculate hash
                            hash_value = get_file_hash(file_path)
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': file_info
                            }
                    else:
                        # Calculate new hash
                        hash_value = get_file_hash(file_path)
                        if hash_value:
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': get_file_info(file_path)
                            }
                    
                    if hash_value:
                        search_hashes[hash_value] = file_path
                        search_files.append(file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
        
        # Save updated cache
        if use_cache and len(cache) > 0:
            save_cache(cache_file, cache)
            
        end_time = time.time()
        print(f"Search folder scan completed in {end_time - start_time:.2f} seconds")
        
        # Process source folder files
        print(f"Scanning source folder: {source_folder}")
        start_time = time.time()
        
        duplicates = []
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    # Check if we have cached data
                    file_key = os.path.relpath(file_path, source_folder)
                    cached_data = cache.get(file_key)
                    
                    if cached_data:
                        # Validate cache (check if file was modified)
                        file_info = get_file_info(file_path)
                        if file_info and file_info == cached_data.get('info'):
                            hash_value = cached_data['hash']
                            print(f"Using cached hash for {file_path}")
                        else:
                            # File was modified, recalculate hash
                            hash_value = get_file_hash(file_path)
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': file_info
                            }
                    else:
                        # Calculate new hash
                        hash_value = get_file_hash(file_path)
                        if hash_value:
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': get_file_info(file_path)
                            }
                    
                    if hash_value and hash_value in search_hashes:
                        duplicates.append((file_path, search_hashes[hash_value]))
                        
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
        
        end_time = time.time()
        print(f"Source folder scan completed in {end_time - start_time:.2f} seconds")
        
        return duplicates
        
    except Exception as e:
        print(f"Error during duplicate search: {e}")
        return []

# Добавим функцию для поиска дубликатов
def find_duplicates(source_folder, search_folder):
    import os
    import hashlib
    
    # Словарь для хранения хэшей файлов
    file_hashes = {}
    duplicates = []
    
    def get_file_hash(filepath):
        """Вычисляет хэш файла"""
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                # Читаем файл блоками для экономии памяти
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            print(f"Error reading file {filepath}: {e}")
            return None
    
    # Сначала собираем хэши файлов из папки поиска
    for root, dirs, files in os.walk(search_folder):
        for file in files:
            filepath = os.path.join(root, file)
            try:
                file_hash = get_file_hash(filepath)
                if file_hash:
                    if file_hash not in file_hashes:
                        file_hashes[file_hash] = []
                    file_hashes[file_hash].append(filepath)
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
    
    # Теперь проверяем файлы из исходной папки
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            filepath = os.path.join(root, file)
            try:
                file_hash = get_file_hash(filepath)
                if file_hash and file_hash in file_hashes:
                    # Если хэш найден в поисковой папке, добавляем в дубликаты
                    for search_filepath in file_hashes[file_hash]:
                        # Проверяем, что это разные файлы (не один и тот же)
                        if filepath != search_filepath:
                            duplicates.append((filepath, search_filepath))
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
    
    return duplicates

# Основная программа
if __name__ == "__main__":
    import sys
    
    # Получаем параметры командной строки
    if len(sys.argv) >= 3:
        source_folder = sys.argv[1]
        search_folder = sys.argv[2]
    else:
        # Если параметры не заданы, используем текущую папку
        source_folder = "."
        search_folder = "."
    
    print(f"Source folder: {source_folder}")
    print(f"Search folder: {search_folder}")
    
    # Проверяем существование папок
    import os
    if not os.path.exists(source_folder):
        print(f"Error: Source folder '{source_folder}' does not exist.")
        sys.exit(1)
        
    if not os.path.exists(search_folder):
        print(f"Error: Search folder '{search_folder}' does not exist.")
        sys.exit(1)
    
    # Ищем дубликаты
    duplicates = find_duplicates(source_folder, search_folder)
    
    # Выводим результаты
    if duplicates:
        print(f"\nFound {len(duplicates)} duplicate files:")
        for source_file, search_file in duplicates:
            print(f"  {source_file} -> {search_file}")
    else:
        print("\nNo duplicates found.")
        
    print("\nDone.")

