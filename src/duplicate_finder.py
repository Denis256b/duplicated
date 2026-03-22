import os
import sys
import hashlib
import json
import time

def get_file_hash(file_path, chunk_size=8192):
    """Вычисляет MD5 хэш файла"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            # Читаем файл блоками для эффективной обработки больших файлов
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Ошибка чтения файла {file_path}: {e}")
        return None

def load_cache(cache_file):
    """Загружает кэш хэшей из файла"""
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки кэша: {e}")
    return {}

def save_cache(cache_file, cache_data):
    """Сохраняет кэш хэшей в файл"""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)
    except Exception as e:
        print(f"Ошибка сохранения кэша: {e}")

def get_file_info(file_path):
    """Получает размер файла и время модификации для проверки кэша"""
    try:
        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'mtime': stat.st_mtime
        }
    except Exception as e:
        print(f"Ошибка получения информации о файле {file_path}: {e}")
        return None

def is_hidden_file(filepath):
    """Проверяет, является ли файл скрытым"""
    # Получаем имя файла из пути
    filename = os.path.basename(filepath)
    # В Unix/Linux скрытые файлы начинаются с точки
    if filename.startswith('.'):
        return True
    # В Windows скрытые файлы могут быть помечены атрибутом
    try:
        import stat
        file_stat = os.stat(filepath)
        if file_stat.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN:
            return True
    except:
        # Если не удалось проверить атрибуты (например, на Unix)
        pass
    return False

def find_duplicates_between_folders(source_folder, search_folder, use_cache=True):
    """Находит файлы в source_folder, которые имеют дубликаты в search_folder"""
    
    # Путь к файлу кэша
    cache_file = f".duplicate_cache_{os.path.basename(search_folder)}"
    
    # Загружаем существующий кэш
    cache = {}
    if use_cache:
        cache = load_cache(cache_file)
    
    # Словарь для хранения хэшей файлов из папки поиска
    search_hashes = {}
    search_files = []
    
    # Кэш для файлов из исходной папки
    source_cache = {}
    
    try:
        # Обрабатываем файлы папки поиска
        print(f"Сканирование папки поиска: {search_folder}")
        start_time = time.time()
        
        for root, dirs, files in os.walk(search_folder):
            # Пропускаем скрытые папки
            dirs[:] = [d for d in dirs if not is_hidden_file(os.path.join(root, d))]
            
            for file in files:
                file_path = os.path.join(root, file)
                # Пропускаем скрытые файлы
                if is_hidden_file(file_path):
                    continue
                    
                try:
                    # Проверяем, есть ли данные в кэше
                    file_key = os.path.relpath(file_path, search_folder)
                    cached_data = cache.get(file_key)
                    
                    if cached_data:
                        # Проверяем кэш (проверяем, был ли файл изменён)
                        file_info = get_file_info(file_path)
                        if file_info and file_info == cached_data.get('info'):
                            hash_value = cached_data['hash']
                            print(f"Используем кэшированный хэш для {file_path}")
                        else:
                            # Файл был изменён, пересчитываем хэш
                            hash_value = get_file_hash(file_path)
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': file_info
                            }
                    else:
                        # Вычисляем новый хэш
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
                    print(f"Ошибка обработки {file_path}: {e}")
        
        # Сохраняем обновлённый кэш
        if use_cache and len(cache) > 0:
            save_cache(cache_file, cache)
            
        end_time = time.time()
        print(f"Сканирование папки поиска завершено за {end_time - start_time:.2f} секунд")
        
        # Обрабатываем файлы из исходной папки
        print(f"Сканирование исходной папки: {source_folder}")
        start_time = time.time()
        
        duplicates = []
        for root, dirs, files in os.walk(source_folder):
            # Пропускаем скрытые папки
            dirs[:] = [d for d in dirs if not is_hidden_file(os.path.join(root, d))]
            
            for file in files:
                file_path = os.path.join(root, file)
                # Пропускаем скрытые файлы
                if is_hidden_file(file_path):
                    continue
                    
                try:
                    # Проверяем, есть ли данные в кэше
                    file_key = os.path.relpath(file_path, source_folder)
                    cached_data = cache.get(file_key)
                    
                    if cached_data:
                        # Проверяем кэш (проверяем, был ли файл изменён)
                        file_info = get_file_info(file_path)
                        if file_info and file_info == cached_data.get('info'):
                            hash_value = cached_data['hash']
                            print(f"Используем кэшированный хэш для {file_path}")
                        else:
                            # Файл был изменён, пересчитываем хэш
                            hash_value = get_file_hash(file_path)
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': file_info
                            }
                    else:
                        # Вычисляем новый хэш
                        hash_value = get_file_hash(file_path)
                        if hash_value:
                            cache[file_key] = {
                                'hash': hash_value,
                                'info': get_file_info(file_path)
                            }
                    
                    if hash_value and hash_value in search_hashes:
                        duplicates.append((file_path, search_hashes[hash_value]))
                        
                except Exception as e:
                    print(f"Ошибка обработки {file_path}: {e}")
        
        end_time = time.time()
        print(f"Сканирование исходной папки завершено за {end_time - start_time:.2f} секунд")
        
        return duplicates
        
    except Exception as e:
        print(f"Ошибка при поиске дубликатов: {e}")
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
            print(f"Ошибка чтения файла {filepath}: {e}")
            return None
    
    def is_hidden_file(filepath):
        """Проверяет, является ли файл скрытым"""
        # Получаем имя файла из пути
        filename = os.path.basename(filepath)
        # В Unix/Linux скрытые файлы начинаются с точки
        if filename.startswith('.'):
            return True
        # В Windows скрытые файлы могут быть помечены атрибутом
        try:
            import stat
            file_stat = os.stat(filepath)
            if file_stat.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN:
                return True
        except:
            # Если не удалось проверить атрибуты (например, на Unix)
            pass
        return False
    
    # Сначала собираем хэши файлов из папки поиска
    for root, dirs, files in os.walk(search_folder):
        # Пропускаем скрытые папки
        dirs[:] = [d for d in dirs if not is_hidden_file(os.path.join(root, d))]
        
        for file in files:
            filepath = os.path.join(root, file)
            # Пропускаем скрытые файлы
            if is_hidden_file(filepath):
                continue
                
            try:
                file_hash = get_file_hash(filepath)
                if file_hash:
                    if file_hash not in file_hashes:
                        file_hashes[file_hash] = []
                    file_hashes[file_hash].append(filepath)
            except Exception as e:
                print(f"Ошибка обработки {filepath}: {e}")
    
    # Теперь проверяем файлы из исходной папки
    for root, dirs, files in os.walk(source_folder):
        # Пропускаем скрытые папки
        dirs[:] = [d for d in dirs if not is_hidden_file(os.path.join(root, d))]
        
        for file in files:
            filepath = os.path.join(root, file)
            # Пропускаем скрытые файлы
            if is_hidden_file(filepath):
                continue
                
            try:
                file_hash = get_file_hash(filepath)
                if file_hash and file_hash in file_hashes:
                    # Если хэш найден в поисковой папке, добавляем в дубликаты
                    for search_filepath in file_hashes[file_hash]:
                        # Проверяем, что это разные файлы (не один и тот же)
                        if filepath != search_filepath:
                            duplicates.append((filepath, search_filepath))
            except Exception as e:
                print(f"Ошибка обработки {filepath}: {e}")
    
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
    
    print(f"Исходная папка: {source_folder}")
    print(f"Папка поиска: {search_folder}")
    
    # Проверяем существование папок
    import os
    if not os.path.exists(source_folder):
        print(f"Ошибка: Исходная папка '{source_folder}' не существует.")
        sys.exit(1)
        
    if not os.path.exists(search_folder):
        print(f"Ошибка: Папка поиска '{search_folder}' не существует.")
        sys.exit(1)
    
    # Ищем дубликаты
    duplicates = find_duplicates(source_folder, search_folder)
    
    # Выводим результаты
    if duplicates:
        print(f"\nНайдено {len(duplicates)} дублирующихся файлов:")
        for source_file, search_file in duplicates:
            print(f"  {source_file} -> {search_file}")
    else:
        print("\nДубликаты не найдены.")
        
    print("\nГотово.")
