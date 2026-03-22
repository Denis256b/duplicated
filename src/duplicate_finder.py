import os
import sys
import hashlib
import json
import time
import platform
from utils import get_file_hash, load_cache, save_cache, get_file_info, is_hidden_file
from report_generator import generate_html_report, open_html_report

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
                        # Проверяем, что файлы не совпадают
                        search_file_path = search_hashes[hash_value]
                        if file_path != search_file_path:
                            duplicates.append((file_path, search_file_path))
                        
                except Exception as e:
                    print(f"Ошибка обработки {file_path}: {e}")
        
        end_time = time.time()
        print(f"Сканирование исходной папки завершено за {end_time - start_time:.2f} секунд")
        
        return duplicates
        
    except Exception as e:
        print(f"Ошибка при поиске дубликатов: {e}")
        return []

if __name__ == "__main__":
    # Обрабатываем параметры командной строки
    if len(sys.argv) == 1:
        # Если параметры не заданы - ищем дубликаты только в текущей папке
        source_folder = "."
        search_folder = "."
    elif len(sys.argv) == 2:
        # Если задан один параметр - ищем дубликаты только в этой папке
        source_folder = sys.argv[1]
        search_folder = sys.argv[1]
    elif len(sys.argv) == 3:
        # Если заданы два параметра - это исходная папка и папка для поиска
        source_folder = sys.argv[1]
        search_folder = sys.argv[2]
    else:
        print("Использование:")
        print("  python duplicate_finder.py                 # Поиск дубликатов в текущей папке")
        print("  python duplicate_finder.py <папка>          # Поиск дубликатов в указанной папке")
        print("  python duplicate_finder.py <исходная> <поиск> # Поиск дубликатов между папками")
        sys.exit(1)
    
    print(f"Исходная папка: {source_folder}")
    print(f"Папка поиска: {search_folder}")
    
    # Проверяем существование папок
    if not os.path.exists(source_folder):
        print(f"Ошибка: Исходная папка '{source_folder}' не существует.")
        sys.exit(1)
        
    if not os.path.exists(search_folder):
        print(f"Ошибка: Папка поиска '{search_folder}' не существует.")
        sys.exit(1)
    
    # Ищем дубликаты
    duplicates = find_duplicates_between_folders(source_folder, search_folder)
    
    # Выводим результаты в консоль
    if duplicates:
        print(f"\nНайдено {len(duplicates)} дублирующихся файлов:")
        for source_file, search_file in duplicates:
            print(f"  {source_file} -> {search_file}")
    else:
        print("\nДубликаты не найдены.")
    
    # Генерируем HTML-отчет
    html_file = generate_html_report(duplicates, source_folder, search_folder)
    
    if html_file:
        # Открываем отчет в браузере
        open_html_report(html_file)
    
    print("\nГотово.")