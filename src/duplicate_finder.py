import os
import sys
import hashlib
import json
import time
import webbrowser
import platform

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

def generate_html_report(duplicates, source_folder, search_folder, output_file="duplicate_report.html"):
    """Генерирует HTML-отчет о найденных дубликатах"""
    
    # Подсчитываем количество дубликатов
    total_duplicates = len(duplicates)
    
    # Генерируем HTML-контент
    html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Отчет о дубликатах файлов</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            text-align: center;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .summary {{
            background-color: #e8f5e8;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .summary p {{
            margin: 5px 0;
            font-size: 16px;
        }}
        .duplicates-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        .duplicates-table th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
        }}
        .duplicates-table td {{
            padding: 12px;
            border-bottom: 1px solid #ddd;
        }}
        .duplicates-table tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        .duplicates-table tr:hover {{
            background-color: #e8f5e8;
        }}
        .file-path {{
            word-break: break-all;
            font-size: 14px;
        }}
        .footer {{
            margin-top: 30px;
            text-align: center;
            color: #666;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Отчет о дубликатах файлов</h1>
        
        <div class="summary">
            <p><strong>Исходная папка:</strong> {source_folder}</p>
            <p><strong>Папка поиска:</strong> {search_folder}</p>
            <p><strong>Найдено дубликатов:</strong> {total_duplicates}</p>
            <p><strong>Дата генерации:</strong> {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <h2>Список дубликатов</h2>
        
        {"""
        <table class="duplicates-table">
            <thead>
                <tr>
                    <th>Файл в исходной папке</th>
                    <th>Файл в папке поиска</th>
                </tr>
            </thead>
            <tbody>
        """ + "".join(f"""
                <tr>
                    <td class="file-path">{duplicate[0]}</td>
                    <td class="file-path">{duplicate[1]}</td>
                </tr>
            """ for duplicate in duplicates) + """
            </tbody>
        </table>
        """ if duplicates else "<p>Дубликаты не найдены.</p>"}
        
        <div class="footer">
            <p>Сгенерировано с помощью программы поиска дубликатов файлов</p>
        </div>
    </div>
</body>
</html>"""
    
    # Записываем HTML-файл
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML-отчет сохранен в файл: {output_file}")
        return output_file
    except Exception as e:
        print(f"Ошибка при сохранении HTML-файла: {e}")
        return None

def open_html_report(file_path):
    """Открывает HTML-файл в браузере"""
    try:
        # Определяем операционную систему
        system = platform.system()
        
        if system == "Windows":
            os.startfile(file_path)
        elif system == "Darwin":  # macOS
            os.system(f"open {file_path}")
        else:  # Linux и другие
            os.system(f"xdg-open {file_path}")
            
        print(f"Отчет открыт в браузере: {file_path}")
    except Exception as e:
        print(f"Ошибка при открытии файла в браузере: {e}")

# Основная программа
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
    import os
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
