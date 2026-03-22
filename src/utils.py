import os
import hashlib
import json
import time

def get_file_hash(file_path, chunk_size=8192):
    """Вычисляет MD5 хэш файла"""
    try:
        with open(file_path, 'rb') as f:
            hash_md5 = hashlib.md5()
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Ошибка при вычислении хэша файла {file_path}: {e}")
        return None

def load_cache(cache_file):
    """Загружает кэш из файла"""
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Ошибка при загрузке кэша: {e}")
        return {}

def save_cache(cache_file, cache_data):
    """Сохраняет кэш в файл"""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"Ошибка при сохранении кэша: {e}")

def get_file_info(file_path):
    """Получает информацию о файле"""
    try:
        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'mtime': stat.st_mtime
        }
    except Exception as e:
        print(f"Ошибка при получении информации о файле {file_path}: {e}")
        return None

def is_hidden_file(file_path):
    """Проверяет, является ли файл скрытым"""
    try:
        filename = os.path.basename(file_path)
        return filename.startswith('.') and not filename in ['.', '..']
    except Exception as e:
        print(f"Ошибка при проверке скрытого файла {file_path}: {e}")
        return False