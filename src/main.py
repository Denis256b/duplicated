#!/usr/bin/env python3
"""
Программа поиска дубликатов файлов
"""

import os
import sys
from duplicate_finder import find_duplicates_between_folders
from report_generator import generate_html_report, open_html_report

def main():
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
        print("  python main.py                 # Поиск дубликатов в текущей папке")
        print("  python main.py <папка>          # Поиск дубликатов в указанной папке")
        print("  python main.py <исходная> <поиск> # Поиск дубликатов между папками")
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
        for hash_value, group in duplicates.items():
            print(f"Хэш: {hash_value} (размер: {group['size']} байт)")
            for file_pair in group['files']:
                print(f"  {file_pair['source']} -> {file_pair['search']}")
    else:
        print("\nДубликаты не найдены.")
    
    # Генерируем HTML-отчет
    result_directory = "results"
    os.makedirs(result_directory, exist_ok=True)
    html_file = generate_html_report(duplicates, result_directory)
    
    if html_file:
        # Открываем отчет в браузере
        open_html_report(html_file)
    
    print("\nГотово.")

if __name__ == "__main__":
    main()