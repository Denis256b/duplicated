import time
import platform
import os

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