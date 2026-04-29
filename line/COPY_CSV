Для импорта CSV-файла в PostgreSQL, когда в файле может быть больше столбцов, чем в целевой таблице, нужно на стороне приложения отфильтровать лишние столбцы. Ниже приведён надёжный код на C# с использованием библиотек **CsvHelper** (для чтения CSV с правильной обработкой кавычек) и **Npgsql** (для выполнения `COPY`).

## Необходимые NuGet пакеты
```bash
dotnet add package CsvHelper
dotnet add package Npgsql
```

## Полный код импорта с игнорированием лишних столбцов

```csharp
using CsvHelper;
using CsvHelper.Configuration;
using Npgsql;
using System;
using System.Globalization;
using System.IO;
using System.Linq;

public class PostgresCsvImporter
{
    /// <summary>
    /// Импортирует данные из CSV-файла в таблицу PostgreSQL.
    /// Лишние столбцы в CSV игнорируются.
    /// </summary>
    /// <param name="connectionString">Строка подключения к PostgreSQL</param>
    /// <param name="csvFilePath">Путь к CSV-файлу</param>
    /// <param name="targetTable">Имя целевой таблицы (включая схему, если нужно)</param>
    /// <param name="targetColumns">Массив имён столбцов целевой таблицы, которые нужно заполнить</param>
    public static void ImportWithExtraColumns(
        string connectionString,
        string csvFilePath,
        string targetTable,
        string[] targetColumns)
    {
        using var conn = new NpgsqlConnection(connectionString);
        conn.Open();

        // Настройка чтения CSV (предполагаем заголовок и запятую как разделитель)
        var readConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            Delimiter = ","
        };

        using var reader = new StreamReader(csvFilePath);
        using var csvReader = new CsvReader(reader, readConfig);

        // Читаем заголовки CSV
        csvReader.Read();
        csvReader.ReadHeader();
        var csvHeaders = csvReader.HeaderRecord
            ?? throw new InvalidOperationException("CSV-файл не содержит заголовков");

        // Для каждого целевого столбца находим его индекс в CSV (или -1, если отсутствует)
        var columnIndices = targetColumns
            .Select(col => Array.IndexOf(csvHeaders, col))
            .ToArray();

        if (columnIndices.All(idx => idx == -1))
            throw new InvalidOperationException("Ни один из целевых столбцов не найден в CSV");

        // Формируем команду COPY, указывая только нужные столбцы таблицы
        var quotedColumns = targetColumns.Select(c => $"\"{c}\"");
        var copyCommand = $"COPY {targetTable} ({string.Join(",", quotedColumns)}) FROM STDIN (FORMAT csv, DELIMITER ',')";

        // Начинаем импорт через COPY
        using var writer = conn.BeginTextImport(copyCommand);
        // Используем CsvWriter для корректного экранирования полей и записи в поток COPY
        using var csvWriter = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = "," });

        // Построчное чтение из исходного CSV и запись только нужных столбцов
        while (csvReader.Read())
        {
            for (int i = 0; i < targetColumns.Length; i++)
            {
                int idx = columnIndices[i];
                if (idx >= 0)
                    csvWriter.WriteField(csvReader.GetField(idx));
                else
                    csvWriter.WriteField(null); // Если столбец отсутствует в CSV, пишем NULL
            }
            csvWriter.NextRecord();
        }
    }
}
```

## Пример использования

```csharp
var connectionString = "Host=localhost;Database=mydb;Username=postgres;Password=pass";
var csvPath = @"C:\data\users.csv";
string[] tableColumns = { "id", "name", "email" }; // целевый столбцы таблицы "users"

PostgresCsvImporter.ImportWithExtraColumns(connectionString, csvPath, "users", tableColumns);
```

## Как это работает

1. **Чтение заголовков CSV** – определяем, какие столбцы присутствуют в файле.
2. **Сопоставление столбцов** – для каждого целевого столбца таблицы находим его позицию в CSV. Если столбец отсутствует, индекс будет `-1` – тогда вместо него будет вставлен `NULL`.
3. **COPY с фильтрацией** – команда `COPY` указывает только те столбцы таблицы, которые мы собираемся заполнить. PostgreSQL ожидает ровно столько полей в каждой строке.
4. **Запись через CsvWriter** – он автоматически экранирует кавычки, запятые и специальные символы, формируя корректную CSV-строку для `COPY`.

## Преимущества подхода

- **Устойчивость к лишним столбцам** – лишние данные в CSV просто игнорируются.
- **Производительность** – единый поток `COPY` на уровне базы данных, без построчных `INSERT`.
- **Корректная обработка кавычек и разделителей** – благодаря `CsvHelper` и `CsvWriter`.
- **Гибкость** – легко добавить преобразование типов, обработку NULL или логирование.

## Примечания

- Убедитесь, что порядок `targetColumns` соответствует тому, как вы хотите заполнять таблицу. Порядок в CSV не важен – мы сопоставляем по именам заголовков.
- Если CSV-файл не содержит заголовков, измените `HasHeaderRecord = false` и укажите индексы столбцов вручную.
- При очень больших файлах (сотни МБ) и ограниченной памяти можно настроить буферизацию, но в большинстве случаев код работает эффективно.

Этот код решает задачу полностью: импорт не ломается при наличии лишних столбцов в CSV.
