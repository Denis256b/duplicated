Бинарный формат `COPY` — это самый быстрый способ загрузить данные в PostgreSQL. Если текстовый формат (CSV) тратит время на парсинг и обработку специальных символов, то бинарный передает данные в "сыром" виде, как они хранятся на диске, что значительно ускоряет процесс.

### 🚀 В чем преимущество и главная сложность?

Главный плюс — производительность. В зависимости от структуры данных, загрузка может быть на **6-10% быстрее**, а для определенных типов данных (например, числовых или `BYTEA`) разница может быть еще более значительной. Также решается проблема экранирования разделителей, которые могут встретиться в тексте.

Сложность заключается в том, что вам придется вручную реализовать сериализацию данных под каждый тип столбца. Это делает код более громоздким по сравнению с простым CSV.

### 🛠️ Как это работает: структура бинарного потока

Вместо текста вы отправляете поток байтов, строго соответствующий протоколу PostgreSQL. Поток начинается с 11-байтового магического заголовка `PGCOPY\nÿ\r\n\0`, за которым следует 32-битный флаг и 32-битная длина заголовка. Затем для каждой строки отправляется 16-битное количество колонок, и для каждой колонки — 32-битная длина значения и само значение в бинарном виде. После всех строк ставится 16-битный признак конца данных (`-1`).

### ☕ Пример Java-кода с `CopyManager` и `DataOutputStream`

Вот полный пример вставки данных из `ResultSet` (например, из SAP HANA) в PostgreSQL.

```java
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class BinaryCopyExample {

    public static void main(String[] args) {
        // Параметры подключения (замените на свои)
        String hanaUrl = "jdbc:sap://your_hana_host:39013/?databaseName=HXE&encrypt=true";
        String hanaUser = "SYSTEM";
        String hanaPassword = "YourPassword";

        String pgUrl = "jdbc:postgresql://your_pg_host:5432/your_db";
        String pgUser = "postgres";
        String pgPassword = "postgres";

        String selectQuery = "SELECT id, name, value FROM your_hana_table";
        String targetPgTable = "public.your_postgres_table";

        try (Connection hanaConn = DriverManager.getConnection(hanaUrl, hanaUser, hanaPassword);
             Connection pgConn = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
             Statement hanaStmt = hanaConn.createStatement();
             ResultSet rs = hanaStmt.executeQuery(selectQuery)) {

            // Отключаем авто-коммит для массовой вставки
            pgConn.setAutoCommit(false);

            // Получаем CopyManager и командуем BINARY COPY
            BaseConnection pgBaseConn = pgConn.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgBaseConn);
            String copyCommand = "COPY " + targetPgTable + " FROM STDIN WITH (FORMAT BINARY)";

            // Запускаем копирование
            long rowsCopied = copyManager.copyIn(copyCommand, () -> resultSetToBinaryStream(rs));

            // Фиксируем транзакцию
            pgConn.commit();
            System.out.printf("Успешно перенесено %d записей.\n", rowsCopied);

        } catch (Exception e) {
            System.err.println("Ошибка при переносе данных:");
            e.printStackTrace();
        }
    }

    private static ByteArrayOutputStream resultSetToBinaryStream(ResultSet rs) throws SQLException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // --- 1. Записываем бинарный заголовок COPY ---
        // Магическая сигнатура "PGCOPY\nÿ\r\n\0"
        byte[] header = {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', '\0'};
        dos.write(header);
        dos.writeInt(0); // Флаги (0 - без OID)
        dos.writeInt(0); // Длина расширения заголовка (обычно 0)

        // --- 2. Обрабатываем данные из ResultSet ---
        while (rs.next()) {
            dos.writeShort(columnCount); // Количество колонок в строке

            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                if (value == null) {
                    dos.writeInt(-1); // -1 означает NULL в PostgreSQL binary copy
                    continue;
                }

                // Определяем тип данных и пишем его в бинарном виде
                switch (metaData.getColumnTypeName(i).toUpperCase()) {
                    case "INTEGER":
                    case "INT4":
                        dos.writeInt(4); // Длина int в байтах
                        dos.writeInt(((Number) value).intValue());
                        break;
                    case "BIGINT":
                    case "INT8":
                        dos.writeInt(8); // Длина long в байтах
                        dos.writeLong(((Number) value).longValue());
                        break;
                    case "DOUBLE PRECISION":
                    case "FLOAT8":
                        dos.writeInt(8);
                        dos.writeDouble(((Number) value).doubleValue());
                        break;
                    case "BOOLEAN":
                        dos.writeInt(1);
                        dos.writeBoolean((Boolean) value);
                        break;
                    case "VARCHAR":
                    case "TEXT":
                    case "CHAR":
                        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                        dos.writeInt(bytes.length);
                        dos.write(bytes);
                        break;
                    default:
                        // Для неучтенных типов - как строка (не лучший вариант)
                        byte[] defBytes = value.toString().getBytes(StandardCharsets.UTF_8);
                        dos.writeInt(defBytes.length);
                        dos.write(defBytes);
                        break;
                }
            }
        }

        // --- 3. Завершаем поток данных ---
        dos.writeShort(-1); // Маркер конца данных
        dos.flush();
        dos.close();
        return baos;
    }
}
```

> **Важно:** В этом примере `switch` обрабатывает базовые типы. Для таких типов, как `DATE`, `TIMESTAMP` или `NUMERIC`, требуется особое бинарное представление. Утилита `PgBulkInsert` (см. ниже) полностью решает эту проблему.

### ⚖️ Бинарный формат: плюсы и минусы

| Плюсы | Минусы |
| :--- | :--- |
| **Максимальная скорость.** Самый быстрый способ массовой загрузки данных. | **Сложность реализации.** Требует написания кода сериализации для каждого типа данных. |
| **Точность данных.** Избавляет от проблем с экранированием символов и преобразованием типов. | **Непрозрачность.** Данные нельзя просто посмотреть в текстовом редакторе. |
| **Идеален для бинарных данных.** Самый эффективный способ загружать `BYTEA` и подобные типы. | **Привязан к схеме БД.** Любое изменение порядка или типа колонок "ломает" код. |

### 💡 Совет: Упростите работу с PgBulkInsert

Если вы не хотите вручную разбираться с бинарной сериализацией для каждого типа, используйте готовую Java-библиотеку `PgBulkInsert`. Она берет на себя всю сложность бинарного протокола и предоставляет удобный API.

Пример использования `PgBulkInsert`:

```java
// Пример использования PgBulkInsert (псевдокод)
// PgBulkInsert<MyData> bulkInsert = new PgBulkInsert<>(
//     new SimpleRowWriter<>(MyData.class, "my_table")
// );
// bulkInsert.saveAll(pgConn, myDataStream);
```

### 💎 Итоги и рекомендации

1.  **Начните с CSV.** Для большинства задач текстового формата `COPY ... CSV` более чем достаточно. Он прост, читаем и отлично работает.
2.  **Переходите на BINARY, если:**
    *   У вас десятки или сотни миллионов строк, и важна каждая секунда.
    *   В данных есть много символов, требующих экранирования (кавычки, запятые).
    *   Вы загружаете бинарные данные (`bytea`), которые в текстовом виде требуют кодирования в Base64 и обратно.
3.  **Используйте готовые библиотеки.** Для регулярной работы с бинарным `COPY` в Java-проектах настоятельно рекомендуется использовать `PgBulkInsert`.

Если у вас возникнут вопросы по адаптации примера под конкретные типы данных или по использованию `PgBulkInsert`, обращайтесь.
