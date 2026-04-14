Вероятно, это самый быстрый способ вставить большой объём данных в PostgreSQL из Java-приложения — использовать команду `COPY` через `CopyManager`.

Почти все бенчмарки показывают, что `COPY` **на порядок быстрее** пакетной вставки (Batch Insert) на больших объёмах. Например, вставка 1 миллиона строк с помощью `COPY` занимает около 5–10 секунд, в то время как пакетная вставка может потребовать 50–120 секунд. На проектах с десятками миллионов записей этот разрыв становится критическим.

*   **JDBC Batch Insert (Пакетная вставка)**: Это значительный шаг вперёд по сравнению с одиночными вставками, так как он группирует запросы и снижает количество обращений к сети. Однако каждый `INSERT` всё равно проходит через SQL-парсер и планировщик PostgreSQL, что создаёт накладные расходы.
*   **PostgreSQL `COPY`**: Это специализированная команда для массовой загрузки данных, которая работает на более низком уровне. Она **обходит** парсер и планировщик SQL, минимизирует запись в журнал WAL и передаёт данные от клиента к серверу в виде непрерывного потока. Это делает её абсолютным лидером по скорости.

### 🚀 Пример кода: Использование `CopyManager`

Вот готовый код, который загружает данные из вашего `ResultSet` (например, из SAP HANA) напрямую в PostgreSQL.

**Важно:** Для работы этого кода необходимо добавить в проект драйвер PostgreSQL JDBC (`org.postgresql:postgresql`).

```java
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class HanaToPostgresFastInsert {

    public static void main(String[] args) {
        // --- Параметры подключения (замените на свои) ---
        String hanaUrl = "jdbc:sap://your_hana_host:39013/?databaseName=HXE&encrypt=true";
        String hanaUser = "SYSTEM";
        String hanaPassword = "YourPassword";

        String pgUrl = "jdbc:postgresql://your_pg_host:5432/your_db";
        String pgUser = "postgres";
        String pgPassword = "postgres";
        
        String selectQuery = "SELECT id, name, value FROM your_hana_table"; // Ваш запрос в HANA
        String targetPgTable = "your_postgres_table"; // Целевая таблица в PostgreSQL
        // -------------------------------------------------

        // 1. Подключаемся к обеим базам
        try (Connection hanaConn = DriverManager.getConnection(hanaUrl, hanaUser, hanaPassword);
             Connection pgConn = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
             Statement hanaStmt = hanaConn.createStatement();
             ResultSet rs = hanaStmt.executeQuery(selectQuery)) {

            System.out.println("Начинаем перенос данных...");

            // 2. Получаем объект CopyManager из PostgreSQL-соединения
            BaseConnection pgBaseConn = pgConn.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgBaseConn);

            // 3. Готовим команду COPY для PostgreSQL
            String copyCommand = "COPY " + targetPgTable + " FROM STDIN WITH (FORMAT CSV, DELIMITER '|')";
            
            // 4. Конвертируем ResultSet в CSV-поток и отправляем в PostgreSQL
            long rowsCopied = copyManager.copyIn(copyCommand, () -> resultSetToCsvStream(rs));

            System.out.printf("Успешно перенесено %d записей.\n", rowsCopied);

        } catch (SQLException | IOException e) {
            System.err.println("Ошибка при переносе данных:");
            e.printStackTrace();
        }
    }

    /**
     * Конвертирует ResultSet в поток ByteArrayInputStream в формате CSV.
     * Данные записываются в память и только потом отправляются.
     */
    private static ByteArrayInputStream resultSetToCsvStream(ResultSet rs) throws SQLException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            StringBuilder line = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                String value = rs.getString(i);
                // Экранируем значения, если они содержат разделитель или кавычки
                if (value != null) {
                    if (value.contains("|") || value.contains("\"") || value.contains("\n")) {
                        value = "\"" + value.replace("\"", "\"\"") + "\"";
                    }
                    line.append(value);
                }
                if (i < columnCount) line.append("|");
            }
            line.append("\n");
            baos.write(line.toString().getBytes(StandardCharsets.UTF_8));
        }
        return new ByteArrayInputStream(baos.toByteArray());
    }
}
```

### 📝 Как это работает

1.  **Подготовка:** Код устанавливает соединения с HANA и PostgreSQL, затем получает `CopyManager` из вашего подключения к Postgres.
2.  **Формирование команды:** Ключевая строка — `COPY target_table FROM STDIN WITH (FORMAT CSV, DELIMITER '|')`. Она говорит PostgreSQL ожидать данные в формате CSV с разделителем `|` из стандартного потока ввода.
3.  **Потоковая передача:** Метод `resultSetToCsvStream` читает `ResultSet` из HANA строка за строкой и преобразует его в CSV-формат в памяти. `CopyManager` берёт этот поток и отправляет его напрямую в PostgreSQL.
4.  **Отключение авто-коммита:** **Обязательно** выполняйте вставку больших объёмов в рамках одной транзакции (`pgConn.setAutoCommit(false);`) и делайте коммит только в конце. Это значительно ускорит процесс.

Этот подход позволяет перегнать данные из SAP HANA в PostgreSQL максимально быстро, сводя к минимуму накладные расходы на сеть и обработку SQL.

Если у вас появятся вопросы по адаптации этого кода под конкретные типы данных или структуру таблиц, спрашивайте.
