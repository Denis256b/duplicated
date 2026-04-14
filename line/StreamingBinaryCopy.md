Ошибка `OutOfMemoryError` возникает из-за того, что метод `resultSetToBinaryStream()` копит **все данные в памяти** перед отправкой. Когда строк сотни тысяч или миллионы, это приводит к исчерпанию heap.

Решение — **потоковая передача**: писать данные в `PipedOutputStream` из одного потока, а в другом потоке `CopyManager` будет читать из связанного `PipedInputStream`. Это позволяет отправлять данные пачками, не храня весь массив в памяти.

Вот исправленный код, использующий `PipedInputStream` / `PipedOutputStream` для потоковой загрузки:

```java
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class StreamingBinaryCopy {

    public static void main(String[] args) {
        String hanaUrl = "jdbc:sap://...";
        String hanaUser = "...";
        String hanaPassword = "...";
        String pgUrl = "jdbc:postgresql://...";
        String pgUser = "...";
        String pgPassword = "...";
        String selectQuery = "SELECT id, name, value FROM your_hana_table";
        String targetPgTable = "public.your_postgres_table";

        try (Connection hanaConn = DriverManager.getConnection(hanaUrl, hanaUser, hanaPassword);
             Connection pgConn = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
             Statement hanaStmt = hanaConn.createStatement();
             ResultSet rs = hanaStmt.executeQuery(selectQuery)) {

            pgConn.setAutoCommit(false);
            BaseConnection pgBaseConn = pgConn.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgBaseConn);
            String copyCommand = "COPY " + targetPgTable + " FROM STDIN WITH (FORMAT BINARY)";

            // Создаём пайп: PipedOutputStream -> PipedInputStream
            PipedOutputStream pos = new PipedOutputStream();
            PipedInputStream pis = new PipedInputStream(pos, 1024 * 1024); // буфер 1 МБ

            // Запускаем поток для записи данных в пайп
            Thread writerThread = new Thread(() -> {
                try {
                    writeResultSetToBinaryStream(rs, pos);
                    pos.close(); // важно закрыть после завершения
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            writerThread.start();

            // copyIn читает из PipedInputStream
            long rowsCopied = copyManager.copyIn(copyCommand, pis);
            writerThread.join(); // дождаться завершения потока записи

            pgConn.commit();
            System.out.printf("Успешно перенесено %d записей.\n", rowsCopied);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeResultSetToBinaryStream(ResultSet rs, OutputStream out) 
            throws SQLException, IOException {
        DataOutputStream dos = new DataOutputStream(out);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Бинарный заголовок
        byte[] header = {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', '\0'};
        dos.write(header);
        dos.writeInt(0); // флаги
        dos.writeInt(0); // расширение заголовка

        while (rs.next()) {
            dos.writeShort(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                if (value == null) {
                    dos.writeInt(-1);
                    continue;
                }
                switch (metaData.getColumnTypeName(i).toUpperCase()) {
                    case "INTEGER": case "INT4":
                        dos.writeInt(4);
                        dos.writeInt(((Number) value).intValue());
                        break;
                    case "BIGINT": case "INT8":
                        dos.writeInt(8);
                        dos.writeLong(((Number) value).longValue());
                        break;
                    case "DOUBLE PRECISION": case "FLOAT8":
                        dos.writeInt(8);
                        dos.writeDouble(((Number) value).doubleValue());
                        break;
                    case "BOOLEAN":
                        dos.writeInt(1);
                        dos.writeBoolean((Boolean) value);
                        break;
                    case "VARCHAR": case "TEXT": case "CHAR":
                        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                        dos.writeInt(bytes.length);
                        dos.write(bytes);
                        break;
                    default:
                        byte[] defBytes = value.toString().getBytes(StandardCharsets.UTF_8);
                        dos.writeInt(defBytes.length);
                        dos.write(defBytes);
                }
            }
            dos.flush(); // периодический сброс, чтобы не копить в буфере потока
        }
        dos.writeShort(-1); // конец данных
        dos.flush();
    }
}
```

### 🔍 Что изменилось и почему это решает проблему памяти?

1. **Убрали `ByteArrayOutputStream`** — теперь данные не накапливаются в одном большом массиве.
2. **Использовали `PipedOutputStream` / `PipedInputStream`** — они работают как труба между потоками: пока один поток пишет, второй читает и отправляет в PostgreSQL. Данные хранятся только в небольшом кольцевом буфере пайпа (1 МБ в примере).
3. **`dos.flush()`** — принудительно выталкивает данные из буфера `DataOutputStream` в пайп, чтобы они не копились внутри.

### 📌 Альтернативный вариант: текстовый COPY через `Reader`

Если бинарный формат не принципиален, можно использовать **текстовый CSV** с `Reader`, что тоже потоково и гораздо проще в реализации:

```java
Reader reader = new InputStreamReader(resultSetToCsvStream(rs), StandardCharsets.UTF_8);
long rowsCopied = copyManager.copyIn("COPY table FROM STDIN WITH CSV", reader);
```

Но для этого нужно реализовать `resultSetToCsvStream` как потоковую генерацию CSV (например, через `StringBuilder` с периодическим сбросом в `PipedWriter`).

### 💎 Итог

Теперь вы можете переносить миллионы записей без риска `OutOfMemoryError`. Если появятся вопросы по работе кода или нужна помощь с адаптацией под конкретные типы данных — обращайтесь.
