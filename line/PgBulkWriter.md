Библиотека `PgBulkInsert` — это, пожалуй, самый удобный способ реализовать высокоскоростную бинарную вставку в PostgreSQL, который мы обсуждали. Она полностью скрывает сложность ручной сериализации данных.

### 📦 Шаг 1: Добавление зависимости

Для начала добавьте библиотеку в ваш `pom.xml` (актуальная версия 9.0.0):

```xml
<dependency>
    <groupId>de.bytefish</groupId>
    <artifactId>pgbulkinsert</artifactId>
    <version>9.0.0</version>
</dependency>
```

### 🚀 Шаг 2: Написание кода (API версии 9.x)

В новых версиях библиотеки используется функциональный и типобезопасный API, состоящий из трёх шагов.

#### 1. Определите модель данных (POJO / Record)
Создайте класс, который будет представлять строку вашей таблицы. Рекомендуется использовать `record` для неизменяемости.

```java
import java.time.Instant;
import java.util.UUID;

public record Person(
    UUID id,
    String firstName,
    String lastName,
    Instant birthDate
) {}
```

#### 2. Настройте маппинг (с помощью `PgMapper`)
Опишите, как поля вашего Java-класса соотносятся с колонками в таблице PostgreSQL.

```java
import de.bytefish.pgbulkinsert.mapping.PgMapper;
import de.bytefish.pgbulkinsert.types.PostgresTypes;

// PgMapper — потокобезопасный объект, его можно переиспользовать
PgMapper<Person> personMapper = PgMapper.forClass(Person.class)
        .map("id", PostgresTypes.UUID.from(Person::id)) // Маппинг UUID
        .map("first_name", PostgresTypes.TEXT.removeNullCharacters().from(Person::firstName)) // Безопасная обработка строк (удаление \u0000)
        .map("last_name", PostgresTypes.TEXT.removeNullCharacters().from(Person::lastName))
        .map("birth_date", PostgresTypes.TIMESTAMPTZ.instant(Person::birthDate)); // Типобезопасная работа со временем
```

#### 3. Выполните вставку (с помощью `PgBulkWriter`)
Создайте объект `PgBulkWriter`, передав ему ваш маппинг, и вызовите метод `saveAll`.

```java
import de.bytefish.pgbulkinsert.PgBulkWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class BulkInsertExample {

    public static void main(String[] args) {
        // Ваши данные для вставки
        List<Person> people = List.of(
            new Person(UUID.randomUUID(), "John", "Doe", Instant.now()),
            new Person(UUID.randomUUID(), "Jane", "Doe", Instant.now())
            // ... другие объекты Person
        );

        // 1. Создаём маппинг (как показано выше)
        PgMapper<Person> personMapper = ...; 

        // 2. Создаём PgBulkWriter с буфером 256KB
        PgBulkWriter<Person> writer = new PgBulkWriter<>(personMapper)
                .withBufferSize(256 * 1024); 

        // 3. Выполняем вставку
        String jdbcUrl = "jdbc:postgresql://localhost:5432/your_database";
        String user = "your_username";
        String password = "your_password";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            // Передаём название таблицы (возможно, со схемой) и коллекцию данных
            writer.saveAll(conn, "public.people", people);
            System.out.println("Данные успешно вставлены!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### ⚙️ Альтернатива: API для старых версий (до 9.x)

Если вы используете более раннюю версию библиотеки, код будет выглядеть иначе. Вместо функционального маппинга нужно создать отдельный класс, наследующий `AbstractMapping`.

```java
// Файл: PersonMapping.java
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;

public class PersonMapping extends AbstractMapping<Person> {

    public PersonMapping() {
        super("public", "people"); // Указываем схему и таблицу

        this.mapText("first_name", Person::getFirstName);
        this.mapText("last_name", Person::getLastName);
        this.mapDate("birth_date", Person::getBirthDate);
        // ... другие маппинги
    }
}
```

Затем в основном коде вы создаёте экземпляр этого класса и передаёте его в `PgBulkInsert`:

```java
import de.bytefish.pgbulkinsert.PgBulkInsert;
// ...

// Создаём объект для вставки, используя наш маппинг
PgBulkInsert<Person> bulkInsert = new PgBulkInsert<>(new PersonMapping());

try (Connection conn = ...) {
    // Вызываем saveAll, передавая поток данных
    bulkInsert.saveAll(conn, people.stream());
}
```

### 💡 Полезные советы

*   **Потоковая передача**: `saveAll` может принимать `Iterable`, включая `Stream`. Это позволяет вставлять данные поточно, не загружая их все в память, что критически важно для очень больших объёмов.
*   **Обработка ошибок**: Строковые значения в PostgreSQL не могут содержать нуль-символы (`\u0000`). Метод `removeNullCharacters()` делает маппинг устойчивым к таким данным.
*   **Буферизация**: Регулируйте размер буфера методом `withBufferSize()`. Это может повлиять на производительность в зависимости от вашей сетевой инфраструктуры.

Если у вас будут другие вопросы по библиотеке или её использованию, обращайтесь.
