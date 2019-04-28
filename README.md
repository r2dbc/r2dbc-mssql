# Reactive Relational Database Connectivity Microsoft SQL Server Implementation

This project contains the [Microsoft SQL Server][m] implementation of the [R2DBC SPI][r]. This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to

[m]: https://microsoft.com/sqlserver
[r]: https://github.com/r2dbc/r2dbc-spi

This driver provides the following features:

* Login with username/password with temporary SSL encryption
* Full SSL encryption support (for e.g. Azure usage).
* Transaction Control
* Simple execution of SQL batches (direct and cursored execution)
* Execution of parametrized statements (direct and cursored execution)
* Extensive type support (including `TEXT`, `VARCHAR(MAX)`, `IMAGE`, `VARBINARY(MAX)` and national variants, see below for exceptions)

Next steps:

* Execution of stored procedures 
* Add support for TVP and UDTs

## Maven
Both milestone and snapshot artifacts (library, source, and javadoc) can be found in Maven repositories.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-mssql</artifactId>
  <version>1.0.0.BUILD-SNAPSHOT</version>
</dependency>
```

Artifacts can bound found at the following repositories.

### Repositories
```xml
<repository>
    <id>spring-snapshots</id>
    <name>Spring Snapshots</name>
    <url>https://repo.spring.io/snapshot</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

```xml
<repository>
    <id>spring-milestones</id>
    <name>Spring Milestones</name>
    <url>https://repo.spring.io/milestone</url>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
```

## Usage

Connection Factory Discovery:

```java
ConnectionFactoryOptions options = builder()
    .option(DRIVER, "mssql")
    .option(HOST, "…")
    .option(PORT, …)  // optional, defaults to 1433
    .option(USER, "…")
    .option(PASSWORD, "…")
    .option(DATABASE, "…") // optional
    .option(SSL, true) // optional, defaults to false
    .option(Option.valueOf("applicationName"), "…") // optional
    .option(Option.valueOf("preferCursoredExecution"), true/false) // optional
    .option(Option.valueOf("connectionId"), new UUID(…)) // optional
    .build();

ConnectionFactory connectionFactory = ConnectionFactories.get(options);

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

Programmatic:

```java
MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
    .host("…")
    .username("…")
    .password("…")
    .database("…")
    .preferCursoredExecution(…)
    .build();

MssqlConnectionFactory factory = new MssqlConnectionFactory(configuration);

Mono<MssqlConnection> connectionMono = factory.create();
```

Microsoft SQL Server uses named parameters that are prefixed with `@`. The following SQL statement makes use of parameters:

```sql
INSERT INTO person (id, first_name, last_name) VALUES(@id, @firstname, @lastname)
```

Parameters are referenced without the `@` prefix when binding these:

```java
connection.createStatement("INSERT INTO person (id, first_name, last_name) VALUES(@id, @firstname, @lastname)")
            .bind("id", 1)
            .bind("firstname", "Walter")
            .bind("lastname", "White")
            .execute()
``` 

Binding also allows positional index (zero-based) references. The parameter index is derived from the parameter discovery order when parsing the query.

Supported ConnectionFactory Discovery Options:

Core options:

* `driver`: Must be `mssql`.
* `host`: Server hostname to connect to.
* `port`: Server port to connect to. Defaults to `1433`.
* `username`: Login username.
* `password`: Login password.

Additional options:

* `applicationName`: Name of the application. Defaults to driver name and version.
* `connectionId`: Connection Id for tracing purposes. Defaults to a random Id.
* `connectTimeout`: Connection Id for tracing purposes. Defaults to 30 seconds.
* `database`: Initial database to select. Defaults to SQL Server user profile settings.
* `ssl`: Whether to use transport-level encryption for the entire SQL server traffic, defaults to `false`.
* `preferCursoredExecution`: Whether to prefer cursors  or direct execution for queries. Uses by default direct. Cursors require more round-trips but are more backpressure-friendly. Defaults to direct execution. Can be `boolean` or a `Predicate<String>` accepting the SQL query.

### Data Type Mapping 

This reference table shows the type mapping between [Microsoft SQL Server][m] and Java data types:


| Microsoft SQL Server Type                 | Java Data Type                                                                                                                           | 
|:------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [`bit`][sql-bit-ref]                      | [**`Boolean`**][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref] |
| [`tinyint`][sql-all-int-ref]              | [**`Byte`**][java-byte-ref], [`Boolean`][java-boolean-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref] |
| [`smallint`][sql-all-int-ref]             | [**`Short`**][java-short-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref] |
| [`int`][sql-all-int-ref]                  | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref] |
| [`bigint`][sql-all-int-ref]               | [**`Long`**][java-long-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref] |
| [`real`][sql-float-real-ref]              | [**`Float`**][java-float-ref], [`Double`][java-double-ref]   
| [`float`][sql-float-real-ref]             | [**`Double`**][java-double-ref], [`Float`][java-float-ref] 
| [`decimal`][sql-decimal-ref]              | [`BigDecimal`][java-bigdecimal-ref] 
| [`numeric`][sql-decimal-ref]              | [`BigDecimal`][java-bigdecimal-ref]
| [`uniqueidentifier`][sql-uid-ref]         | [**`UUID`**][java-uuid-ref], [`String`][java-string-ref]   
| [`smalldatetime`][sql-smalldatetime-ref]  | [`LocalDateTime`][java-ldt-ref] 
| [`datetime`][sql-datetime-ref]            | [`LocalDateTime`][java-ldt-ref] 
| [`datetime2`][sql-datetime2-ref]          | [`LocalDateTime`][java-ldt-ref] 
| [`date`][sql-date-ref]                    | [`LocalDate`][java-ld-ref] 
| [`time`][sql-time-ref]                    | [`LocalTime`][java-lt-ref] 
| [`datetimeoffset`][sql-dtof-ref]          | [**`OffsetDateTime`**][java-odt-ref], [`ZonedDateTime`][java-zdt-ref]  
| [`timestamp`][sql-timestamp-ref]          | [`byte[]`][java-byte-ref]
| [`smallmoney`][sql-money-ref]             | [`BigDecimal`][java-bigdecimal-ref]
| [`money`][sql-money-ref]                  | [`BigDecimal`][java-bigdecimal-ref]
| [`char`][sql-(var)char-ref]               | [`String`][java-string-ref]
| [`varchar`][sql-(var)char-ref]            | [`String`][java-string-ref]
| [`varcharmax`][sql-(var)char-ref]         | [`String`][java-string-ref]
| [`nchar`][sql-n(var)char-ref]             | [`String`][java-string-ref]
| [`nvarchar`][sql-n(var)char-ref]          | [`String`][java-string-ref]
| [`nvarcharmax`][sql-n(var)char-ref]       | [`String`][java-string-ref]
| [`text`][sql-(n)text-ref]                 | [`String`][java-string-ref]
| [`ntext`][sql-(n)text-ref]                | [`String`][java-string-ref]
| [`image`][sql-(n)text-ref]                | [**`byte[]`**][java-byte-ref], [`ByteBuffer`][java-ByteBuffer-ref]
| [`binary`][sql-binary-ref]                | [**`byte[]`**][java-byte-ref], [`ByteBuffer`][java-ByteBuffer-ref]
| [`varbinary`][sql-binary-ref]             | [**`byte[]`**][java-byte-ref], [`ByteBuffer`][java-ByteBuffer-ref]
| [`varbinarymax`][sql-binary-ref]          | [**`byte[]`**][java-byte-ref], [`ByteBuffer`][java-ByteBuffer-ref]
| [`sql_variant`][sql-sql-variant-ref]      | Not yet supported.
| [`xml`][sql-xml-ref]                      | Not yet supported.
| [`udt`][sql-udt-ref]                      | Not yet supported.
| [`geometry`][sql-geometry-ref]            | Not yet supported.
| [`geography`][sql-geography-ref]          | Not yet supported.

Types in **bold** indicate the native (default) Java type.

**Note:** BLOB (`image`, `binary`, `varbinary` and `varbinary(max)`) and CLOB (`text`, `ntext`, `varchar(max)` and `nvarchar(max)`)
values are fully materialized in the client before decoding. Make sure to account for proper memory sizing.


[sql-bit-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql?view=sql-server-2017
[sql-all-int-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql?view=sql-server-2017
[sql-float-real-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-2017
[sql-decimal-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-2017
[sql-smalldatetime-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/smalldatetime-transact-sql?view=sql-server-2017
[sql-datetime-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-2017
[sql-datetime2-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-2017
[sql-date-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql?view=sql-server-2017
[sql-time-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/time-transact-sql?view=sql-server-2017
[sql-timestamp-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-2017
[sql-uid-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/uniqueidentifier-transact-sql?view=sql-server-2017
[sql-dtof-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-2017
[sql-money-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/money-and-smallmoney-transact-sql?view=sql-server-2017
[sql-(var)char-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-2017
[sql-n(var)char-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql?view=sql-server-2017
[sql-(n)text-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/ntext-text-and-image-transact-sql?view=sql-server-2017
[sql-binary-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-2017
[sql-sql-variant-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/sql-variant-transact-sql?view=sql-server-2017
[sql-xml-ref]: https://docs.microsoft.com/en-us/sql/t-sql/xml/xml-transact-sql?view=sql-server-2017
[sql-udt-ref]: https://docs.microsoft.com/en-us/sql/relational-databases/clr-integration-database-objects-user-defined-types/clr-user-defined-types?view=sql-server-2017
[sql-geometry-ref]: https://docs.microsoft.com/en-us/sql/t-sql/spatial-geometry/spatial-types-geometry-transact-sql?view=sql-server-2017
[sql-geography-ref]: https://docs.microsoft.com/en-us/sql/t-sql/spatial-geography/spatial-types-geography?view=sql-server-2017


[java-bigdecimal-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html
[java-boolean-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Boolean.html
[java-byte-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Byte.html
[java-ByteBuffer-ref]: https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
[java-double-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html
[java-float-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Float.html
[java-integer-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Integer.html
[java-long-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Long.html
[java-ldt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java-ld-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[java-lt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[java-odt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetDateTime.html
[java-short-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Short.html
[java-string-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/String.html
[java-uuid-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/UUID.html
[java-zdt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
