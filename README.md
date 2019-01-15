# Reactive Relational Database Connectivity Microsoft SQL Server Implementation

This project contains the [Microsoft SQL Server][m] implementation of the [R2DBC SPI][r]. This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to

[m]: http://microsoft.com/sqlserver
[r]: https://github.com/r2dbc/r2dbc-spi

This driver provides the following features:

* Login with username/password with temporary SSL encryption
* Full SSL encryption support (for e.g. Azure usage).
* Transaction Control
* Simple (un-cursored) execution of SQL batches
* Execution of prepared statements
* Execution of SQL cursored statements
* Read support for all data types except binary types (BLOB)

Next steps:

* Add encoding for remaining codecs (VARBINARY, XML, UDT)
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
    .option(Option.valueOf("connectionId"), new UUID(…)) // optional
    .build();

ConnectionFactory connectionFactory = ConnectionFactories.get(options);

Mono<Connection> connectionMono = factory.create();
```

Programmatic:

```java
MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
    .host("…")
    .username("…")
    .password("…")
    .database("…")
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

* `driver`: Must be `mysql`. Mandatory.
* `host`: Server hostname to connect to. Mandatory.
* `port`: Server port to connect to. Defaults to `1433` if not set.
* `username`: Login username. Mandatory.
* `password`: Login password. Mandatory.
* `database`: Initial database to select. Defaults to SQL Server user profile settings if not set.
* `ssl`: Whether to use transport-level encryption for the entire SQL server traffic, defaults to `false`.
* `applicationName`: Name of the application. Defaults to driver name and version if not set.
* `connectionId`: Connection Id for tracing purposes. Random Id if not set.

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
