# Reactive Relational Database Connectivity Microsoft SQL Server Implementation

This project contains the [Microsoft SQL Server][m] implementation of the [R2DBC SPI][r]. This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to

[m]: http://microsoft.com/sqlserver
[r]: https://github.com/r2dbc/r2dbc-spi

Current state of this project is:

* Login
* Transaction Control
* Simple (un-cursored) execution of SQL batches
* Read support for all data types except binary types (BLOB)

Next steps:

* Error handling for failed logins
* Execution of SQL cursored statements
* Execution of prepared statements
* Add encoding for remaining codecs
* Execution of stored procedures 
* Support for null-bit compression (NBCROW)
* Batch support (`Connection.createBatch`)
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

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
