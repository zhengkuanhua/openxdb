# OpenXDB JDBC Driver

A pure-Java JDBC (java.sql) driver for OpenXDB speaking the `pkg/server` line
protocol directly (no third-party dependencies, Java 8+).

## URL

```
jdbc:openxdb://host:port
```

Port defaults to `7788` when omitted. User/password connection properties are
accepted and ignored — the OpenXDB wire protocol has no authentication yet
(see `docs/T19_client_drivers.md` for boundaries).

## Usage

```java
Class.forName("org.openxdb.jdbc.OpenXDBDriver"); // optional: static block registers it
try (Connection conn = DriverManager.getConnection("jdbc:openxdb://localhost:7788");
     Statement st = conn.createStatement()) {

    st.executeUpdate("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)");
    st.executeUpdate("INSERT INTO t VALUES (1, 'alice')");

    try (ResultSet rs = st.executeQuery("SELECT id, name FROM t")) {
        while (rs.next()) {
            System.out.println(rs.getInt("id") + " " + rs.getString("name"));
        }
    }

    // prepared statements: client-side literal binding
    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
        ps.setInt(1, 2);
        ps.setString(2, "bob");
        ps.executeUpdate();
    }

    // transactions
    conn.setAutoCommit(false);
    st.executeUpdate("INSERT INTO t VALUES (3, 'carol')");
    conn.commit();
}
```

## Build & test

Prerequisites: JDK 8+ and Maven 3.6+ (see `drivers/scripts/install_tools.ps1`).

```powershell
# from the repo root, build the server binary used by tests:
go build -o drivers/.bin/openxdb.exe ./cmd/openxdb   # or run the install script

cd drivers/jdbc
mvn test   # starts a real openxdb server on a random port with a temp data dir
mvn package  # produces target/openxdb-jdbc-0.10.0-alpha.jar
```

## Supported surface

- `DriverManager` registration (static block + `META-INF/services` not yet shipped)
- `Connection`: createStatement / prepareStatement / setAutoCommit(false) with
  BEGIN, commit / rollback (COMMIT / ROLLBACK), close, isValid (PING)
- `Statement`: execute / executeQuery / executeUpdate (+ update counts)
- `PreparedStatement`: client-side `?` binding for int/long/short/byte/float/double/
  BigDecimal/String/boolean/bytes/Date/Time/Timestamp; strings are escaped with SQL
  double-single-quote (`O''Brien`); bytes are sent as uppercase-hex literals
- `ResultSet`: forward-only, read-only; next / getString / getInt / getLong /
  getDouble / getFloat / getBoolean / getBigDecimal / getBytes / getObject /
  wasNull (empty cell ⇒ NULL)

## Boundaries (documented in T19)

- No authentication on the wire; user/password ignored.
- NULL parameters are not supported by the line protocol.
- No server-side prepared statements: binding happens client side.
- ResultSetMetaData / DatabaseMetaData / streams / blobs / clobs / savepoints /
  batching / generated keys / updatable result sets are not supported.
- String parameters containing LF/CR are rejected (line protocol constraint).
