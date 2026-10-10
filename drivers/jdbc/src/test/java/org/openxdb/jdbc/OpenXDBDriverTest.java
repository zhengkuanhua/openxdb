package org.openxdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * End-to-end tests: start a real openxdb server (drivers/.bin/openxdb.exe) on a
 * random port with a temporary data dir, then exercise the JDBC driver against it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenXDBDriverTest {

    private static Process server;
    private static Path dataDir;
    private static String jdbcUrl;
    private static int port;

    @BeforeAll
    void startServer() throws Exception {
        Class.forName("org.openxdb.jdbc.OpenXDBDriver");
        port = findFreePort();
        dataDir = Paths.get(System.getProperty("java.io.tmpdir"), "openxdb-jdbc-" + UUID.randomUUID().toString());
        Files.createDirectories(dataDir);

        Path exe = findServerBinary();
        ProcessBuilder init = new ProcessBuilder(exe.toString(), "init", "--data-dir", dataDir.toString());
        Process initProc = init.start();
        assertEquals(0, initProc.waitFor(), "openxdb init failed");

        ProcessBuilder start = new ProcessBuilder(exe.toString(), "start", "--data-dir", dataDir.toString(),
                "--port", String.valueOf(port));
        start.redirectErrorStream(true);
        server = start.start();

        // wait until the port accepts connections
        boolean up = false;
        for (int i = 0; i < 100; i++) {
            if (!server.isAlive()) {
                break;
            }
            try (Socket s = new Socket("127.0.0.1", port)) {
                up = true;
                break;
            } catch (Exception ignored) {
                Thread.sleep(200);
            }
        }
        if (!up) {
            throw new IllegalStateException("openxdb server did not become ready on port " + port
                    + (server != null && !server.isAlive() ? " (process exited)" : ""));
        }
        jdbcUrl = "jdbc:openxdb://127.0.0.1:" + port;
    }

    @AfterAll
    void stopServer() throws Exception {
        if (server != null && server.isAlive()) {
            server.destroy();
            server.waitFor();
        }
        // best-effort cleanup of the temp data dir
        if (dataDir != null) {
            deleteRecursively(dataDir.toFile());
        }
    }

    private static Path findServerBinary() throws Exception {
        String exeName = System.getProperty("os.name", "").toLowerCase().contains("win") ? "openxdb.exe" : "openxdb";
        Path candidates = Paths.get(System.getProperty("user.dir"), "..", ".bin", exeName);
        Path abs = candidates.toAbsolutePath().normalize();
        if (!Files.exists(abs)) {
            throw new IllegalStateException("server binary not found: " + abs
                    + " (run drivers/scripts/build_server.ps1 or the install script first)");
        }
        return abs;
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    private static void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursively(c);
            }
        }
        f.delete();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    // ---- tests ---------------------------------------------------------------

    @Test
    void driverRegistersAndConnects() throws Exception {
        assertNotNull(DriverManager.getDriver(jdbcUrl), "driver should be registered via static block");
        try (Connection c = connect()) {
            assertNotNull(c);
            assertFalse(c.isClosed());
            assertTrue(c.isValid(2), "PING should succeed");
        }
    }

    @Test
    void executeQueryIteratesRows() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_query (id INT, name TEXT, score DECIMAL, PRIMARY KEY (id))");
            st.executeUpdate("INSERT INTO t_query VALUES (1, 'alice', 9.5), (2, 'bob', 8.25)");
            try (ResultSet rs = st.executeQuery("SELECT id, name, score FROM t_query ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("alice", rs.getString("name"));
                assertEquals("9.5000", rs.getString("score"));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals("bob", rs.getString(2));
                assertFalse(rs.next(), "no third row");
            }
        } finally {
            dropTable("t_query");
        }
    }

    @Test
    void executeUpdateReturnsRowCounts() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_upd (id INT, v TEXT, PRIMARY KEY (id))");
            assertEquals(2, st.executeUpdate("INSERT INTO t_upd VALUES (1, 'a'), (2, 'b')"));
            assertEquals(1, st.executeUpdate("UPDATE t_upd SET v = 'x' WHERE id = 1"));
            assertEquals(1, st.executeUpdate("DELETE FROM t_upd WHERE id = 2"));
            assertEquals(0, st.executeUpdate("DELETE FROM t_upd WHERE id = 999"));
        } finally {
            dropTable("t_upd");
        }
    }

    @Test
    void transactionCommitAndRollback() throws Exception {
        try (Connection c = connect()) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE t_txn (id INT, PRIMARY KEY (id))");

                // rollback path
                c.setAutoCommit(false);
                st.executeUpdate("INSERT INTO t_txn VALUES (1)");
                c.rollback();
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_txn")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "row must be gone after rollback");
                }

                // commit path
                st.executeUpdate("INSERT INTO t_txn VALUES (2)");
                c.commit();
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_txn")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1), "row must persist after commit");
                }
                c.setAutoCommit(true);
            } finally {
                dropTable("t_txn");
            }
        }
    }

    @Test
    void prepareStatementBindsParameters() throws Exception {
        try (Connection c = connect()) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE t_prep (id INT, name TEXT, score DECIMAL, data BLOB, PRIMARY KEY (id))");
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO t_prep VALUES (?, ?, ?, ?)")) {
                ps.setInt(1, 10);
                ps.setString(2, "O'Brien's \"quoted\" name");
                ps.setBigDecimal(3, new java.math.BigDecimal("7.25"));
                ps.setBytes(4, new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT id, name, score, data FROM t_prep WHERE id = ?")) {
                ps.setInt(1, 10);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(10, rs.getInt("id"));
                    assertEquals("O'Brien's \"quoted\" name", rs.getString("name"));
                    assertEquals("7.2500", rs.getString("score"));
                    assertEquals(4, rs.getBytes("data").length);
                    assertEquals(0xDE, rs.getBytes("data")[0] & 0xFF);
                    assertFalse(rs.next());
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT name FROM t_prep WHERE id = ?")) {
                ps.setInt(1, 999);
                try (ResultSet rs = ps.executeQuery()) {
                    assertFalse(rs.next(), "no row for missing id");
                }
            }
            dropTable("t_prep");
        }
    }

    @Test
    void nullAndMissingValuesAreVisible() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_null (id INT, note TEXT, PRIMARY KEY (id))");
            st.executeUpdate("INSERT INTO t_null VALUES (1, '')");
            try (ResultSet rs = st.executeQuery("SELECT note FROM t_null WHERE id = 1")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1), "NULL cell should map to null");
                assertTrue(rs.wasNull());
            }
        } finally {
            dropTable("t_null");
        }
    }

    @Test
    void errorsSurfaceAsSQLException() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertThrows(SQLException.class, () -> st.executeQuery("SELECT * FROM t_no_such_table_xyz"));
        }
    }

    private void dropTable(String table) throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("DROP TABLE " + table);
        } catch (SQLException ignored) {
            // table may already be gone
        }
    }
}
