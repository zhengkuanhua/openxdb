package org.openxdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * End-to-end tests for the pipelined batch API (OpenXDBStatement.addBatch /
 * executeBatch backed by OpenXDBProtocol.pipeline). A real openxdb server is
 * started once per class; every batch writes all statements in one burst and
 * reads replies in order, exercising the server REPLSession consecutive-line
 * path.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenXDBPipelineTest {

    private static Process server;
    private static Path dataDir;
    private static String jdbcUrl;

    @BeforeAll
    void startServer() throws Exception {
        Class.forName("org.openxdb.jdbc.OpenXDBDriver");
        int port = findFreePort();
        dataDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "openxdb-jdbc-pipe-" + UUID.randomUUID().toString());
        Files.createDirectories(dataDir);

        Path exe = findServerBinary();
        ProcessBuilder init = new ProcessBuilder(exe.toString(), "init",
                "--data-dir", dataDir.toString());
        Process initProc = init.start();
        assertEquals(0, initProc.waitFor(), "openxdb init failed");

        ProcessBuilder start = new ProcessBuilder(exe.toString(), "start",
                "--data-dir", dataDir.toString(), "--port", String.valueOf(port));
        start.redirectErrorStream(true);
        server = start.start();

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
            throw new IllegalStateException("openxdb server did not become ready on port " + port);
        }
        jdbcUrl = "jdbc:openxdb://127.0.0.1:" + port;
    }

    @AfterAll
    void stopServer() throws Exception {
        if (server != null && server.isAlive()) {
            server.destroy();
            server.waitFor();
        }
        if (dataDir != null) {
            deleteRecursively(dataDir.toFile());
        }
    }

    private static Path findServerBinary() throws Exception {
        String exeName = System.getProperty("os.name", "").toLowerCase().contains("win")
                ? "openxdb.exe" : "openxdb";
        Path abs = Paths.get(System.getProperty("user.dir"), "..", ".bin", exeName)
                .toAbsolutePath().normalize();
        if (!Files.exists(abs)) {
            throw new IllegalStateException("server binary not found: " + abs);
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

    @Test
    void batchPipelineMixedStatementsInOrder() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))");
            st.clearBatch();
            st.addBatch("INSERT INTO t_pipe VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            st.addBatch("SELECT id, v FROM t_pipe ORDER BY id");
            st.addBatch("UPDATE t_pipe SET v = 'x' WHERE id = 2");
            st.addBatch("INSERT INTO t_no_such_table_pipe VALUES (1)");
            st.addBatch("DELETE FROM t_pipe WHERE id = 1");

            BatchUpdateException thrown = null;
            try {
                st.executeBatch();
            } catch (BatchUpdateException e) {
                thrown = e;
            }
            assertTrue(thrown != null, "batch with a failing statement must throw BatchUpdateException");
            int[] counts = thrown.getUpdateCounts();
            assertEquals(3, counts[0], "INSERT affected rows");
            assertEquals(Statement.SUCCESS_NO_INFO, counts[1], "SELECT has no update count");
            assertEquals(1, counts[2], "UPDATE affected rows");
            assertTrue(thrown.getMessage().contains("t_no_such_table_pipe"),
                    "failure message names the failing statement");

            // Server-side ordering: DELETE ran after the error, row 1 is gone.
            try (ResultSet rs = st.executeQuery("SELECT id, v FROM t_pipe ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals("c", rs.getString(2));
                assertFalse(rs.next());
            }
        } finally {
            dropTable("t_pipe");
        }
    }

    @Test
    void batchPipelineUpdateCountsMatchSequential() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_btch (id INT, PRIMARY KEY (id))");
            st.addBatch("INSERT INTO t_btch VALUES (1)");
            st.addBatch("INSERT INTO t_btch VALUES (2)");
            st.addBatch("INSERT INTO t_btch VALUES (3)");
            assertArrayEquals(new int[]{1, 1, 1}, st.executeBatch());

            st.clearBatch();
            st.addBatch("DELETE FROM t_btch WHERE id = 2");
            assertArrayEquals(new int[]{1}, st.executeBatch());

            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_btch")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        } finally {
            dropTable("t_btch");
        }
    }

    @Test
    void batchPipelineMatchesSequentialSingle() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_seq (id INT, v TEXT, PRIMARY KEY (id))");
            st.executeUpdate("INSERT INTO t_seq VALUES (1, 'a')");
            st.addBatch("UPDATE t_seq SET v = 'x' WHERE id = 1");
            assertArrayEquals(new int[]{1}, st.executeBatch());
            try (ResultSet rs = st.executeQuery("SELECT v FROM t_seq WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("x", rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            dropTable("t_seq");
        }
    }

    @Test
    void executeBatchWithEmptyBatchIsNoOp() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertArrayEquals(new int[0], st.executeBatch());
        }
    }

    @Test
    void batchThroughputBenchmark() throws Exception {
        final int n = 60;
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE t_bench (id INT, v TEXT, PRIMARY KEY (id))");
            // warm-up
            st.addBatch("INSERT INTO t_bench VALUES (0, 'warm')");
            st.executeBatch();
            st.executeUpdate("DELETE FROM t_bench");

            long t0 = System.nanoTime();
            for (int i = 1; i <= n; i++) {
                st.executeUpdate("INSERT INTO t_bench VALUES (" + i + ", 'v" + i + "')");
            }
            long tSeq = System.nanoTime() - t0;

            st.executeUpdate("DELETE FROM t_bench");
            st.clearBatch();
            long t1 = System.nanoTime();
            for (int i = 1; i <= n; i++) {
                st.addBatch("INSERT INTO t_bench VALUES (" + i + ", 'v" + i + "')");
            }
            st.executeBatch();
            long tBatch = System.nanoTime() - t1;

            double seqMs = tSeq / 1_000_000.0;
            double batchMs = tBatch / 1_000_000.0;
            double speedup = seqMs / batchMs;
            System.out.println("[pipeline benchmark] " + n + " INSERTs: sequential="
                    + String.format("%.4f", seqMs) + "ms batch="
                    + String.format("%.4f", batchMs) + "ms speedup="
                    + String.format("%.1f", speedup) + "x");

            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_bench")) {
                assertTrue(rs.next());
                assertEquals(n, rs.getInt(1));
            }
            assertTrue(batchMs <= seqMs * 1.05 + 1.0,
                    "batched insert must not be slower than sequential round trips");
        } finally {
            dropTable("t_bench");
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
