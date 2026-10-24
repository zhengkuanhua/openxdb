package org.openxdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * P2-12 row-protocol boundaries via JDBC: NULL vs empty string are
 * distinguishable on the wire ({@code \N} marker) and string values containing
 * newlines / carriage returns / backslashes round-trip losslessly through
 * {@code E'...'} escape-string literals.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenXDBRowProtoTest {

    private static Process server;
    private static Path dataDir;
    private static String jdbcUrl;
    private static int port;

    @BeforeAll
    void startServer() throws Exception {
        Class.forName("org.openxdb.jdbc.OpenXDBDriver");
        port = findFreePort();
        dataDir = Paths.get(System.getProperty("java.io.tmpdir"), "openxdb-jdbc-rp-" + UUID.randomUUID().toString());
        Files.createDirectories(dataDir);

        Path exe = findServerBinary();
        Process init = new ProcessBuilder(exe.toString(), "init", "--data-dir", dataDir.toString()).start();
        assertEquals(0, init.waitFor(), "openxdb init failed");

        ProcessBuilder start = new ProcessBuilder(exe.toString(), "start", "--data-dir", dataDir.toString(),
                "--port", String.valueOf(port));
        start.redirectErrorStream(true);
        server = start.start();
        for (int i = 0; i < 100; i++) {
            if (server.isAlive() && isPortOpen(port)) {
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(isPortOpen(port), "openxdb server did not become ready");
        jdbcUrl = "jdbc:openxdb://127.0.0.1:" + port;
    }

    @AfterAll
    void stopServer() throws Exception {
        if (server != null && server.isAlive()) {
            server.destroy();
            server.waitFor();
        }
        if (dataDir != null) {
            // best-effort cleanup
            deleteRecursive(dataDir.toFile());
        }
    }

    @Test
    void nullVsEmptyStringRoundtrip() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE t_rp (id INT, v TEXT, PRIMARY KEY (id))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t_rp (id, v) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "");
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.executeUpdate();

                ps.setInt(1, 3);
                ps.setObject(2, null);
                ps.executeUpdate();
            }
            try (ResultSet rs = st.executeQuery("SELECT id, v FROM t_rp ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("", rs.getString("v"));
                assertFalse(rs.wasNull(), "empty TEXT cell must NOT be NULL");

                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertNull(rs.getString("v"));
                assertTrue(rs.wasNull(), "\\N cell must be NULL");

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertNull(rs.getString("v"));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void newlineAndBackslashRoundtrip() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE t_esc (id INT, v TEXT, PRIMARY KEY (id))");
            String tricky = "line1\nline2\rc\\d'e|pipe\\tail";
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t_esc (id, v) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, tricky);
                ps.executeUpdate();
            }
            try (ResultSet rs = st.executeQuery("SELECT v FROM t_esc WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(tricky, rs.getString("v"));
                assertFalse(rs.wasNull());
            }
        }
    }

    private static boolean isPortOpen(int port) {
        try (ServerSocket s = new ServerSocket()) {
            s.bind(new java.net.InetSocketAddress(port));
            return false; // bind succeeded -> nothing was listening
        } catch (Exception e) {
            return true;
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static Path findServerBinary() {
        Path fromEnv = Paths.get(System.getProperty("openxdb.bin",
                "drivers/.bin/openxdb.exe"));
        if (Files.exists(fromEnv)) {
            return fromEnv;
        }
        // repo root fallback relative to the maven working dir
        Path root = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        Path cand = root.resolve("drivers/.bin/openxdb.exe");
        if (Files.exists(cand)) {
            return cand;
        }
        throw new IllegalStateException("openxdb server binary not found");
    }

    private static void deleteRecursive(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursive(c);
            }
        }
        f.delete();
    }
}
