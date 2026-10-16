package org.openxdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

/**
 * T36 TLS parameter parsing unit tests (no server required).
 */
class OpenXDBSslConfigTest {

    @Test
    void defaultsToPlaintext() throws Exception {
        SslConfig c = OpenXDBDriver.parseSslConfig("jdbc:openxdb://127.0.0.1:7788", new Properties());
        assertFalse(c.ssl);
        assertEquals(SslConfig.MODE_DISABLE, c.mode);
    }

    @Test
    void parsesUrlQuery() throws Exception {
        String url = "jdbc:openxdb://db.internal:7788?ssl=true&sslMode=verify-full&trustStore=ca.pem&hostnameInCertificate=db.internal";
        SslConfig c = OpenXDBDriver.parseSslConfig(url, new Properties());
        assertTrue(c.ssl);
        assertEquals(SslConfig.MODE_VERIFY_FULL, c.mode);
        assertEquals("ca.pem", c.trustStore);
        assertEquals("db.internal", c.hostnameInCertificate);
    }

    @Test
    void propertiesAreFallback() throws Exception {
        Properties info = new Properties();
        info.setProperty("ssl", "true");
        info.setProperty("sslMode", "verify-ca");
        info.setProperty("trustStore", "C:/certs/ca.pem");
        SslConfig c = OpenXDBDriver.parseSslConfig("jdbc:openxdb://127.0.0.1:7788", info);
        assertTrue(c.ssl);
        assertEquals(SslConfig.MODE_VERIFY_CA, c.mode);
        assertEquals("C:/certs/ca.pem", c.trustStore);
    }

    @Test
    void rejectsInvalidSslMode() {
        assertThrows(SQLException.class, () -> OpenXDBDriver.parseSslConfig(
                "jdbc:openxdb://127.0.0.1:7788?ssl=true&sslMode=bogus", new Properties()));
    }

    @Test
    void verifyModesRequireTrustStore() {
        assertThrows(SQLException.class, () -> OpenXDBDriver.parseSslConfig(
                "jdbc:openxdb://127.0.0.1:7788?ssl=true&sslMode=verify-full", new Properties()));
        assertThrows(SQLException.class, () -> OpenXDBDriver.parseSslConfig(
                "jdbc:openxdb://127.0.0.1:7788?ssl=true&sslMode=verify-ca", new Properties()));
    }

    @Test
    void parseHostPortIgnoresQuery() throws Exception {
        String[] hp = OpenXDBDriver.parseHostPort("jdbc:openxdb://127.0.0.1:7788?ssl=true&sslMode=verify-full");
        assertEquals("127.0.0.1", hp[0]);
        assertEquals("7788", hp[1]);
    }
}
