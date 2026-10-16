package org.openxdb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * OpenXDB JDBC driver. Register with {@code Class.forName("org.openxdb.jdbc.OpenXDBDriver")}
 * or rely on the static block / {@code META-INF/services/java.sql.Driver}.
 *
 * <p>Accepted URL form: {@code jdbc:openxdb://host:port} (port defaults to 7788).</p>
 *
 * <p>The driver speaks the OpenXDB pkg/server line protocol directly; there is no
 * authentication on the wire yet, so user/password properties are accepted and
 * ignored (see docs/T19_client_drivers.md for boundaries).</p>
 */
public class OpenXDBDriver implements Driver {

    static final String URL_PREFIX = "jdbc:openxdb://";
    private static final int DEFAULT_PORT = 7788;

    static {
        try {
            DriverManager.registerDriver(new OpenXDBDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public OpenXDBDriver() {
        // required public no-arg constructor for java.sql.Driver
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String[] hostPort = parseHostPort(url);
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        return new OpenXDBConnection(host, port, parseSslConfig(url, info));
    }

    /**
     * Parse T36 TLS parameters from the JDBC URL query string
     * ({@code ?ssl=true&sslMode=verify-full&trustStore=ca.pem&hostnameInCertificate=db}) or from
     * connection {@code Properties}. Mirrors the Python driver and pkg/server semantics.
     */
    static SslConfig parseSslConfig(String url, Properties info) throws SQLException {
        String query = "";
        int q = url.indexOf('?');
        if (q >= 0) {
            query = url.substring(q + 1);
        }
        boolean ssl = parseBool(getParam(query, info, "ssl", null), false);
        String mode = getParam(query, info, "sslMode", "disable");
        if (!SslConfig.MODE_DISABLE.equals(mode)
                && !SslConfig.MODE_VERIFY_CA.equals(mode)
                && !SslConfig.MODE_VERIFY_FULL.equals(mode)) {
            throw new SQLException("invalid sslMode: " + mode + " (expected disable|verify-ca|verify-full)");
        }
        String trustStore = getParam(query, info, "trustStore", null);
        String hostname = getParam(query, info, "hostnameInCertificate", null);
        if (ssl && (SslConfig.MODE_VERIFY_CA.equals(mode) || SslConfig.MODE_VERIFY_FULL.equals(mode))
                && (trustStore == null || trustStore.isEmpty())) {
            throw new SQLException("sslMode " + mode + " requires trustStore=<CA PEM path>");
        }
        return new SslConfig(ssl, mode, trustStore, hostname);
    }

    private static String getParam(String query, Properties info, String key, String dflt) {
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            if (k.equals(key)) {
                return eq < 0 ? "" : pair.substring(eq + 1);
            }
        }
        if (info != null && info.containsKey(key)) {
            return String.valueOf(info.getProperty(key));
        }
        return dflt;
    }

    private static boolean parseBool(String v, boolean dflt) throws SQLException {
        if (v == null || v.isEmpty()) {
            return dflt;
        }
        if ("true".equalsIgnoreCase(v) || "1".equals(v)) {
            return true;
        }
        if ("false".equalsIgnoreCase(v) || "0".equals(v)) {
            return false;
        }
        throw new SQLException("invalid boolean value: " + v);
    }

    /** Parse the host:port part after the scheme. Returns {host, port}. */
    static String[] parseHostPort(String url) throws SQLException {
        String rest = url.substring(URL_PREFIX.length());
        int query = rest.indexOf('?');
        if (query >= 0) {
            rest = rest.substring(0, query);
        }
        int slash = rest.indexOf('/');
        if (slash >= 0) {
            rest = rest.substring(0, slash);
        }
        int colon = rest.lastIndexOf(':');
        if (colon < 0) {
            return new String[]{rest.isEmpty() ? "localhost" : rest, String.valueOf(DEFAULT_PORT)};
        }
        String host = rest.substring(0, colon);
        String port = rest.substring(colon + 1);
        if (host.isEmpty()) {
            host = "localhost";
        }
        try {
            int p = Integer.parseInt(port);
            if (p < 1 || p > 65535) {
                throw new NumberFormatException(port);
            }
        } catch (NumberFormatException e) {
            throw new SQLException("invalid port in JDBC URL: " + url);
        }
        return new String[]{host, port};
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 10;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // no SQL standard compliance claim yet
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("parent logger not supported");
    }
}
