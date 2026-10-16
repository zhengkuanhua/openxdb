package org.openxdb.jdbc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Line-protocol client mirroring pkg/server.Server.Handle.
 *
 * <p>Wire format (each command is one UTF-8 line terminated by LF; replies are
 * line oriented too):</p>
 * <ul>
 *   <li>{@code OK} &mdash; simple acknowledgement (PING/BEGIN/COMMIT/ROLLBACK/QUIT)</li>
 *   <li>{@code (N rows affected)} &mdash; DDL/DML statements</li>
 *   <li>a text table: header line, separator line, data rows, {@code (N rows)}</li>
 *   <li>{@code ERR <message>} &mdash; error reply</li>
 * </ul>
 *
 * <p>This class deliberately mirrors the Python driver ({@code drivers/python})
 * so both drivers share the same framing and parsing rules.</p>
 */
final class OpenXDBProtocol {

    static final String ERR_PREFIX = "ERR ";

    /** Parsed reply of one command. */
    static final class Result {
        enum Kind { OK, AFFECTED, TABLE }

        final Kind kind;
        final int rowCount;
        final List<String> columns;
        final List<String[]> rows;

        private Result(Kind kind, int rowCount, List<String> columns, List<String[]> rows) {
            this.kind = kind;
            this.rowCount = rowCount;
            this.columns = columns;
            this.rows = rows;
        }

        static Result ok() {
            return new Result(Kind.OK, -1, Collections.<String>emptyList(), Collections.<String[]>emptyList());
        }

        static Result affected(int n) {
            return new Result(Kind.AFFECTED, n, Collections.<String>emptyList(), Collections.<String[]>emptyList());
        }

        static Result table(List<String> columns, List<String[]> rows) {
            return new Result(Kind.TABLE, -1, columns, rows);
        }
    }

    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;
    private boolean closed;

    OpenXDBProtocol(String host, int port) throws SQLException {
        this(host, port, null);
    }

    OpenXDBProtocol(String host, int port, SslConfig sslConfig) throws SQLException {
        try {
            Socket s;
            if (sslConfig != null && sslConfig.ssl) {
                SSLSocket ss = sslConfig.createSSLSocket(sslConfig.identityHost(host));
                ss.connect(new InetSocketAddress(host, port), 5000);
                ss.startHandshake();
                s = ss;
            } else {
                s = new Socket();
                s.connect(new InetSocketAddress(host, port), 5000);
            }
            s.setSoTimeout(0);
            this.socket = s;
            this.in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            this.out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new SQLException("cannot connect to openxdb://" + host + ":" + port + ": " + e.getMessage(), e);
        }
    }

    public synchronized void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            socket.close();
        } catch (IOException e) {
            throw new SQLException("error closing connection: " + e.getMessage(), e);
        }
    }

    boolean isClosed() {
        return closed;
    }

    /** Send a raw command line and parse the full reply. */
    synchronized Result command(String cmd) throws SQLException {
        if (closed) {
            throw new SQLException("connection is closed");
        }
        try {
            out.write(cmd);
            out.write("\n");
            out.flush();
        } catch (IOException e) {
            close();
            throw new SQLException("connection lost while sending command: " + e.getMessage(), e);
        }

        String first;
        try {
            first = in.readLine();
        } catch (IOException e) {
            close();
            throw new SQLException("connection lost while reading reply: " + e.getMessage(), e);
        }
        if (first == null) {
            close();
            throw new SQLException("connection closed by server (EOF)");
        }
        if (first.startsWith(ERR_PREFIX)) {
            throw new SQLException(first.substring(ERR_PREFIX.length()));
        }
        if (first.equals("OK")) {
            return Result.ok();
        }
        if (first.equals("PONG")) {
            return Result.ok();
        }
        if (first.equals("BYE")) {
            return Result.ok();
        }
        if (first.startsWith("(") && first.endsWith(" rows affected)")) {
            String inner = first.substring(1, first.length() - " rows affected)".length()).trim();
            try {
                return Result.affected(Integer.parseInt(inner));
            } catch (NumberFormatException e) {
                throw new SQLException("cannot parse affected-rows reply: " + first);
            }
        }
        // Text table: header | separator | data rows | (N rows)
        List<String> columns = splitRow(first);
        String separator;
        List<String[]> rows = new ArrayList<String[]>();
        try {
            separator = in.readLine(); // separator line, ignored
            if (separator == null) {
                close();
                throw new SQLException("connection closed by server (EOF in table)");
            }
            while (true) {
                String line = in.readLine();
                if (line == null) {
                    close();
                    throw new SQLException("connection closed by server (EOF in table)");
                }
                if (line.startsWith("(") && line.endsWith(" rows)")) {
                    break;
                }
                rows.add(splitRow(line).toArray(new String[0]));
            }
        } catch (IOException e) {
            close();
            throw new SQLException("connection lost while reading table: " + e.getMessage(), e);
        }
        return Result.table(columns, rows);
    }

    /** Split one table line into cells. Cells are separated by " | "; values
     *  containing "|" survive in the last column. The server pads cells to a
     *  fixed column width, so every cell is trimmed of alignment whitespace. */
    private static List<String> splitRow(String line) {
        List<String> cells = new ArrayList<String>();
        int n = line.split(" \\| ", -1).length;
        String rest = line;
        for (int i = 0; i < n - 1; i++) {
            int idx = rest.indexOf(" | ");
            if (idx < 0) {
                cells.add(rest.trim());
                rest = "";
                break;
            }
            cells.add(rest.substring(0, idx).trim());
            rest = rest.substring(idx + 3);
        }
        cells.add(rest.trim());
        while (cells.size() < n) {
            cells.add("");
        }
        return cells;
    }

    /** @return trimmed cell, or null for an empty cell (NULL on the wire). */
    static String cellValue(String cell) {
        String t = cell.trim();
        return t.isEmpty() ? null : t;
    }
}

/**
 * TLS configuration for the JDBC driver (T36). Parsed from the JDBC URL query
 * string ({@code ssl=true&sslMode=verify-full&trustStore=ca.pem}) or from
 * {@code Properties}. Modes mirror pkg/server and the Python driver:
 * <ul>
 *   <li>{@code disable} &mdash; plaintext (default) or TLS without verification when ssl=true</li>
 *   <li>{@code verify-ca} &mdash; verify the server certificate chain against trustStore</li>
 *   <li>{@code verify-full} &mdash; verify chain + hostname (hostnameInCertificate / host)</li>
 * </ul>
 */
final class SslConfig {
    static final String MODE_DISABLE = "disable";
    static final String MODE_VERIFY_CA = "verify-ca";
    static final String MODE_VERIFY_FULL = "verify-full";

    final boolean ssl;
    final String mode;
    final String trustStore;
    final String hostnameInCertificate;

    SslConfig(boolean ssl, String mode, String trustStore, String hostnameInCertificate) {
        this.ssl = ssl;
        this.mode = mode == null ? MODE_DISABLE : mode;
        this.trustStore = trustStore;
        this.hostnameInCertificate = hostnameInCertificate;
    }

    boolean verifyChain() {
        return MODE_VERIFY_CA.equals(mode) || MODE_VERIFY_FULL.equals(mode);
    }

    SSLSocket createSSLSocket(String identityHost) throws IOException {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            TrustManager[] tms = new TrustManager[]{verifyChain() ? new PemTrustManager(trustStore) : TRUST_ALL};
            ctx.init(null, tms, new SecureRandom());
            SSLSocketFactory f = ctx.getSocketFactory();
            SSLSocket s = (SSLSocket) f.createSocket();
            if (MODE_VERIFY_FULL.equals(mode) && identityHost != null && !identityHost.isEmpty()) {
                SSLParameters params = s.getSSLParameters();
                params.setEndpointIdentificationAlgorithm("HTTPS");
                try {
                    params.setServerNames(Collections.singletonList(new SNIHostName(identityHost)));
                } catch (IllegalArgumentException ignore) {
                    // non-DNS identity: hostname check still applies via endpoint id
                }
                s.setSSLParameters(params);
            }
            return s;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            IOException io = new IOException("cannot initialize TLS context: " + e.getMessage());
            io.initCause(e);
            throw io;
        }
    }

    /** @return hostname used for verify-full endpoint identification. */
    String identityHost(String connectHost) {
        return hostnameInCertificate == null || hostnameInCertificate.isEmpty()
                ? connectHost : hostnameInCertificate;
    }

    private static final X509TrustManager TRUST_ALL = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // no client certificates on the OpenXDB wire
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // ssl=true without sslMode=verify-*: accept any server certificate
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    /** Trust manager that pins the trustStore CA PEM file(s). */
    private static final class PemTrustManager implements X509TrustManager {
        private final X509Certificate[] cas;

        PemTrustManager(String trustStore) throws IOException {
            if (trustStore == null || trustStore.isEmpty()) {
                throw new IOException("sslMode verify-ca/verify-full requires trustStore=<CA PEM path>");
            }
            try {
                List<X509Certificate> certs = new ArrayList<X509Certificate>();
                try (FileInputStream in = new FileInputStream(trustStore)) {
                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    for (Certificate c : cf.generateCertificates(in)) {
                        if (c instanceof X509Certificate) {
                            certs.add((X509Certificate) c);
                        }
                    }
                }
                if (certs.isEmpty()) {
                    throw new IOException("no X.509 certificates found in trustStore: " + trustStore);
                }
                cas = certs.toArray(new X509Certificate[0]);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                IOException io = new IOException("cannot load trustStore " + trustStore + ": " + e.getMessage());
                io.initCause(e);
                throw io;
            }
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // no client certificates on the OpenXDB wire
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (chain == null || chain.length == 0) {
                throw new CertificateException("empty server certificate chain");
            }
            for (X509Certificate serverCert : chain) {
                for (X509Certificate ca : cas) {
                    if (serverCert.equals(ca)) {
                        return; // self-signed certificate pinned directly in trustStore
                    }
                }
            }
            for (X509Certificate ca : cas) {
                try {
                    chain[0].verify(ca.getPublicKey());
                    return; // chain signed by a pinned CA
                } catch (Exception ignored) {
                    // try next pinned CA
                }
            }
            throw new CertificateException("server certificate not trusted by trustStore");
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return cas.clone();
        }
    }
}
