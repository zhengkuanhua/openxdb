package org.openxdb.jdbc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(host, port), 5000);
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
