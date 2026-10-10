package org.openxdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * OpenXDB {@link PreparedStatement}. Parameter binding is done <b>client side</b>:
 * the OpenXDB protocol has no prepared-statement / binary parameter channel, so
 * {@code ?} placeholders are substituted with safely escaped SQL literals before
 * the statement is sent (same escaping rules as the Python driver; see
 * docs/T19_client_drivers.md). NULL parameters are not supported on the wire.
 */
public class OpenXDBPreparedStatement extends OpenXDBStatement implements PreparedStatement {

    private final String sql;
    private final Object[] params;

    public OpenXDBPreparedStatement(OpenXDBConnection connection, String sql) {
        super(connection);
        this.sql = sql;
        this.params = new Object[countPlaceholders(sql)];
    }

    private static int countPlaceholders(String sql) {
        int count = 0;
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inString) {
                inString = true;
            } else if (c == '\'' && inString) {
                inString = false;
            } else if (c == '?' && !inString) {
                count++;
            }
        }
        return count;
    }

    private void setParam(int parameterIndex, Object value) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > params.length) {
            throw new SQLException("parameter index out of range: " + parameterIndex + " (1.." + params.length + ")");
        }
        params[parameterIndex - 1] = value;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("NULL parameters are not supported by the OpenXDB line protocol");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("NULL parameters are not supported by the OpenXDB line protocol");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, x ? "1" : "0");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, Double.toString(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        setParam(parameterIndex, x.toPlainString());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        if (x.indexOf('\n') >= 0 || x.indexOf('\r') >= 0) {
            throw new SQLException("string parameter containing newline is not supported by the OpenXDB line protocol");
        }
        setParam(parameterIndex, quote(x.replace("'", "''")));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        StringBuilder sb = new StringBuilder("'");
        for (byte b : x) {
            sb.append(String.format("%02X", b & 0xFF));
        }
        sb.append('\'');
        setParam(parameterIndex, sb.toString());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        setParam(parameterIndex, quote(x.toString()));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        setParam(parameterIndex, quote(x.toString()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        setParam(parameterIndex, quote(x.toString()));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("NULL parameters are not supported");
        }
        if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, ((Integer) x).intValue());
        } else if (x instanceof Long) {
            setLong(parameterIndex, ((Long) x).longValue());
        } else if (x instanceof Double) {
            setDouble(parameterIndex, ((Double) x).doubleValue());
        } else if (x instanceof Float) {
            setFloat(parameterIndex, ((Float) x).floatValue());
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, ((Boolean) x).booleanValue());
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else {
            throw new SQLFeatureNotSupportedException("unsupported parameter type: " + x.getClass().getName());
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    // ---- execution -------------------------------------------------------------

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(buildSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(buildSql());
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(buildSql());
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return executeLargeUpdate(buildSql());
    }

    private String buildSql() throws SQLException {
        checkOpen();
        StringBuilder out = new StringBuilder();
        int pi = 0;
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inString) {
                inString = true;
                out.append(c);
            } else if (c == '\'' && inString) {
                inString = false;
                out.append(c);
            } else if (c == '?' && !inString) {
                if (pi >= params.length) {
                    throw new SQLException("too many placeholders in SQL");
                }
                Object v = params[pi];
                if (v == null) {
                    throw new SQLException("parameter " + (pi + 1) + " is not bound");
                }
                out.append(v);
                pi++;
            } else {
                out.append(c);
            }
        }
        if (pi < params.length) {
            throw new SQLException("not all parameters were bound (" + params.length + " expected, " + pi + " set)");
        }
        return out.toString();
    }

    private static String quote(String value) {
        return "'" + value + "'";
    }

    // ---- unsupported / placeholder implementations ----------------------------------

    @Override
    public void clearParameters() throws SQLException {
        for (int i = 0; i < params.length; i++) {
            params[i] = null;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("ParameterMetaData is not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("REF parameters are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB parameters are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB parameters are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB parameters are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB parameters are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB parameters are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB parameters are not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ARRAY parameters are not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("stream parameters are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB parameters are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB parameters are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB parameters are not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML parameters are not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ROWID parameters are not supported");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL parameters are not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("batching is not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSetMetaData is not supported");
    }

    @Override
    public String toString() {
        return "OpenXDBPreparedStatement{" + sql + "}";
    }
}
