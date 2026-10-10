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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Forward-only, read-only {@link ResultSet} backed by the table parsed from the
 * line protocol. Cell values arrive as text; empty cells map to SQL NULL.
 * {@code getString} returns the raw text, numeric getters parse the text.
 */
public class OpenXDBResultSet implements ResultSet {

    private final OpenXDBStatement statement;
    private final java.util.List<String> columns;
    private final java.util.List<String[]> rows;
    private int cursor = -1; // -1: before first
    private boolean wasNull = false;
    private boolean closed = false;

    OpenXDBResultSet(OpenXDBStatement statement, OpenXDBProtocol.Result res) {
        this.statement = statement;
        this.columns = res.columns;
        this.rows = res.rows;
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        if (cursor + 1 >= rows.size()) {
            cursor = rows.size();
            return false;
        }
        cursor++;
        return true;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        statement.resultClosed(this);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNull;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("column not found: " + columnLabel);
    }

    // ---- cell access ----------------------------------------------------------

    private String cell(int columnIndex) throws SQLException {
        checkOpen();
        if (cursor < 0 || cursor >= rows.size()) {
            throw new SQLException("result set is not positioned on a row (call next() first)");
        }
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new SQLException("column index out of range: " + columnIndex + " (1.." + columns.size() + ")");
        }
        String[] row = rows.get(cursor);
        String cell = columnIndex <= row.length ? row[columnIndex - 1] : "";
        String v = OpenXDBProtocol.cellValue(cell);
        wasNull = (v == null);
        return v;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return cell(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String v = cell(columnIndex);
        if (v == null) {
            return 0;
        }
        try {
            return (int) Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("cannot parse integer value: " + v);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String v = cell(columnIndex);
        if (v == null) {
            return 0L;
        }
        try {
            return (long) Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("cannot parse long value: " + v);
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String v = cell(columnIndex);
        if (v == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("cannot parse double value: " + v);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (float) getDouble(columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return (float) getDouble(columnLabel);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String v = cell(columnIndex);
        if (v == null) {
            return false;
        }
        String t = v.trim();
        return "1".equals(t) || "true".equalsIgnoreCase(t) || "TRUE".equals(t) || "t".equalsIgnoreCase(t);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) getInt(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) getInt(columnLabel);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getInt(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) getInt(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String v = cell(columnIndex);
        return v == null ? null : new BigDecimal(v.trim());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        return bd == null ? null : bd.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String v = cell(columnIndex);
        if (v == null) {
            return null;
        }
        String t = v.trim();
        // wire BLOB values are uppercase hex (value.go String()); decode when hex-like
        if (t.length() % 2 == 0 && t.matches("[0-9A-Fa-f]+")) {
            byte[] out = new byte[t.length() / 2];
            for (int i = 0; i < out.length; i++) {
                out[i] = (byte) Integer.parseInt(t.substring(i * 2, i * 2 + 2), 16);
            }
            return out;
        }
        return t.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return cell(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return convert(getObject(columnIndex), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return convert(getObject(columnLabel), type);
    }

    @SuppressWarnings("unchecked")
    private static <T> T convert(Object v, Class<T> type) throws SQLException {
        if (v == null) {
            return null;
        }
        if (type == String.class || type == Object.class) {
            return (T) v;
        }
        String t = v.toString().trim();
        try {
            if (type == Integer.class) {
                return (T) Integer.valueOf((int) Double.parseDouble(t));
            }
            if (type == Long.class) {
                return (T) Long.valueOf((long) Double.parseDouble(t));
            }
            if (type == Double.class) {
                return (T) Double.valueOf(t);
            }
            if (type == Float.class) {
                return (T) Float.valueOf(t);
            }
            if (type == BigDecimal.class) {
                return (T) new BigDecimal(t);
            }
            if (type == Boolean.class) {
                return (T) Boolean.valueOf("1".equals(t) || "true".equalsIgnoreCase(t));
            }
            if (type == byte[].class) {
                if (t.length() % 2 == 0 && t.matches("[0-9A-Fa-f]+")) {
                    byte[] out = new byte[t.length() / 2];
                    for (int i = 0; i < out.length; i++) {
                        out[i] = (byte) Integer.parseInt(t.substring(i * 2, i * 2 + 2), 16);
                    }
                    return (T) out;
                }
                return (T) t.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (NumberFormatException e) {
            throw new SQLException("cannot convert '" + v + "' to " + type.getName());
        }
        throw new SQLFeatureNotSupportedException("unsupported conversion target: " + type.getName());
    }

    // ---- cursor navigation -------------------------------------------------------

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkOpen();
        return cursor < 0 && rows.size() > 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkOpen();
        return cursor >= rows.size() && rows.size() > 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkOpen();
        return cursor == 0 && rows.size() > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkOpen();
        return cursor == rows.size() - 1 && rows.size() > 0;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkOpen();
        cursor = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkOpen();
        cursor = rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkOpen();
        cursor = rows.isEmpty() ? -1 : 0;
        return !rows.isEmpty();
    }

    @Override
    public boolean last() throws SQLException {
        checkOpen();
        cursor = rows.isEmpty() ? -1 : rows.size() - 1;
        return !rows.isEmpty();
    }

    @Override
    public int getRow() throws SQLException {
        checkOpen();
        return cursor < 0 || cursor >= rows.size() ? 0 : cursor + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkOpen();
        if (row == 0) {
            cursor = -1;
            return false;
        }
        int idx = row > 0 ? row - 1 : rows.size() + row;
        if (idx < 0 || idx >= rows.size()) {
            cursor = rows.size();
            return false;
        }
        cursor = idx;
        return true;
    }

    @Override
    public boolean relative(int rowsOff) throws SQLException {
        checkOpen();
        return absolute(cursor + 1 + rowsOff);
    }

    @Override
    public boolean previous() throws SQLException {
        checkOpen();
        if (cursor < 0) {
            return false;
        }
        cursor--;
        return cursor >= 0;
    }

    // ---- settings ---------------------------------------------------------------

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != FETCH_FORWARD) {
            throw new SQLException("only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        // hint only; the protocol returns the whole table at once
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw u("ResultSetMetaData is not supported");
    }

    @Override
    public String getCursorName() throws SQLException {
        throw u("named cursors are not supported");
    }

    // ---- generic helpers ----------------------------------------------------------

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("cannot unwrap to " + iface.getName());
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("result set is closed");
        }
    }

    private static SQLFeatureNotSupportedException u(String msg) {
        return new SQLFeatureNotSupportedException(msg);
    }

    // ---- unsupported getters / updaters ---------------------------------------------

    @Override public java.sql.Date getDate(int columnIndex) throws SQLException { throw u("Date is not supported"); }
    @Override public java.sql.Date getDate(String columnLabel) throws SQLException { throw u("Date is not supported"); }
    @Override public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException { throw u("Date is not supported"); }
    @Override public java.sql.Date getDate(String columnLabel, Calendar cal) throws SQLException { throw u("Date is not supported"); }
    @Override public Time getTime(int columnIndex) throws SQLException { throw u("Time is not supported"); }
    @Override public Time getTime(String columnLabel) throws SQLException { throw u("Time is not supported"); }
    @Override public Time getTime(int columnIndex, Calendar cal) throws SQLException { throw u("Time is not supported"); }
    @Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { throw u("Time is not supported"); }
    @Override public Timestamp getTimestamp(int columnIndex) throws SQLException { throw u("Timestamp is not supported"); }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { throw u("Timestamp is not supported"); }
    @Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { throw u("Timestamp is not supported"); }
    @Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { throw u("Timestamp is not supported"); }
    @Override public InputStream getAsciiStream(int columnIndex) throws SQLException { throw u("stream access is not supported"); }
    @Override public InputStream getAsciiStream(String columnLabel) throws SQLException { throw u("stream access is not supported"); }
    @Override @Deprecated public InputStream getUnicodeStream(int columnIndex) throws SQLException { throw u("stream access is not supported"); }
    @Override @Deprecated public InputStream getUnicodeStream(String columnLabel) throws SQLException { throw u("stream access is not supported"); }
    @Override public InputStream getBinaryStream(int columnIndex) throws SQLException { throw u("stream access is not supported"); }
    @Override public InputStream getBinaryStream(String columnLabel) throws SQLException { throw u("stream access is not supported"); }
    @Override public Reader getCharacterStream(int columnIndex) throws SQLException { throw u("stream access is not supported"); }
    @Override public Reader getCharacterStream(String columnLabel) throws SQLException { throw u("stream access is not supported"); }
    @Override public Ref getRef(int columnIndex) throws SQLException { throw u("Ref is not supported"); }
    @Override public Ref getRef(String columnLabel) throws SQLException { throw u("Ref is not supported"); }
    @Override public Blob getBlob(int columnIndex) throws SQLException { throw u("Blob is not supported"); }
    @Override public Blob getBlob(String columnLabel) throws SQLException { throw u("Blob is not supported"); }
    @Override public Clob getClob(int columnIndex) throws SQLException { throw u("Clob is not supported"); }
    @Override public Clob getClob(String columnLabel) throws SQLException { throw u("Clob is not supported"); }
    @Override public Array getArray(int columnIndex) throws SQLException { throw u("Array is not supported"); }
    @Override public Array getArray(String columnLabel) throws SQLException { throw u("Array is not supported"); }
    @Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { throw u("type maps are not supported"); }
    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { throw u("type maps are not supported"); }
    @Override public URL getURL(int columnIndex) throws SQLException { throw u("URL is not supported"); }
    @Override public URL getURL(String columnLabel) throws SQLException { throw u("URL is not supported"); }
    @Override public RowId getRowId(int columnIndex) throws SQLException { throw u("RowId is not supported"); }
    @Override public RowId getRowId(String columnLabel) throws SQLException { throw u("RowId is not supported"); }
    @Override public NClob getNClob(int columnIndex) throws SQLException { throw u("NClob is not supported"); }
    @Override public NClob getNClob(String columnLabel) throws SQLException { throw u("NClob is not supported"); }
    @Override public SQLXML getSQLXML(int columnIndex) throws SQLException { throw u("SQLXML is not supported"); }
    @Override public SQLXML getSQLXML(String columnLabel) throws SQLException { throw u("SQLXML is not supported"); }
    @Override public String getNString(int columnIndex) throws SQLException { return getString(columnIndex); }
    @Override public String getNString(String columnLabel) throws SQLException { return getString(columnLabel); }
    @Override public Reader getNCharacterStream(int columnIndex) throws SQLException { throw u("stream access is not supported"); }
    @Override public Reader getNCharacterStream(String columnLabel) throws SQLException { throw u("stream access is not supported"); }

    @Override public boolean rowUpdated() throws SQLException { return false; }
    @Override public boolean rowInserted() throws SQLException { return false; }
    @Override public boolean rowDeleted() throws SQLException { return false; }
    @Override public void insertRow() throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateRow() throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void deleteRow() throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void refreshRow() throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void cancelRowUpdates() throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void moveToInsertRow() throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void moveToCurrentRow() throws SQLException { throw u("updatable result sets are not supported"); }

    @Override public void updateNull(int columnIndex) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateByte(int columnIndex, byte x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateShort(int columnIndex, short x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateInt(int columnIndex, int x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateLong(int columnIndex, long x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateFloat(int columnIndex, float x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateDouble(int columnIndex, double x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateString(int columnIndex, String x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateDate(int columnIndex, Date x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateTime(int columnIndex, Time x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateObject(int columnIndex, Object x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNull(String columnLabel) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBoolean(String columnLabel, boolean x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateByte(String columnLabel, byte x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateShort(String columnLabel, short x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateInt(String columnLabel, int x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateLong(String columnLabel, long x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateFloat(String columnLabel, float x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateDouble(String columnLabel, double x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateString(String columnLabel, String x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBytes(String columnLabel, byte[] x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateDate(String columnLabel, Date x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateTime(String columnLabel, Time x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateObject(String columnLabel, Object x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateRef(int columnIndex, Ref x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateRef(String columnLabel, Ref x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBlob(int columnIndex, Blob x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBlob(String columnLabel, Blob x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateClob(int columnIndex, Clob x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateClob(String columnLabel, Clob x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateArray(int columnIndex, Array x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateArray(String columnLabel, Array x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateRowId(int columnIndex, RowId x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateRowId(String columnLabel, RowId x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNString(int columnIndex, String nString) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNString(String columnLabel, String nString) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNCharacterStream(String columnLabel, Reader x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateCharacterStream(String columnLabel, Reader x, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateCharacterStream(String columnLabel, Reader x) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateClob(int columnIndex, Reader reader) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNClob(int columnIndex, Reader reader) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateClob(String columnLabel, Reader reader) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw u("updatable result sets are not supported"); }
    @Override public void updateNClob(String columnLabel, Reader reader) throws SQLException { throw u("updatable result sets are not supported"); }
}
