/**
 * com.mckoi.database.jdbc.MPreparedStatement  22 Jul 2000
 *
 * Mckoi SQL Database ( http://www.mckoi.com/database )
 * Copyright (C) 2000, 2001  Diehl and Associates, Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * Version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License Version 2 for more details.
 *
 * You should have received a copy of the GNU General Public License
 * Version 2 along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * Change Log:
 * 
 * 
 */

package com.mckoi.database.jdbc;

import java.sql.*;
import java.io.*;
import java.util.Calendar;
import java.math.BigDecimal;
import com.mckoi.database.global.ByteLongObject;

/**
 * An implementation of a JDBC prepared statement.
 *
 * Multi-threaded issue:  This class is not designed to be multi-thread
 *  safe.  A PreparedStatement should not be accessed by concurrent threads.
 *
 * @author Tobias Downer
 */

class MPreparedStatement extends MStatement implements PreparedStatement {

  /**
   * The SQLQuery object constructed for this statement.
   */
  private SQLQuery statement;


  /**
   * Constructs the PreparedStatement.
   */
  MPreparedStatement(MConnection connection, String sql) {
    super(connection);
    statement = new SQLQuery(sql);
  }

  // ---------- Utility ----------

  /**
   * Converts the given Object to the given SQL type object.
   */
  Object convertToType(Object ob, int sqlType) throws SQLException {
    // PENDING, no conversion of Java ob to SQL types done currently...
    return ob;
  }


  // ---------- Implemented from PreparedStatement ----------

  public ResultSet executeQuery() throws SQLException {
    return executeQuery(statement);
  }

  public int executeUpdate() throws SQLException {
    MResultSet result_set = executeQuery(statement);
    return result_set.intValue();
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    statement.setVar(parameterIndex - 1, null);
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    statement.setVar(parameterIndex - 1, new Boolean(x));
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
//    statement.setVar(parameterIndex - 1, new BigDecimal(x));
    setLong(parameterIndex, x);
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
//    statement.setVar(parameterIndex - 1, new BigDecimal(x));
    setLong(parameterIndex, x);
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
//    statement.setVar(parameterIndex - 1, new BigDecimal(x));
    setLong(parameterIndex, x);
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
//    statement.setVar(parameterIndex - 1, new BigDecimal(x));
    statement.setVar(parameterIndex - 1, BigDecimal.valueOf(x));
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    statement.setVar(parameterIndex - 1, new BigDecimal(Float.toString(x)));
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    statement.setVar(parameterIndex - 1, new BigDecimal(Double.toString(x)));
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x)
                                                         throws SQLException {
    statement.setVar(parameterIndex - 1, x);
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    statement.setVar(parameterIndex - 1, x);
  }

  public void setBytes(int parameterIndex, byte x[]) throws SQLException {
    ByteLongObject b = new ByteLongObject(x);
    statement.setVar(parameterIndex - 1, b);
  }

  // JDBC Extension ...  Use java.util.Date as parameter
  public void extSetDate(int parameterIndex, java.util.Date x)
                                                         throws SQLException {
    statement.setVar(parameterIndex - 1, x);
  }


  public void setDate(int parameterIndex, java.sql.Date x)
                                                         throws SQLException {
    extSetDate(parameterIndex, new java.util.Date(x.getTime()));
  }

  public void setTime(int parameterIndex, java.sql.Time x)
                                                         throws SQLException {
    extSetDate(parameterIndex, new java.util.Date(x.getTime()));
  }

  public void setTimestamp(int parameterIndex, java.sql.Timestamp x)
                                                         throws SQLException {
    long time = x.getTime() + (x.getNanos() / 1000000);
    extSetDate(parameterIndex, new java.util.Date(time));
  }

  public void setAsciiStream(int parameterIndex, java.io.InputStream x,
                             int length) throws SQLException {
    // Fudge implementation since we fudged the result set version of this.
    // In an ideal world we'd open up a stream with the server and download
    // the information without having to collect all the data to transfer it.
    try {
      StringBuffer buf = new StringBuffer();
      int i = 0;
      while (i < length) {
        int c = x.read();
        if (c == -1) {
          throw new IOException(
                  "End of stream reached before length reached.");
        }
        buf.append((char) c);
        ++i;
      }
      setString(parameterIndex, new String(buf));
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new SQLException("IO Error: " + e.getMessage());
    }
  }

  /**
   * @deprecated
   */
  public void setUnicodeStream(int parameterIndex, java.io.InputStream x,
                               int length) throws SQLException {
    throw new SQLException("Deprecated method not supported");
  }

  public void setBinaryStream(int parameterIndex, java.io.InputStream x,
                              int length) throws SQLException {
    try {
      ByteLongObject b = new ByteLongObject(x, length);
      statement.setVar(parameterIndex - 1, b);
    }
    catch (IOException e) {
      throw new SQLException("IOException reading input stream: " +
                             e.getMessage());
    }
  }

  public void clearParameters() throws SQLException {
    statement.clear();
  }

  //----------------------------------------------------------------------
  // Advanced features:

  public void setObject(int parameterIndex, Object x, int targetSqlType,
                        int scale) throws SQLException {
    x = convertToType(x, targetSqlType);
    if (x instanceof BigDecimal) {
      x = ((BigDecimal) x).setScale(scale, BigDecimal.ROUND_HALF_UP);
    }
    setObject(parameterIndex, x);
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType)
                                                         throws SQLException {
    x = convertToType(x, targetSqlType);
    setObject(parameterIndex, x);
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    statement.setVar(parameterIndex - 1, x);
  }

  public boolean execute() throws SQLException {
    MResultSet result_set = executeQuery(statement);
    return !result_set.isUpdate();
  }

//#IFDEF(JDBC2.0)

  //--------------------------JDBC 2.0-----------------------------

  public void addBatch() throws SQLException {
    throw new SQLException("Not Supported");
  }

  public void setCharacterStream(int parameterIndex,
                                 java.io.Reader reader,
                                 int length) throws SQLException {
    // NOTE: The whole stream is read into a String and the 'setString' method
    //   is called.  This is inappropriate for long streams but probably
    //   won't be an issue any time in the future.
    StringBuffer buf = new StringBuffer();
    final int BUF_LENGTH = 1024;
    char[] char_buf = new char[BUF_LENGTH];
    try {
      while (length > 0) {
        int read = reader.read(char_buf, 0, Math.min(BUF_LENGTH, length));
        if (read > 0) {
          buf.append(char_buf, 0, read);
          length = length - read;
        }
        else {
          throw new SQLException("Premature end of Reader reached.");
        }
      }
    }
    catch (IOException e) {
      throw new SQLException("IOError: " + e.getMessage());
    }
    setString(parameterIndex, new String(buf));
  }

  public void setRef (int i, Ref x) throws SQLException {
    throw new SQLException("Not Supported");
  }

  public void setBlob (int i, Blob x) throws SQLException {
    // ISSUE: BLOBs transfer over to 'setBinaryStream' to handle the binary
    //   information.  The whole stream is copied into memory and then
    //   transfered to the database.  This is inappropriate if large BLOBs are
    //   being handled in which case the information should be split into
    //   smaller chunks and not consume so much resources.
    //   Of course, this is only an issue if dealing with massive BLOBs.  I'm
    //   not sure this will be an issue with the applications of our database.
    long len = x.length();
    if (len > 32768 * 65536) {
      throw new SQLException("BLOB > 2 gigabytes is too large.");
    }
    setBinaryStream(i, x.getBinaryStream(), (int) len);
  }

  public void setClob (int i, Clob x) throws SQLException {
    // ISSUE: CLOBs transfer over to 'setString' to handle the char array
    //   information.  The whole string is copied into memory and then
    //   transfered to the database.  This is inappropriate if large CLOBs are
    //   being handled in which case the information should be split into
    //   smaller chunks and not consume so much resources.
    //   Of course, this is only an issue if dealing with massive CLOBs.  I'm
    //   not sure this will be an issue with the applications of our database.
    long len = x.length();
    if (len > 32768 * 65536) {
      throw new SQLException("CLOB > 2 gigabytes is too large.");
    }
    setString(i, x.getSubString(0, (int) len));
  }

  public void setArray (int i, Array x) throws SQLException {
    throw new SQLException("Not Supported");
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    // TODO....
    throw new SQLException("Not Supported");
  }

  public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
                                                        throws SQLException {
    // Kludge...
    setDate(parameterIndex, x);
  }

  public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
                                                        throws SQLException {
    // Kludge...
    setTime(parameterIndex, x);
  }

  public void setTimestamp(int parameterIndex, java.sql.Timestamp x,
                           Calendar cal) throws SQLException {
    // Kludge...
    setTimestamp(parameterIndex, x);
  }

  public void setNull (int paramIndex, int sqlType, String typeName)
                                                        throws SQLException {
    // Kludge again...
    setNull(paramIndex, sqlType);
  }

//#ENDIF

//#IFDEF(JDBC3.0)

  // ---------- JDBC 3.0 ----------

  public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
    throw new SQLException("Not Supported");
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLException("Not Supported");
  }

//#ENDIF

  /**
   * For diagnostics.
   */
  public String toString() {
    return statement.toString();
  }

}
