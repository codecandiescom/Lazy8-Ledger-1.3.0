/**
 * com.mckoi.database.jdbc.MStatement  20 Jul 2000
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

/**
 * An implementation of JDBC Statement.
 * <p>
 * Multi-threaded issue:  This class is not designed to be multi-thread
 *  safe.  A Statement should not be accessed by concurrent threads.
 *
 * @author Tobias Downer
 */

class MStatement implements Statement {

  /**
   * The MConnection object for this statement.
   */
  private MConnection connection;

  /**
   * The MResultSet object that this statement sends it's results to.
   */
  private MResultSet result_set;

  private int max_field_size;
  private int max_row_count;
  private int query_timeout;
  private int fetch_size;

  private SQLWarning head_warning;

  private boolean escape_processing;

//  private Object warning_lock = new Object();

  /**
   * For multiple result sets, the index of the result set we are currently on.
   */
  private int multi_result_set_index;

  /**
   * Constructs the statement.
   */
  MStatement(MConnection connection) {
    this.connection = connection;
    this.escape_processing = true;
  }

  /**
   * Adds a new SQLWarning to the chain.
   */
  final void addSQLWarning(SQLWarning warning) {
    if (head_warning == null) {
      head_warning = warning;
    }
    else {
      head_warning.setNextWarning(warning);
    }
  }

  /**
   * Returns the ResultSet object for this statement.
   */
  final MResultSet internalResultSet() {
    if (result_set == null) {
      result_set = new MResultSet(connection, this);
    }
    return result_set;
  }


  /**
   * Executes the given SQLQuery object and fill's in at most the top 10
   * entries of the result set.
   */
  protected MResultSet executeQuery(SQLQuery query) throws SQLException {
    // Prepare the query by doing any JDBC escape substitutions.
    query.prepare(escape_processing);

    // Process the query
    MResultSet result_set = internalResultSet();
    multi_result_set_index = 0;
    result_set.closeCurrentResult();
    connection.executeQuery(query, result_set);
    result_set.setFetchSize(fetch_size);
    result_set.setMaxRowCount(max_row_count);
    // If the result row count < 40 then download and store locally in the
    // result set and dispose the resources on the server.
    if (result_set.rowCount() < 40) {
      result_set.storeResultLocally();
    }
    else {
      result_set.updateResultPart(0, 10);
    }
    return result_set;
  }

  // ---------- Implemented from Statement ----------

  public ResultSet executeQuery(String sql) throws SQLException {
    return executeQuery(new SQLQuery(sql));
  }

  public int executeUpdate(String sql) throws SQLException {
    MResultSet result_set = executeQuery(new SQLQuery(sql));
    return result_set.intValue();  // Throws SQL error if not 1 col 1 row
  }

  public void close() throws SQLException {
    // Behaviour of calls to Statement undefined after this method finishes.
    if (result_set != null) {
      internalResultSet().dispose();
      result_set = null;
    }
  }

  //----------------------------------------------------------------------

  public int getMaxFieldSize() throws SQLException {
    // Are there limitations here?  Strings can be any size...
    return max_field_size;
  }

  public void setMaxFieldSize(int max) throws SQLException {
    if (max >= 0) {
      max_field_size = max;
    }
    else {
      throw new SQLException("MaxFieldSize negative.");
    }
  }

  public int getMaxRows() throws SQLException {
    return max_row_count;
  }

  public void setMaxRows(int max) throws SQLException {
    if (max >= 0) {
      max_row_count = max;
    }
    else {
      throw new SQLException("MaxRows negative.");
    }
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    escape_processing = enable;
  }

  public int getQueryTimeout() throws SQLException {
    return query_timeout;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds >= 0) {
      query_timeout = seconds;
      // Hack: We set the global query timeout for the driver in this VM.
      //   This global value is used in RemoteDatabaseInterface.
      //
      //   This is a nasty 'global change' hack.  A developer may wish to
      //   set a long timeout for one statement and a short for a different
      //   one however the timeout for all queries will be the very last time
      //   out set by any statement.  Unfortunately to fix this problem, we'll
      //   need to make a revision to the DatabaseInterface interface.   I
      //   don't think this is worth doing because I don't see this as being a
      //   major limitation of the driver.
      MDriver.QUERY_TIMEOUT = seconds;
    }
    else {
      throw new SQLException("Negative query timout.");
    }
  }

  public void cancel() throws SQLException {
    connection.disposeResult(internalResultSet().getResultID());
  }

  public SQLWarning getWarnings() throws SQLException {
    return head_warning;
  }

  public void clearWarnings() throws SQLException {
    head_warning = null;
  }

  public void setCursorName(String name) throws SQLException {
    // Cursors not supported...
  }
        
  //----------------------- Multiple Results --------------------------

  // NOTE: Mckoi database doesn't support multiple result sets.  I think multi-
  //   result sets are pretty nasty anyway - are they really necessary?
  //   We do support the 'Multiple Results' interface for 1 result set.

  public boolean execute(String sql) throws SQLException {
    MResultSet result_set = executeQuery(new SQLQuery(sql));
    return !result_set.isUpdate();
  }
        
  public ResultSet getResultSet() throws SQLException {
    if (multi_result_set_index == 0) {
      return internalResultSet();
    }
    else {
      return null;
    }
  }

  public int getUpdateCount() throws SQLException {
    if (multi_result_set_index == 0) {
      if (internalResultSet().isUpdate()) {
        return internalResultSet().intValue();
      }
    }
    return -1;
  }

  public boolean getMoreResults() throws SQLException {
    // Move to the next result set.
    ++multi_result_set_index;
//    // Close the current result set
//    close();
    // There's only ever 1 result set.
    return false;
  }

  //--------------------------JDBC 2.0-----------------------------

  // NOTE: These methods are provided as extensions for the JDBC 1.0 driver.

  public void setFetchSize(int rows) throws SQLException {
    if (rows >= 0) {
      fetch_size = rows;
    }
    else {
      throw new SQLException("Negative fetch size.");
    }
  }

  public int getFetchSize() throws SQLException {
    return fetch_size;
  }

//#IFDEF(JDBC2.0)

  public void setFetchDirection(int direction) throws SQLException {
    // We could use this hint to improve cache hits.....
  }

  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_UNKNOWN;
  }

  public int getResultSetConcurrency() throws SQLException {
    // Read only I'm afraid...
    return ResultSet.CONCUR_READ_ONLY;
  }

  public int getResultSetType()  throws SQLException {
    // Scroll insensitive operation only...
    return ResultSet.TYPE_SCROLL_INSENSITIVE;
  }

  public void addBatch( String sql ) throws SQLException {
    throw new SQLException("Pending implementation.");
  }

  public void clearBatch() throws SQLException {
    throw new SQLException("Pending implementation.");
  }

  public int[] executeBatch() throws SQLException {
    throw new SQLException("Pending implementation.");
  }

  public Connection getConnection()  throws SQLException {
    return connection;
  }

//#ENDIF

//#IFDEF(JDBC3.0)

  //--------------------------JDBC 3.0-----------------------------

  public boolean getMoreResults(int current) throws SQLException {
    return getMoreResults();
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException("Not Supported");
  }

  public int executeUpdate(String sql, int autoGeneratedKeys)
                                                        throws SQLException {
    throw new SQLException("Not Supported");
  }

  public int executeUpdate(String sql, int columnIndexes[])
                                                        throws SQLException {
    throw new SQLException("Not Supported");
  }

  public int executeUpdate(String sql, String columnNames[])
                                                        throws SQLException {
    throw new SQLException("Not Supported");
  }

  public boolean execute(String sql, int autoGeneratedKeys)
                                                        throws SQLException {
    throw new SQLException("Not Supported");
  }

  public boolean execute(String sql, int columnIndexes[])
                                                        throws SQLException {
    throw new SQLException("Not Supported");
  }

  public boolean execute(String sql, String columnNames[])
                                                        throws SQLException {
    throw new SQLException("Not Supported");
  }

  public int getResultSetHoldability() throws SQLException {
    // In Mckoi, all cursors may be held over commit.
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

//#ENDIF


  // ---------- Finalize ----------

  /**
   * The statement will close when it is garbage collected.
   */
  public void finalize() {
    try {
      close();
    }
    catch (SQLException e) { /* ignore */ }
  }

}
