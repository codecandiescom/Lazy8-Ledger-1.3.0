/**
 * com.mckoi.database.jdbcserver.AbstractJDBCDatabaseInterface  16 Mar 2002
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

package com.mckoi.database.jdbcserver;

import com.mckoi.database.*;
import com.mckoi.database.global.*;
import com.mckoi.database.interpret.Statement;
import com.mckoi.database.interpret.SQLQueryExecutor;
import com.mckoi.database.sql.SQL;
import com.mckoi.database.sql.ParseException;
import com.mckoi.database.jdbc.*;
import com.mckoi.util.IntegerVector;
import com.mckoi.util.StringUtil;
import com.mckoi.debug.*;

import java.sql.SQLException;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

/**
 * An abstract implementation of JDBCDatabaseInterface that provides a
 * connection between a single DatabaseConnection and a DatabaseInterface
 * implementation.
 * <p>
 * This receives database commands from the JDBC layer and dispatches the
 * queries to the database system.  It also manages ResultSet maps for query
 * results.
 * <p>
 * This implementation does not handle authentication (login) / construction
 * of the DatabaseConnection object, or disposing of the connection.
 * <p>
 * This implementation ignores the AUTO-COMMIT flag when a query is executed.
 * To implement AUTO-COMMIT, you should 'commit' after a command is executed.
 * <p>
 * SYNCHRONIZATION: This interface is NOT thread-safe.  To make a thread-safe
 *   implementation use the LockingMechanism.
 * <p>
 * See JDBCDatabaseInterface for a standard server-side implementation of this
 * class.
 *
 * @author Tobias Downer
 */

public abstract class AbstractJDBCDatabaseInterface
                                               implements DatabaseInterface {

  /**
   * The Databas object that represents the context of this
   * database interface.
   */
  private Database database;

  /**
   * The mapping that maps from result id number to Table object that this
   * JDBC connection is currently maintaining.
   * <p>
   * NOTE: All Table objects are now valid over a database shutdown + init.
   */
  private HashMap result_set_map;

  /**
   * This is incremented every time a result set is added to the map.  This
   * way, we always have a unique key on hand.
   */
  private int unique_result_id;

  /**
   * Access to information regarding the user logged in on this connection.
   * If no user is logged in, this is left as 'null'.  We can also use this to
   * retreive the Database object the user is logged into.
   */
  private User user = null;

  /**
   * The database connection transaction.
   */
  private DatabaseConnection database_connection;

  /**
   * The SQL parser object for this interface.  When a statement is being
   * parsed, this object is sychronized.
   */
  private SQLQueryExecutor sql_executor;
//  private SQL sql_parser;

  /**
   * Set to true when this database interface is disposed.
   */
  private boolean disposed;


  /**
   * Sets up the database interface.
   */
  public AbstractJDBCDatabaseInterface(Database database) {
    this.database = database;
    result_set_map = new HashMap();
    unique_result_id = 1;
    disposed = false;
  }

  // ---------- Utility methods ----------

  /**
   * Initializes this database interface with a User and DatabaseConnection
   * object.  This would typically be called from inside an authentication
   * method, or from 'login'.  This must be set before the object can be
   * used.
   */
  protected final void init(User user, DatabaseConnection connection) {
    this.user = user;
    this.database_connection = connection;
    // Set up the sql parser.
    sql_executor = new SQLQueryExecutor();
//    sql_parser = new SQL(new StringReader(""));
  }

  /**
   * Returns the Database that is the context of this interface.
   */
  protected final Database getDatabase() {
    return database;
  }

  /**
   * Returns the User object for this connection.
   */
  protected final User getUser() {
    return user;
  }

  /**
   * Returns a DebugLogger object that can be used to log debug messages
   * against.
   */
  public final DebugLogger Debug() {
    return getDatabase().Debug();
  }

  /**
   * Returns the DatabaseConnection objcet for this connection.
   */
  protected final DatabaseConnection getDatabaseConnection() {
    return database_connection;
  }

  /**
   * Adds this result set to the list of result sets being handled through
   * this processor.  Returns a number that unique identifies this result
   * set.
   */
  private int addResultSet(ResultSetInfo result) {
    // Lock the roots of the result set.
    result.lockRoot(-1);  // -1 because lock_key not implemented

    // Make a new result id
    int result_id;
    // This ensures this block can handle concurrent updates.
    synchronized (result_set_map) {
      result_id = ++unique_result_id;
      // Add the result to the map.
      result_set_map.put(new Integer(result_id), result);
    }

    return result_id;
  }

  /**
   * Gets the result set with the given result_id.
   */
  private ResultSetInfo getResultSet(int result_id) {
    synchronized (result_set_map) {
      return (ResultSetInfo) result_set_map.get(new Integer(result_id));
    }
  }

  /**
   * Disposes of the result set with the given result_id.  After this has
   * been called, the GC should garbage the table.
   */
  private void disposeResultSet(int result_id) {
    // Remove this entry.
    ResultSetInfo table;
    synchronized (result_set_map) {
      table = (ResultSetInfo) result_set_map.remove(new Integer(result_id));
    }
    if (table != null) {
      table.dispose();
    }
    else {
      Debug().write(Lvl.ERROR, this,
                  "Attempt to dispose invalid 'result_id'.");
    }
  }

  /**
   * Clears the contents of the result set map.  This removes all result_id
   * ResultSetInfo maps.
   */
  protected final void clearResultSetMap() {
    Iterator keys;
    ArrayList list;
    synchronized (result_set_map) {
      keys = result_set_map.keySet().iterator();

      list = new ArrayList();
      while (keys.hasNext()) {
        list.add(keys.next());
      }
    }
    keys = list.iterator();

    while (keys.hasNext()) {
      int result_id = ((Integer) keys.next()).intValue();
      disposeResultSet(result_id);
    }
  }

  /**
   * Wraps a Throwable thrown by the execution of a query in DatabaseConnection
   * with an SQLException and puts the appropriate error messages to the debug
   * log.
   */
  protected final SQLException handleExecuteThrowable(Throwable e,
                                                      SQLQuery query) {
    if (e instanceof ParseException) {

      Debug().writeException(Lvl.WARNING, e);

      // Parse exception when parsing the SQL.
      String msg = e.getMessage();
      msg = StringUtil.searchAndReplace(msg, "\r", "");
      return new MSQLException(msg, msg, 35, e);

    }
    else if (e instanceof TransactionException) {

      TransactionException te = (TransactionException) e;

      // Output query that was in error to debug log.
      Debug().write(Lvl.INFORMATION, this,
                  "Transaction error on: " + query);
      Debug().writeException(Lvl.INFORMATION, e);

      // Denotes a transaction exception.
      return new MSQLException(e.getMessage(), e.getMessage(),
                               200 + te.getType(), e);
    }
    else {

      // Output query that was in error to debug log.
      Debug().write(Lvl.WARNING, this,
                  "Exception thrown during query processing on: " + query);
      Debug().writeException(Lvl.WARNING, e);

      // Error, we need to return exception to client.
      return new MSQLException(e.getMessage(), e.getMessage(), 1, e);

    }

  }

  /**
   * Disposes all resources associated with this object.  This clears the
   * ResultSet map, and NULLs all references to help the garbage collector.
   * This method would normally be called from implementations of the
   * 'dispose' method.
   */
  protected final void internalDispose() {
    disposed = true;
    // Clear the result set mapping
    clearResultSetMap();
    user.close();
    user = null;
    database_connection = null;
    sql_executor = null;
  }

  // ---------- Implemented from DatabaseInterface ----------


  public QueryResponse execQuery(SQLQuery query) throws SQLException {

    if (disposed) {
      throw new RuntimeException("Interface is disposed.");
    }

    // Record the query start time
    long start_time = System.currentTimeMillis();
    // Where query result eventually resides.
    ResultSetInfo result_set_info;
    int result_id = -1;

    try {

//      // StatementTree caching
//
//      // Create a new parser and set the parameters...
//      String query_str = query.getQuery();
//      StatementTree statement_tree = null;
//      StatementCache statement_cache = getSystem().getStatementCache();
//
//      if (statement_cache != null) {
//        // Is this query cached?
//        statement_tree = statement_cache.get(query_str);
//      }
//      if (statement_tree == null) {
//        synchronized (sql_parser) {
//          sql_parser.ReInit(new StringReader(query_str));
//          sql_parser.reset();
//          // Parse the statement.
//          statement_tree = sql_parser.Statement();
//        }
//        // Put the statement tree in the cache
//        if (statement_cache != null) {
//          statement_cache.put(query_str, statement_tree);
//        }
//      }
//
//      // Substitute all parameter substitutions in the statement tree.
//      final Object[] vars = query.getVars();
//      ExpressionPreparer preparer = new ExpressionPreparer() {
//        public boolean canPrepare(Object element) {
//          return (element instanceof ParameterSubstitution);
//        }
//        public Object prepare(Object element) {
//          ParameterSubstitution ps = (ParameterSubstitution) element;
//          int param_id = ps.getID();
//          Object v = vars[param_id];
//          if (v == null) {
//            v = NullObject.NULL_OBJ;
//          }
//          return v;
//        }
//      };
//      statement_tree.prepareAllExpressions(preparer);
//
//      // Convert the StatementTree to a statement object
//      Statement statement;
//      String statement_class = statement_tree.getClassName();
//      try {
//        Class c = Class.forName(statement_class);
//        statement = (Statement) c.newInstance();
//      }
//      catch (ClassNotFoundException e) {
//        throw new SQLException(
//                      "Could not find statement class: " + statement_class);
//      }
//      catch (InstantiationException e) {
//        throw new SQLException(
//                      "Could not instantiate class: " + statement_class);
//      }
//      catch (IllegalAccessException e) {
//        throw new SQLException(
//                      "Could not access class: " + statement_class);
//      }
//
//
//      // Initialize the statement
//      statement.init(database_connection, statement_tree);
//
//      // Automated statement tree preparation
//      statement.resolveTree();
//
//      // Prepare the statement.
//      statement.prepare();
//
//      // Evaluate the SQL statement.
//      Table result = statement.evaluate();

      // Evaluate the sql query.
      Table result = sql_executor.execute(database_connection, query);

      // Put the result in the result cache...  This will lock this object
      // until it is removed from the result set cache.  Returns an id that
      // uniquely identifies this result set in future communication.
      // NOTE: This locks the roots of the table so that its contents
      //   may not be altered.
      result_set_info = new ResultSetInfo(query, result);
      result_id = addResultSet(result_set_info);

    }
    catch (Throwable e) {
      // If result_id set, then dispose the result set.
      if (result_id != -1) {
        disposeResultSet(result_id);
      }

      // Handle the throwable during query execution
      throw handleExecuteThrowable(e, query);

    }

//    catch (ParseException e) {
//
//      Debug().writeException(Lvl.WARNING, e);
//
//      // If result_id set, then dispose the result set.
//      if (result_id != -1) {
//        disposeResultSet(result_id);
//      }
//
//      // Parse exception when parsing the SQL.
//      String msg = e.getMessage();
//      msg = StringUtil.searchAndReplace(msg, "\r", "");
//      throw new MSQLException(msg, msg, 35, e);
//    }
//    catch (TransactionException e) {
//
//      // If result_id set, then dispose the result set.
//      if (result_id != -1) {
//        disposeResultSet(result_id);
//      }
//
//      // Output query that was in error to debug log.
//      Debug().write(Lvl.INFORMATION, this,
//                  "Transaction error on: " + query);
//      Debug().writeException(Lvl.INFORMATION, e);
//
//      // Denotes a transaction exception.
//      throw new MSQLException(e.getMessage(), e.getMessage(),
//                              200 + e.getType(), e);
//    }
//    catch (Throwable e) {
//
//      // If result_id set, then dispose the result set.
//      if (result_id != -1) {
//        disposeResultSet(result_id);
//      }
//
//      // Output query that was in error to debug log.
//      Debug().write(Lvl.WARNING, this,
//                  "Exception thrown during query processing on: " + query);
//      Debug().writeException(Lvl.WARNING, e);
//
//      // Error, we need to return exception to client.
//      throw new MSQLException(e.getMessage(), e.getMessage(), 1, e);
//    }

    // The time it took the query to execute.
    long taken = System.currentTimeMillis() - start_time;

    // Return the query response
    return new JDIQueryResponse(result_id, result_set_info, (int) taken, "");

  }


  public ResultPart getResultPart(int result_id, int row_number,
                                  int row_count) throws SQLException {
    if (disposed) {
      throw new RuntimeException("Interface is disposed.");
    }

    ResultSetInfo table = getResultSet(result_id);
    if (table == null) {
      throw new MSQLException("'result_id' invalid.", null, 4,
                              (Throwable) null);
    }

    int row_end = row_number + row_count;

    if (row_number < 0 || row_number >= table.getRowCount() ||
        row_end > table.getRowCount()) {
      throw new MSQLException("Result part out of range.", null, 4,
                              (Throwable) null);
    }

    try {
      int col_count = table.getColumnCount();
      ResultPart block = new ResultPart(row_count * col_count);
      for (int r = row_number; r < row_end; ++r) {
        for (int c = 0; c < col_count; ++c) {
          block.addElement(table.getCellContents(c, r).getCell());
        }
      }
      return block;
    }
    catch (Throwable e) {
      Debug().writeException(Lvl.WARNING, e);
      // If an exception was generated while getting the cell contents, then
      // throw an SQLException.
      throw new MSQLException(
          "Exception while reading results: " + e.getMessage(),
          e.getMessage(), 4, e);
    }

  }


  public void disposeResult(int result_id) throws SQLException {
    if (disposed) {
      throw new RuntimeException("Interface is disposed.");
    }

    disposeResultSet(result_id);
  }


  // ---------- Clean up ----------

  /**
   * Clean up if this object is GC'd.
   */
  public void finalize() throws Throwable {
    super.finalize();
    try {
      if (!disposed) {
        dispose();
      }
    }
    catch (Throwable e) { /* ignore this */ }
  }

  // ---------- Inner classes ----------

  /**
   * The response to a query.
   */
  private final static class JDIQueryResponse implements QueryResponse {

    int           result_id;
    ResultSetInfo result_set_info;
    int           query_time;
    String        warnings;

    JDIQueryResponse(int result_id, ResultSetInfo result_set_info,
                     int query_time, String warnings) {
      this.result_id = result_id;
      this.result_set_info = result_set_info;
      this.query_time = query_time;
      this.warnings = warnings;
    }

    public int getResultID() {
      return result_id;
    }

    public int getQueryTimeMillis() {
      return query_time;
    }

    public int getRowCount() {
      return result_set_info.getRowCount();
    }

    public int getColumnCount() {
      return result_set_info.getColumnCount();
    }

    public ColumnDescription getColumnDescription(int n) {
      return result_set_info.getFields()[n];
    }

    public String getWarnings() {
      return warnings;
    }

  }




  /**
   * Whenever a ResultSet is generated, this object contains the result set.
   * This class only allows calls to safe methods in Table.
   * <p>
   * NOTE: This is safe provided,
   *   a) The column topology doesn't change (NOTE: issues with ALTER command)
   *   b) Root locking prevents modification to rows.
   */
  private final static class ResultSetInfo {

    /**
     * The SQLQuery that was executed to produce this result.
     */
    private SQLQuery query;

    /**
     * The table that is the result.
     */
    private Table result;

    /**
     * A set of ColumnDescription that describes each column in the ResultSet.
     */
    private ColumnDescription[] col_desc;

    /**
     * IntegerVector that contains the row index into the table for each
     * row of the result.  For example, row.intAt(5) will return the row index
     * of 'result' of the 5th row item.
     */
    private IntegerVector row_index_map;

    /**
     * Incremented when we lock roots.
     */
    private int locked;

    /**
     * Constructs the result set.
     */
    ResultSetInfo(SQLQuery query, Table table) {
      this.query = query;
      this.result = table;

      // HACK: Read the contents of the first row so that we can pick up
      //   any errors with reading, and also to fix the 'uniquekey' bug
      //   that causes a new transaction to be started if 'uniquekey' is
      //   a column and the value is resolved later.
      RowEnumeration row_enum = table.rowEnumeration();
      if (row_enum.hasMoreRows()) {
        int row_index = row_enum.nextRowIndex();
        for (int c = 0; c < table.getColumnCount(); ++c) {
          table.getCellContents(c, row_index);
        }
      }
      row_enum = null;

      // Build 'row_index_map'
      row_index_map = new IntegerVector(table.getRowCount());
      RowEnumeration enum = table.rowEnumeration();
      while (enum.hasMoreRows()) {
        row_index_map.addInt(enum.nextRowIndex());
      }

      // This is a safe operation provides we are shared.
      // Copy all the TableField columns from the table to our own
      // ColumnDescription array, naming each column by what is returned from
      // the 'getResolvedVariable' method.
      final int col_count = table.getColumnCount();
      col_desc = new ColumnDescription[col_count];
      for (int i = 0; i < col_count; ++i) {
        Variable v = table.getResolvedVariable(i);
        String field_name;
        if (v.getTableName() == null) {
          // This means the column is an alias
          field_name = "@a" + v.getName();
        }
        else {
          // This means the column is an schema/table/column reference
          field_name = "@f" + v.toString();
        }
        col_desc[i] = new ColumnDescription(field_name, table.getFieldAt(i));
      }

      locked = 0;
    }

    /**
     * Returns the SQLQuery that was used to produce this result.
     */
    SQLQuery getSQLQuery() {
      return query;
    }

    /**
     * Disposes this object.
     */
    void dispose() {
      while (locked > 0) {
        unlockRoot(-1);
      }
      result = null;
      row_index_map = null;
      col_desc = null;
    }

    /**
     * Gets the cell contents of the cell at the given row/column.
     * <p>
     * Safe only if roots are locked.
     */
    DataCell getCellContents(int column, int row) {
      if (locked > 0) {
        int real_row = row_index_map.intAt(row);
        return result.getCellContents(column, real_row);
      }
      else {
        throw new Error("Table roots not locked!");
      }
    }

    /**
     * Returns the column count.
     */
    int getColumnCount() {
      return result.getColumnCount();
    }

    /**
     * Returns the row count.
     */
    int getRowCount() {
      return row_index_map.size();
    }

    /**
     * Returns the ColumnDescription array of all the columns in the result.
     */
    ColumnDescription[] getFields() {
      return col_desc;
    }

    /**
     * Locks the root of the result set.
     */
    void lockRoot(int key) {
      result.lockRoot(key);
      ++locked;
    }

    /**
     * Unlocks the root of the result set.
     */
    void unlockRoot(int key) {
      result.unlockRoot(key);
      --locked;
    }

  }

}
