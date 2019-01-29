/**
 * com.mckoi.database.jdbc.DatabaseInterface  15 Aug 2000
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

import java.sql.SQLException;

/**
 * The interface with the Database whether it be remotely via TCP/IP or
 * locally within the current JVM.
 *
 * @author Tobias Downer
 */

public interface DatabaseInterface {

  /**
   * Attempts to log in to the database as the given username with the given
   * password.  Only one user may be authenticated per connection.  This must
   * be called before the other methods are used.
   * <p>
   * A DatabaseCallBack implementation must be given here that is notified
   * of all events from the database.  Events are only received if the
   * login was successful.
   */
  boolean login(String default_schema, String username, String password,
                DatabaseCallBack call_back) throws SQLException;

  /**
   * Executes the query and returns a QueryResponse object that describes the
   * result of the query.  The QueryResponse object describes the number of
   * rows, describes the columns, etc.  This method will block until the query
   * has completed.  The QueryResponse can be used to obtain the 'result id'
   * variable that is used in subsequent queries to the engine to retrieve
   * the actual result of the query.
   */
  QueryResponse execQuery(SQLQuery sql) throws SQLException;

  /**
   * Returns a part of a result set.  The result set part is referenced via the
   * 'result id' found in the QueryResponse.  This is used to read parts
   * of the query once it has been found via 'execQuery'.
   * <p>
   * The returned List object contains the result requested.
   */
  ResultPart getResultPart(int result_id, int row_number, int row_count)
                                                          throws SQLException;

  /**
   * Disposes of a result of a query on the server.  This frees up server side
   * resources allocated to a query.  This should be called when the ResultSet
   * of a query closes.  We should try and use this method as soon as possible
   * because if frees locks on tables and allows deleted rows to be
   * reclaimed.
   */
  void disposeResult(int result_id) throws SQLException;

  /**
   * Called when the connection is disposed.  This will terminate the
   * connection if there is any connection to terminate.
   */
  void dispose() throws SQLException;

}
