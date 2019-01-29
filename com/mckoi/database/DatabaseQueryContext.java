/**
 * com.mckoi.database.DatabaseQueryContext  25 Mar 2002
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

package com.mckoi.database;

/**
 * An implementation of a QueryContext based on a DatabaseConnection object.
 *
 * @author Tobias Downer
 */

public class DatabaseQueryContext extends AbstractQueryContext {

  /**
   * The DatabaseConnection.
   */
  private DatabaseConnection database;

  /**
   * Constructs the QueryContext.
   */
  public DatabaseQueryContext(DatabaseConnection database) {
    this.database = database;
  }

  /**
   * Returns the Database object that this context is a child of.
   */
  public Database getDatabase() {
    return database.getDatabase();
  }

  /**
   * Returns a TransactionSystem object that is used to determine information
   * about the transactional system.
   */
  public TransactionSystem getSystem() {
    return getDatabase().getSystem();
  }

  /**
   * Returns a DataTable from the database with the given table name.
   */
  public DataTable getTable(TableName name) {
    database.addSelectedFromTable(name);
    return database.getTable(name);
  }

  /**
   * Returns a unique key for the given table source in the database.
   */
  public long nextUniqueID(String name) {
    return database.nextUniqueID(name);
  }

  /**
   * Returns the user name of the connection.
   */
  public String getUserName() {
    return database.getUser().getUserName();
  }

}
