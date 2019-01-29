/**
 * com.mckoi.database.QueryContext  05 Nov 2001
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
 * Facts about a particular query including the root table sources, constant
 * substitutions, etc.
 *
 * @author Tobias Downer
 */

public interface QueryContext {

  /**
   * Returns a TransactionSystem object that is used to determine information
   * about the transactional system.
   */
  TransactionSystem getSystem();

  /**
   * Returns a unique key for the given table source in the database.
   */
  long nextUniqueID(String name);

  /**
   * Returns the user name of the connection.
   */
  String getUserName();


  // ---------- Caching ----------

  /**
   * Marks a table in a query plan.
   */
  void addMarkedTable(String mark_name, Table table);

  /**
   * Returns a table that was marked in a query plan or null if no mark was
   * found.
   */
  Table getMarkedTable(String mark_name);

  /**
   * Put a Table into the cache.
   */
  void putCachedNode(long id, Table table);

  /**
   * Returns a cached table or null if it isn't cached.
   */
  Table getCachedNode(long id);

  /**
   * Clears the cache of any cached tables.
   */
  void clearCache();

}
