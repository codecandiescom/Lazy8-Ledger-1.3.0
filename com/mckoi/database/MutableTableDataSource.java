/**
 * com.mckoi.database.MutableTableDataSource  19 Nov 2000
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
 * A mutable data source that allows for the addition and removal of rows.
 *
 * @author Tobias Downer
 */

public interface MutableTableDataSource extends TableDataSource {

  /**
   * Adds a row to the source.  This will add a perminant record into the
   * the underlying data structure.  It will also update the indexing
   * schemes as appropriate, and also add the row into the set returned by
   * the 'rowEnumeration' iterator.
   * <p>
   * It returns a row index that is used to reference this data in future
   * queries.  Throws an exception if the row additional was not possible
   * because of IO reasons.
   */
  int addRow(RowData row_data);

  /**
   * Completely removes a row from the source.  This will perminantly remove
   * the record from the underlying data structure.  It also updates the
   * indexing schemes and removes the row index from the set returned by
   * the 'rowEnumeration' iterator.
   * <p>
   * Throws an exception if the row index does not reference a valid row within
   * the context of this data source.
   */
  void removeRow(int row_index);

  /**
   * Updates a row in the source.  This will make a perminant change to the
   * underlying data structure.  It will update the indexing schemes as
   * appropriate, and also add the row into the set returned by the
   * 'rowEnumeration' iterator.
   * <p>
   * It returns a row index for the new updated records.  Throws an exception
   * if the row update was not possible because of IO reasons or the row
   * index not being a valid reference to a record in this data source.
   */
  int updateRow(int row_index, RowData row_data);

  /**
   * Returns a journal that details the changes to this data source since it
   * was created.  This method may return a 'null' object to denote that no
   * logging is being done.  If this returns a MasterTableJournal, then all
   * 'addRow' and 'removeRow' method calls and their relative order will be
   * described in this journal.
   */
  MasterTableJournal getJournal();

  /**
   * Disposes this table data source.  After this method is called, most use
   * of this object is undefined, except for the 'getCellContent' and
   * 'compareCellContent' methods which are valid provided the source is
   * under a root lock.
   */
  void dispose();

  /**
   * Puts this source under a 'root lock'.  A root lock means the root row
   * structure of this object must not change.  A root lock is obtained on
   * a table when a ResultSet keeps hold of an object outside the life of
   * the transaction that created the table.  It is important that the order
   * of the rows stays constant (committed deleted rows are not really
   * deleted and reused, etc) while a table holds at least 1 root lock.
   */
  void addRootLock();

  /**
   * Removes a root lock from this source.
   */
  void removeRootLock();

}
