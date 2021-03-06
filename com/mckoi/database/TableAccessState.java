/**
 * com.mckoi.database.TableAccessState  13 Sep 1998
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
 * This class provides very limited access to a Table object.  The purpose of
 * this object is to define the functionality of a table when the root table(s)
 * are locked via the 'Table.lockRoot(int)' method, and when the Table is no
 * longer READ or WRITE locked via the 'LockingMechanism' system.  During these
 * conditions, the table is in a semi-volatile state, so this class provides
 * a safe way to access the table without having to worry about using some
 * functionality of Table which isn't supported at this time.
 * <p>
 * @author Tobias Downer
 */

public final class TableAccessState {

  /**
   * The underlying Table object.
   */
  private Table table;

  /**
   * Set to true when the table is first locked.
   */
  private boolean been_locked;

  /**
   * The Constructor.
   */
  TableAccessState(Table table) {
    this.table = table;
    been_locked = false;
  }

  /**
   * Returns the cell at the given row/column coordinates in the table.
   * This method is valid because it doesn't use any of the SelectableScheme
   * information in any of its parent tables which could change at any time
   * when there is no READ or WRITE lock on the table.
   */
  public DataCell getCellContents(int column, int row) {
    return table.getCellContents(column, row);
  }

  /**
   * Returns the TableField object of the given column.
   * This information is constant per table.
   */
  public TableField getFieldAt(int column) {
    return table.getFieldAt(column);
  }

  /**
   * Returns a fully resolved name of the given column.
   */
  public String getResolvedColumnName(int column) {
    return table.getResolvedColumnName(column);
  }

  /**
   * Locks the root rows of the table.
   * This method is a bit of a HACK - why should the contract include being
   * able to lock the root rows?
   * This method only permits the roots to be locked once.
   */
  public void lockRoot(int key) {
    if (!been_locked) {
      table.lockRoot(key);
      been_locked = true;
    }
  }

  /**
   * Unlocks the root rows of the table.
   */
  public void unlockRoot(int key) {
    if (been_locked) { // && table.hasRootsLocked()) {
      table.unlockRoot(key);
      been_locked = false;
    }
    else {
      throw new RuntimeException("The root rows aren't locked.");
    }
  }

}
