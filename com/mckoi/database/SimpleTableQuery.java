/**
 * com.mckoi.database.SimpleTableQuery  16 Oct 2001
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

import com.mckoi.util.IntegerVector;

/**
 * A simple convenience interface for querying a MutableTableDataSource
 * instance.  This is used as a very lightweight interface for changing a
 * table.  It is most useful for internal low level users of a database
 * table which doesn't need the overhead of the Mckoi table hierarchy
 * mechanism.
 *
 * @author Tobias Downer
 */

public final class SimpleTableQuery {

  /**
   * The DataTableDef for this table.
   */
  private DataTableDef table_def;

  /**
   * The MutableTableDataSource we are wrapping.
   */
  private MutableTableDataSource table;

  /**
   * Constructs the SimpleTableQuery with the given MutableTableDataSource
   * object.
   */
  public SimpleTableQuery(MutableTableDataSource in_table) {
    in_table.addRootLock();
    this.table = in_table;
    this.table_def = table.getDataTableDef();
  }

  /**
   * Returns a RowEnumeration that is used to iterate through the entire list
   * of valid rows in the table.
   */
  public RowEnumeration rowEnumeration() {
    return table.rowEnumeration();
  }

  /**
   * Gets the object (as a Java object) at the given cell in the table.
   * Returns null if the cell contains a null value.  Note that the offset
   * between one valid row and the next may not necessily be 1.  It is possible
   * for there to be gaps in the data.  For an iterator that returns
   * successive row indexes, use the 'rowEnumeration' method.
   */
  public Object get(int column, int row) {
    DataCell cell = table.getCellContents(column, row);
    return cell.getCell();
  }

  /**
   * Finds the index of all the rows in the table where the given column is
   * equal to the given object.
   * <p>
   * We assume value is not null, and it is either a BigDecimal to represent
   * a number, a String, a java.util.Date or a ByteLongObject.
   */
  public IntegerVector selectIndexesEqual(int column, Object value) {
    DataCell cell = DataCellFactory.fromObject(
                            table_def.columnAt(column).getDBType(), value);
    return table.getColumnScheme(column).selectEqual(cell);
  }

  /**
   * Finds the index of all the rows in the table where the given column is
   * equal to the given object for both of the clauses.  This implies an
   * AND for the two searches.
   * <p>
   * We assume value is not null, and it is either a BigDecimal to represent
   * a number, a String, a java.util.Date or a ByteLongObject.
   */
  public IntegerVector selectIndexesEqual(int col1, Object val1,
                                          int col2, Object val2) {
    DataCell cell1 = DataCellFactory.fromObject(
                            table_def.columnAt(col1).getDBType(), val1);
    DataCell cel12 = DataCellFactory.fromObject(
                            table_def.columnAt(col2).getDBType(), val2);

    // All the indexes that equal the first clause
    IntegerVector ivec = table.getColumnScheme(col1).selectEqual(cell1);

//    System.out.println(ivec);

    // From this, remove all the indexes that don't equals the second clause.
    int index = ivec.size() - 1;
    while (index >= 0) {
      // If the value in column 2 at this index is not equal to value then
      // remove it from the list and move to the next.
      if (!get(col2, ivec.intAt(index)).equals(val2)) {
        ivec.removeIntAt(index);
      }
      --index;
    }

    return ivec;
  }

  /**
   * Returns true if there is a single row in the table where the given column
   * is equal to the given value, otherwise returns false.  If there are 2 or
   * more rows an assertion exception is thrown.
   */
  public boolean existsSingle(int col, Object val) {
    IntegerVector ivec = selectIndexesEqual(col, val);
    if (ivec.size() == 0) {
      return false;
    }
    else if (ivec.size() == 1) {
      return true;
    }
    else {
      throw new Error("Assertion failed: existsSingle found multiple values.");
    }
  }

  /**
   * Assuming the table stores a key/value mapping, this returns the contents
   * of value_column for any rows where key_column is equal to the key_value.
   * An assertion exception is thrown if there is more than 2 rows that match
   * the key.  If no rows match the key then null is returned.
   */
  public Object getVar(int value_column, int key_column, Object key_value) {
    // All indexes in the table where the key value is found.
    IntegerVector ivec = selectIndexesEqual(key_column, key_value);
    if (ivec.size() > 1) {
      throw new Error("Assertion failed: getVar found multiple key values.");
    }
    else if (ivec.size() == 0) {
      // Key found so return the value
      return get(value_column, ivec.intAt(0));
    }
    else {
      // Key not found so return null
      return null;
    }
  }

  // ---------- Table mutable methods ---------

  /**
   * Adds a new key/value mapping in this table.  If the key already exists
   * the old key/value row is deleted first.  This method accepts two
   * arguments, the column that contains the key value, and an Object[] array
   * that is the list of cells to insert into the table.  The Object[] array
   * must be the size of the number of columns in this tbale.
   * <p>
   * NOTE: Change will come into effect globally at the next commit.
   * <p>
   * NOTE: This method must be assured of exlusive access to the table within
   *   the transaction.
   */
  public void setVar(int key_column, Object[] vals) {
    // All indexes in the table where the key value is found.
    IntegerVector ivec = selectIndexesEqual(key_column, vals[key_column]);
    if (ivec.size() > 1) {
      throw new Error("Assertion failed: setVar found multiple key values.");
    }
    else if (ivec.size() == 1) {
      // Remove the current key
      table.removeRow(ivec.intAt(0));
    }
    // Insert the new key
    RowData row_data = new RowData(table);
    for (int i = 0; i < table_def.columnCount(); ++i) {
      row_data.setColumnData(i, DataCellFactory.fromObject(
                             table_def.columnAt(i).getDBType(), vals[i]));
    }
    table.addRow(row_data);
  }

  /**
   * Deletes a single entry from the table where the given column equals the
   * given value.  If there are multiple values found an assertion exception
   * is thrown.  If a single value was found and deleted 'true' is returned
   * otherwise false.
   */
  public boolean deleteSingle(int col, Object val) {
    IntegerVector ivec = selectIndexesEqual(col, val);
    if (ivec.size() == 0) {
      return false;
    }
    else if (ivec.size() == 1) {
      table.removeRow(ivec.intAt(0));
      return true;
    }
    else {
      throw new Error("Assertion failed: deleteSingle found multiple values.");
    }
  }

  /**
   * Deletes all the given indexes in this table.
   */
  public void deleteRows(IntegerVector list) {
    for (int i = 0; i < list.size(); ++i) {
      table.removeRow(list.intAt(i));
    }
  }






  /**
   * Disposes this object and frees any resources associated with it.  This
   * should be called when the query object is no longer being used.
   */
  public void dispose() {
    if (table != null) {
      table.removeRootLock();
      table = null;
    }
  }

  /**
   * To be save we call dispose from the finalize method.
   */
  public void finalize() {
    dispose();
  }

}
