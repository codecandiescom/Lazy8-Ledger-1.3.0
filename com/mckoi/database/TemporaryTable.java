/**
 * com.mckoi.database.TemporaryTable  11 Apr 1998
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

import java.util.ArrayList;
import com.mckoi.util.IntegerVector;
import com.mckoi.debug.*;
import com.mckoi.database.global.TypeUtil;
import com.mckoi.database.global.NullObject;


/**
 * This class represents a temporary table that is built from data that is
 * not related to any underlying DataTable object from the database.
 * <p>
 * For example, an aggregate function generates data would be put into a
 * TemporaryTable.
 * <p>
 * @author Tobias Downer
 */

public final class TemporaryTable extends DefaultDataTable {

  /**
   * A Vector that represents the storage of DataCell[] arrays for each row
   * of the table.
   */
  private ArrayList table_storage;

  /**
   * The Constructor.
   */
  public TemporaryTable(Database database,
                        String name, TableField[] fields)
                                                    throws DatabaseException {
    super(database, name, fields);

    table_storage = new ArrayList();

  }

  /**
   * Constructs this TemporaryTable based on the fields from the given
   * Table object.
   */
  public TemporaryTable(String name, Table based_on)
                                                    throws DatabaseException {
    this(based_on.getDatabase(), name, based_on.getFields());
  }

  /**
   * Constructs this TemporaryTable based on the given Table object.
   */
  public TemporaryTable(DefaultDataTable based_on) throws DatabaseException {
    this(based_on.getDatabase(), based_on.getName(), based_on.getFields());
  }



  /* ====== Methods that are only for TemporaryTable interface ====== */

  /**
   * Resolves the given column name (eg 'id' or 'Customer.id' or
   * 'APP.Customer.id') to a column in this table.
   */
  private Variable resolveToVariable(String col_name) {
    Variable partial = Variable.resolve(col_name);
    return partial;
//    return partial.resolveTableName(TableName.resolve(getName()));
  }

  /**
   * Creates a new row where cells can be inserted into.
   */
  public void newRow() {
    table_storage.add(new DataCell[getColumnCount()]);
    ++row_count;
  }

  /**
   * Sets the cell in the given column / row to the given value.
   */
  public void setRowCell(DataCell cell, int column, int row) {
    DataCell[] cells = (DataCell[]) table_storage.get(row);
    cells[column] = cell;
  }

  /**
   * Sets the cell in the column of the last row of this table to the given
   * DataCell.
   */
  public void setRowCell(DataCell cell, String col_name) {
    Variable v = resolveToVariable(col_name);
    setRowCell(cell, findFieldName(v), row_count - 1);
  }

  /**
   * Sets the cell in the column of the last row of this table to the given
   * DataCell.
   */
  public void setRowObject(Object ob, int col_index, int row) {
    if (ob == null) {
      ob = NullObject.NULL_OBJ;
    }
    setRowCell(DataCellFactory.generateDataCell(getFieldAt(col_index), ob),
               col_index, row);
  }

  /**
   * Sets the cell in the column of the last row of this table to the given
   * DataCell.
   */
  public void setRowObject(Object ob, String col_name) {
    Variable v = resolveToVariable(col_name);
    setRowObject(ob, findFieldName(v));
  }

  /**
   * Sets the cell in the column of the last row of this table to the given
   * DataCell.
   */
  public void setRowObject(Object ob, int col_index) {
    setRowObject(ob, col_index, row_count - 1);
  }

  /**
   * Copies the cell from the given table (src_col, src_row) to the last row
   * of the column specified of this table.
   */
  public void setCellFrom(Table table, int src_col, int src_row,
                          String to_col) {
    Variable v = resolveToVariable(to_col);
    DataCell cell = table.getCellContents(src_col, src_row);
    setRowCell(cell, findFieldName(v), row_count - 1);
  }

  /**
   * Copies the contents of the row of the given Table onto the end of this
   * table.  Only copies columns that exist in both tables.
   */
  public void copyFrom(Table table, int row) {
    newRow();

    Variable[] vars = new Variable[table.getColumnCount()];
    for (int i = 0; i < vars.length; ++i) {
      vars[i] = table.getResolvedVariable(i);
    }

    for (int i = 0; i < getColumnCount(); ++i) {
      Variable v = getResolvedVariable(i);
      String col_name = v.getName();
      try {
        int tcol_index = -1;
        for (int n = 0; n < vars.length || tcol_index == -1; ++n) {
          if (vars[n].getName().equals(col_name)) {
            tcol_index = n;
          }
        }
        setRowCell(table.getCellContents(tcol_index, row), i, row_count - 1);
      }
      catch (Exception e) {
        Debug().writeException(e);
        throw new Error(e.getMessage());
      }
    }

  }





  /**
   * This should be called if you want to perform table operations on this
   * TemporaryTable.  It should be called *after* all the rows have been set.
   * It generates SelectableScheme object which sorts the columns of the table
   * and lets us execute Table operations on this table.
   * NOTE: After this method is called, the table must not change in any way.
   */
  public void setupAllSelectableSchemes() {
    blankSelectableSchemes(1);   // <- blind search
    for (int row_number = 0; row_number < row_count; ++row_number) {
      addRowToColumnSchemes(row_number);
    }
  }

  /* ====== Methods that are implemented for Table interface ====== */

  /**
   * Returns an object that represents the information in the given cell
   * in the table.  This can be used to obtain information about the given
   * table cells.
   */
  public DataCell getCellContents(int column, int row) {
    DataCell[] cells = (DataCell[]) table_storage.get(row);
    DataCell cell = cells[column];
    if (cell == null) {
      throw new Error("NULL cell!  (" + column + ", " + row + ")");
    }
    return cell;
  }

  /**
   * Compares the object to the object at the given cell in the table.  The
   * Object may only be one of the types allowed in the database.
   * Returns: LESS_THAN, GREATER_THAN, or EQUAL
   * Allowable types are: Long, Double, String, Time
   */
  public int compareCellTo(DataCell ob, int column, int row) {
    DataCell cell = getCellContents(column, row);
    int result = ob.compareTo(cell);
    if (result > 0) {
      return GREATER_THAN;
    }
    else if (result == 0) {
      return EQUAL;
    }
    return LESS_THAN;
  }

  /**
   * This inner class represents a Row Enumeration for this table.
   */
  class TTRowEnumeration implements RowEnumeration {
    int index = 0;

    public final boolean hasMoreRows() {
      return (index < row_count);
    }

    public final int nextRowIndex() {
      ++index;
      return index - 1;
    }
  }

  /**
   * Returns an Enumeration of the rows in this table.
   * Each call to 'nextRowIndex' returns the next valid row index in the table.
   */
  public RowEnumeration rowEnumeration() {
    return new TTRowEnumeration();
  }

  /**
   * Adds a DataTableListener to the DataTable objects at the root of this
   * table tree hierarchy.  If this table represents the join of a number of
   * tables then the DataTableListener is added to all the DataTable objects
   * at the root.
   * <p>
   * A DataTableListener is notified of all modifications to the raw entries
   * of the table.  This listener can be used for detecting changes in VIEWs,
   * for triggers or for caching of common queries.
   */
  void addDataTableListener(DataTableListener listener) {
    // Nothing to be notified on with a Temporary table...
  }

  /**
   * Removes a DataTableListener from the DataTable objects at the root of
   * this table tree hierarchy.  If this table represents the join of a
   * number of tables, then the DataTableListener is removed from all the
   * DataTable objects at the root.
   */
  void removeDataTableListener(DataTableListener listener) {
    // No listeners can be in a TemporaryTable.
  }

  /**
   * Locks the root table(s) of this table so that it is impossible to
   * overwrite the underlying rows that may appear in this table.
   * This is used when cells in the table need to be accessed 'outside' the
   * lock.  So we may have late access to cells in the table.
   * 'lock_key' is a given key that will also unlock the root table(s).
   * NOTE: This is nothing to do with the 'LockingMechanism' object.
   */
  public void lockRoot(int lock_key) {
    // We don't need to do anything for temporary tables, because they have
    // no root to lock.
  }

  /**
   * Unlocks the root tables so that the underlying rows may
   * once again be used if they are not locked and have been removed.  This
   * should be called some time after the rows have been locked.
   */
  public void unlockRoot(int lock_key) {
    // We don't need to do anything for temporary tables, because they have
    // no root to unlock.
  }

  /**
   * Returns true if the table has its row roots locked (via the lockRoot(int)
   * method.
   */
  public boolean hasRootsLocked() {
    // A temporary table _always_ has its roots locked.
    return true;
  }


  // ---------- Static convenience methods ----------

  /**
   * Creates a table with a single column with the given name and type.
   */
  static final TemporaryTable singleColumnTable(Database database,
                                                String col_name, Class c) {
    try {
      int type = TypeUtil.toDBType(c);
      TableField[] fields =
                 { new TableField(col_name, type, Integer.MAX_VALUE, false) };
      TemporaryTable table = new TemporaryTable(database, "single", fields);
      return table;
    }
    catch (DatabaseException e) {
      database.Debug().writeException(e);
      throw new Error(e.getMessage());
    }
  }

}
