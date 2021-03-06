/**
 * com.mckoi.database.FilterTable  13 Jul 2000
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
 * A table that is a filter for another table.  By default, all Table methods
 * are implemented to call the parent.  This class should be used when we
 * want to implement a Table filter of some kind.  For example, a filter
 * for specific columns, or even rows, etc.
 * <p>
 * NOTE: For efficiency reasons, this will store SelectableScheme objects
 *   generated by the parent like VirtualTable.
 *
 * @author Tobias Downer
 */

public class FilterTable extends Table {

  /**
   * The Table we are filtering the columns from.
   */
  protected Table parent;

  /**
   * The schemes to describe the entity relation in the given column.
   */
  private SelectableScheme[] column_scheme;

  /**
   * The Constructor.
   */
  public FilterTable(Table parent) {
    this.parent = parent;
  }

  /**
   * Returns the parent table.
   */
  protected Table getParent() {
    return parent;
  }

  /**
   * Returns the parent Database object.
   */
  public Database getDatabase() {
    return parent.getDatabase();
  }

  /**
   * Returns the number of columns in the table.
   */
  public int getColumnCount() {
    return parent.getColumnCount();
  }

  /**
   * Returns the number of rows stored in the table.
   */
  public int getRowCount() {
    return parent.getRowCount();
  }

  /**
   * Returns a list of all the fields within the table.  The list is ordered
   * the same way the fields were added in to the table.  BIG NOTE: The
   * names of the fields returned by this method do not contain any domain
   * information.
   */
  public TableField[] getFields() {
    return parent.getFields();
  }

  /**
   * Returns the field at the given column.  BIG NOTE: The names of the fields
   * returned by this method do not contain any domain information.
   */
  public TableField getFieldAt(int column) {
    return parent.getFieldAt(column);
  }

  /**
   * Given a field name, ie. 'CUSTOMER.CUSTOMERID' this will return the
   * column number the field is at.  Returns -1 if the field does not exist
   * in this table.
   *
   * @deprecated
   */
  public int findFieldName(String name) {
    return parent.findFieldName(name);
  }

  /**
   * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
   * this will return the column number the field is at.  Returns -1 if the
   * field does not exist in the table.
   */
  public int findFieldName(Variable v) {
    return parent.findFieldName(v);
  }

  /**
   * Returns the fully resolved name of the given column in this table.  This
   * must return the fully resolved string of the format
   *
   * @deprecated
   */
  public String getResolvedColumnName(int column) {
    return parent.getResolvedColumnName(column);
  }

  /**
   * Returns a fully qualified Variable object that represents the name of
   * the column at the given index.  For example,
   *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
   */
  public Variable getResolvedVariable(int column) {
    return parent.getResolvedVariable(column);
  }

  /**
   * Returns a SelectableScheme for the given column in the given VirtualTable
   * row domain.
   */
  SelectableScheme getSelectableSchemeFor(int column,
                                          int original_column, Table table) {

    if (column_scheme == null) {
      column_scheme = new SelectableScheme[parent.getColumnCount()];
    }

    // Is there a local scheme available?
    SelectableScheme scheme = column_scheme[column];
    if (scheme == null) {

      // If we are asking for the selectable schema of this table we must
      // tell the parent we are looking for its selectable scheme.
      Table t = table;
      if (table == this) {
        t = parent;
      }

      // Scheme is not cached in this table so ask the parent.
      scheme = parent.getSelectableSchemeFor(column, original_column, t);
      if (table == this) {
        column_scheme[column] = scheme;
      }

    }
    else {
      // If this has a cached scheme and we are in the correct domain then
      // return it.
      if (table == this) {
        return scheme;
      }
      else {
        // Otherwise we must calculate the subset of the scheme
        return scheme.getSubsetScheme(table, original_column);
      }
    }
    return scheme;
  }

  /**
   * Given a set, this trickles down through the Table hierarchy resolving
   * the given row_set to a form that the given ancestor understands.
   * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
   * down three levels and return a new 7 element set with the rows fully
   * resolved to the given ancestors domain.
   */
  void setToRowTableDomain(int column, IntegerVector row_set,
                           TableDataSource ancestor) {
    if (ancestor == this || ancestor == parent) {
      return;
    }
    else {
      parent.setToRowTableDomain(column, row_set, ancestor);
    }
  }

  /**
   * Return the list of DataTable and row sets that make up the raw information
   * in this table.
   */
  RawTableInformation resolveToRawTable(RawTableInformation info) {
    return parent.resolveToRawTable(info);
  }

  /**
   * Returns an object that represents the information in the given cell
   * in the table.  This will generally be an expensive algorithm, so calls
   * to it should be kept to a minimum.  Note that the offset between two
   * rows is not necessarily 1.
   */
  public DataCell getCellContents(int column, int row) {
    return parent.getCellContents(column, row);
  }

  /**
   * Compares the object to the object at the given cell in the table.  The
   * Object may only be one of the types allowed in the database. This will
   * generally be an expensive algorithm, so calls to it should be kept to a
   * minimum.  See the Note in the above method about row offsets.
   * Returns: LESS_THAN, GREATER_THAN, or EQUAL
   */
  public int compareCellTo(DataCell ob, int column, int row) {
    return parent.compareCellTo(ob, column, row);
  }

  /**
   * Returns an Enumeration of the rows in this table.
   * The Enumeration is a fast way of retrieving consequtive rows in the table.
   */
  public RowEnumeration rowEnumeration() {
    return parent.rowEnumeration();
  }

//  /**
//   * Returns an IntegerVector with a list of column numbers that are in this
//   * table.
//   */
//  boolean[] validColumns() {
//    return parent.validColumns();
//  }

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
    parent.addDataTableListener(listener);
  }

  /**
   * Removes a DataTableListener from the DataTable objects at the root of
   * this table tree hierarchy.  If this table represents the join of a
   * number of tables, then the DataTableListener is removed from all the
   * DataTable objects at the root.
   */
  void removeDataTableListener(DataTableListener listener) {
    parent.removeDataTableListener(listener);
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
    parent.lockRoot(lock_key);
  }

  /**
   * Unlocks the root tables so that the underlying rows may
   * once again be used if they are not locked and have been removed.  This
   * should be called some time after the rows have been locked.
   */
  public void unlockRoot(int lock_key) {
    parent.unlockRoot(lock_key);
  }

  /**
   * Returns true if the table has its row roots locked (via the lockRoot(int)
   * method.
   */
  public boolean hasRootsLocked() {
    return parent.hasRootsLocked();
  }



  /**
   * Prints a graph of the table hierarchy to the stream.
   */
  public void printGraph(java.io.PrintStream out, int indent) {
    for (int i = 0; i < indent; ++i) {
      out.print(' ');
    }
    out.println("F[" + getClass());

    parent.printGraph(out, indent + 2);

    for (int i = 0; i < indent; ++i) {
      out.print(' ');
    }
    out.println("]");
  }

}
