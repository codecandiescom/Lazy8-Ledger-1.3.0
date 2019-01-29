/**
 * com.mckoi.database.SelectableScheme  12 Mar 1998
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
import com.mckoi.util.BlockIntegerList;
import com.mckoi.debug.DebugLogger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Represents a base class for a mechanism to select ranges from a given set.
 * Such schemes could include BinaryTree, Hashtable or just a blind search.
 * <p>
 * A given element in the set is specified through a 'row' integer whose
 * contents can be obtained through the 'table.getCellContents(column, row)'.
 * Every scheme is given a table and column number that the set refers to.
 * While a given set element is refered to as a 'row', the integer is really
 * only a pointer into the set list which can be de-referenced with a call to
 * table.getCellContents(row).  Better performance schemes will keep such
 * calls to a minimum.
 * <p>
 * A scheme may choose to retain knowledge about a given element when it is
 * added or removed from the set, such as a BinaryTree that catalogs all
 * elements with respect to each other.
 * <p>
 * @author Tobias Downer
 */

public abstract class SelectableScheme implements java.io.Serializable {

  /**
   * The table data source with the column this scheme indexes.
   */
  private final TableDataSource table;

  /**
   * The column number in the tree this tree helps.
   */
  private final int column;

  /**
   * Set to true if this scheme is immutable (can't be changed).
   */
  private boolean immutable = false;

//  /**
//   * Set to true if this scheme is closed and invalidated.
//   */
//  private boolean closed = false;

  /**
   * The constructor for all schemes.
   */
  public SelectableScheme(TableDataSource table, int column) {
    this.table = table;
    this.column = column;
  }

  /**
   * Returns the Table.
   */
  protected final TableDataSource getTable() {
    return table;
  }

  /**
   * Returns the global transaction system.
   */
  protected final TransactionSystem getSystem() {
    return table.getSystem();
  }

  /**
   * Returns the DebugLogger object to log debug messages to.
   */
  protected final DebugLogger Debug() {
    return getSystem().Debug();
  }

  /**
   * Returns the column this scheme is indexing in the table.
   */
  protected final int getColumn() {
    return column;
  }

  /**
   * Obtains the given cell in the row from the table.
   */
  protected final DataCell getCellContents(int row) {
    return table.getCellContents(column, row);
  }

  /**
   * Compares the given cell to the cell in the given row.
   */
  protected final int compareCellTo(DataCell ob, int row) {
    return table.compareCellTo(ob, column, row);
  }

  /**
   * Sets this scheme to immutable.
   */
  public final void setImmutable() {
    immutable = true;
  }

  /**
   * Returns true if this scheme is immutable.
   */
  public final boolean isImmutable() {
    return immutable;
  }

  /**
   * Diagnostic information.
   */
  public String toString() {
    // Name of the table
    String table_name;
    if (table instanceof DefaultDataTable) {
      table_name = ((DefaultDataTable) table).getName();
    }
    else {
      table_name = "VirtualTable";
    }

    StringBuffer buf = new StringBuffer();
    buf.append("[ SelectableScheme ");
    buf.append(super.toString());
    buf.append(" for table: ");
    buf.append(table_name);
    buf.append("]");

    return new String(buf);
  }

  /**
   * Writes the entire contents of the scheme to an OutputStream object.
   */
  public abstract void writeTo(OutputStream out) throws IOException;

  /**
   * Reads the entire contents of the scheme from a InputStream object.  If the
   * scheme is full of any information it throws an exception.
   */
  public abstract void readFrom(InputStream in) throws IOException;

  /**
   * Returns an exact copy of this scheme including any optimization
   * information.  The copied scheme is identical to the original but does not
   * share any parts.  Modifying any part of the copied scheme will have no
   * effect on the original and vice versa.
   * <p>
   * The newly copied scheme can be given a new table source.  If
   * 'immutable' is true, then the resultant scheme is an immutable version
   * of the parent.  An immutable version may share information with the
   * copied version so can not be changed.
   * <p>
   * NOTE: Even if the scheme maintains no state you should still be careful
   *   to ensure a fresh SelectableScheme object is returned here.
   */
  public abstract SelectableScheme copy(TableDataSource table,
                                        boolean immutable);

  /**
   * Dispose and invalidate this scheme.
   */
  public abstract void dispose();


  /**
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Abstract methods for selection of rows, and maintenance of rows
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
  /**
   * Inserts the given element into the set.  This is called just after a
   * row has been initially added to a table.
   */
  abstract void insert(int row);

  /**
   * Removes the given element from the set.  This is called just before the
   * row is removed from the table.
   */
  abstract void remove(int row);

  /**
   * Returns a BlockIntegerList that represents the given row_set sorted
   * in the order of this scheme.  The values in 'row_set' must be references
   * to rows in the domain of the table this scheme represents.
   * <p>
   * The returned set must be stable, meaning if values are equal they keep
   * the same ordering.
   */
  abstract BlockIntegerList internalOrderIndexSet(IntegerVector row_set);

  /**
   * Asks the Scheme for a SelectableScheme abject that describes a sub-set
   * of the set handled by this Scheme.  Since a Table stores a subset
   * of a given DataTable, we pass this as the argument.  It returns a
   * new SelectableScheme that orders the rows in the given columns order.
   * The 'column' variable specifies the column index of this column in the
   * given table.
   */
  abstract SelectableScheme getSubsetScheme(Table table, int table_column);

  /**
   * These are the select operations that are the main purpose of the scheme.
   * They retrieve the given information from the set.  Different schemes will
   * have varying performance on different types of data sets.
   * The select operations must *always* return a resultant row set that
   * is sorted from lowest to highest.
   */
  public abstract IntegerVector selectAll();
  // Returns the first equal cells
  abstract IntegerVector selectFirst();
  abstract IntegerVector selectNotFirst();
  // Returns the last equal cells
  abstract IntegerVector selectLast();
  abstract IntegerVector selectNotLast();


  public IntegerVector selectEqual(DataCell ob) {
    return selectRange(new SelectableRange(
                         SelectableRange.FIRST_VALUE, ob,
                         SelectableRange.LAST_VALUE, ob));
  }

  public IntegerVector selectNotEqual(DataCell ob) {
    return selectRange(new SelectableRange[] {
          new SelectableRange(
                  SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
                  SelectableRange.BEFORE_FIRST_VALUE, ob)
          , new SelectableRange(
                  SelectableRange.AFTER_LAST_VALUE, ob,
                  SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET)
          });
  }

  public IntegerVector selectGreater(DataCell ob) {
    return selectRange(new SelectableRange(
               SelectableRange.AFTER_LAST_VALUE, ob,
               SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
  }

  public IntegerVector selectLess(DataCell ob) {
    return selectRange(new SelectableRange(
               SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
               SelectableRange.BEFORE_FIRST_VALUE, ob));
  }

  public IntegerVector selectGreaterOrEqual(DataCell ob) {
    return selectRange(new SelectableRange(
               SelectableRange.FIRST_VALUE, ob,
               SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
  }

  public IntegerVector selectLessOrEqual(DataCell ob) {
    return selectRange(new SelectableRange(
               SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
               SelectableRange.LAST_VALUE, ob));
  }

  // Inclusive of rows that are >= ob1 and < ob2
  // NOTE: This is not compatible with SQL BETWEEN predicate which is all
  //   rows that are >= ob1 and <= ob2
  public IntegerVector selectBetween(DataCell ob1, DataCell ob2) {
    return selectRange(new SelectableRange(
               SelectableRange.FIRST_VALUE, ob1,
               SelectableRange.BEFORE_FIRST_VALUE, ob2));
  }

  /**
   * Selects the given range of values from this index.  The SelectableRange
   * must contain a 'start' value that compares <= to the 'end' value.
   * <p>
   * This must guarentee that the returned set is sorted from lowest to
   * highest value.
   */
  abstract IntegerVector selectRange(SelectableRange range);

  /**
   * Selects a set of ranges from this index.  The ranges must not overlap and
   * each range must contain a 'start' value that compares <= to the 'end'
   * value.  Every range in the array must represent a range that's lower than
   * the preceeding range (if it exists).
   * <p>
   * If the above rules are enforced (as they must be) then this method will
   * return a set that is sorted from lowest to highest value.
   * <p>
   * This must guarentee that the returned set is sorted from lowest to
   * highest value.
   */
  abstract IntegerVector selectRange(SelectableRange[] ranges);

}
