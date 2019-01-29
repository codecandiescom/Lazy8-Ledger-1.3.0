/**
 * com.mckoi.database.VirtualTable  08 Mar 1998
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

import java.util.Vector;
import com.mckoi.util.IntegerVector;
import com.mckoi.util.BlockIntegerList;
import java.io.IOException;

/**
 * A VirtualTable is a representation of a table whose rows are actually
 * physically stored in another table.  In other words, this table just
 * stores pointers to rows in other tables.
 * <p>
 * We use the VirtualTable to represent temporary tables created from select,
 * join, etc operations.
 * <p>
 * We can not add new rows to a virtual table (as DataRow), however a DataTable
 * can add pointers to rows in a DataTable.
 * <p>
 * An important note about VirtualTables:  When we perform a 'select' operation
 * on a virtual table, unlike a DataTable that perminantly stores information
 * about column cell relations, we must resolve column relations between the
 * sub-set at select time.  This involves asking the tables parent(s) for a
 * scheme to describe relations in a sub-set.
 *
 * @author Tobias Downer
 */

public class VirtualTable extends Table {

  /**
   * Array of parent Table objects.
   */
  protected Table[] reference_list;

  /**
   * Array of IntegerVectors that represent the rows taken from the given
   * parents.
   */
  protected IntegerVector[] row_list;

  /**
   * The number of rows in the table.
   */
  private int row_count;

  /**
   * The schemes to describe the entity relation in the given column.
   */
  protected SelectableScheme[] column_scheme;

  /**
   * These two arrays are lookup tables created in the constructor.  They allow
   * for quick resolution of where a given column should be 'routed' to in
   * the ancestors.
   */
  /**
   * Maps the column number in this table to the reference_list array to route
   * to.
   */
  protected int[] column_table;

  /**
   * Gives a column filter to the given column to route correctly to the
   * ancestor.
   */
  protected int[] column_filter;

  /**
   * The column that we are sorted against.  This is an optimization set by
   * the 'optimisedPostSet' method.
   */
  private int sorted_against_column = -1;

  /**
   * Incremented when the roots are locked.
   * See the 'lockRoot' and 'unlockRoot' methods.
   * NOTE: This should only ever be 1 or 0.
   */
  private byte roots_locked;

  /**
   * Helper function for the constructor.
   */
  protected void init(Table[] tables) {
    int table_count = tables.length;
    reference_list = tables;
    row_list = new IntegerVector[table_count];
    for (int i = 0; i < table_count; ++i) {
      row_list[i] = new IntegerVector();
    }

    final int col_count = getColumnCount();
    column_scheme = new SelectableScheme[col_count];

    // Generate look up tables for column_table and column_filter information

    column_table = new int[col_count];
    column_filter = new int[col_count];
    int index = 0;
    for (int i = 0; i < reference_list.length; ++i) {
      int max = 0;
      int ref_col_count = reference_list[i].getColumnCount();
      for (int n = 0; n < ref_col_count; ++n) {
        column_filter[index] = max;
        column_table[index] = i;
        ++index;
        ++max;
      }
    }

  }

  /**
   * The Constructor.  It is constructed with a list of tables that this
   * virtual table is a sub-set or join of.
   */
  VirtualTable(Table[] tables) {
    super();
    init(tables);
  }

  VirtualTable(Table table) {
    super();
    Table[] tables = new Table[1];
    tables[0] = table;
    init(tables);
  }

  protected VirtualTable() {
    super();
  }

  /**
   * Returns a row reference list.  This is an IntegerVector that represents a
   * 'reference' to the rows in our virtual table.
   * <p>
   * ISSUE: We should be able to optimise these types of things out.
   */
  private IntegerVector calculateRowReferenceList() {
    int size = getRowCount();
    IntegerVector all_list = new IntegerVector(size);
    for (int i = 0; i < size; ++i) {
      all_list.addInt(i);
    }
    return all_list;
  }

  /**
   * We simply pick the first table to resolve the Database object.
   */
  public Database getDatabase() {
    return reference_list[0].getDatabase();
  }

  /**
   * Returns the number of columns in the table.  This simply returns the
   * column counts in the parent table(s).
   */
  public int getColumnCount() {
    int column_count_sum = 0;
    for (int i = 0; i < reference_list.length; ++i) {
      column_count_sum += reference_list[i].getColumnCount();
    }
    return column_count_sum;
  }

  /**
   * Returns the number of rows stored in the table.
   */
  public int getRowCount() {
    return row_count;
  }

  /**
   * Returns a list of all the fields within the table.
   */
  public TableField[] getFields() {
    Vector field_vec = new Vector();
    for (int i = 0; i < reference_list.length; ++i) {
      TableField[] f_list = reference_list[i].getFields();
      for (int n = 0; n < f_list.length; ++n) {
        field_vec.addElement(f_list[n]);
      }
    }
    TableField[] all_fields = new TableField[field_vec.size()];
    field_vec.copyInto(all_fields);
    return all_fields;
  }

  /**
   * Returns the field object at the given column.  This must query its
   * sub-tables for the information.
   */
  public TableField getFieldAt(int column) {
    Table parent_table = reference_list[column_table[column]];
    return parent_table.getFieldAt(column_filter[column]);
  }

  /**
   * Given a field name, ie. 'CUSTOMER.CUSTOMERID' this will return the
   * column number the field is at.  Returns -1 if the field name does not
   * exist within the table.  This must query each sub-table for the
   * information.
   *
   * @deprecated
   */
  public int findFieldName(String name) {
    int col_index = 0;
    for (int i = 0; i < reference_list.length; ++i) {
      int col = reference_list[i].findFieldName(name);
      if (col != -1) {
        return col + col_index;
      }
      col_index += reference_list[i].getColumnCount();
    }
    return -1;
  }

  /**
   * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
   * this will return the column number the field is at.  Returns -1 if the
   * field does not exist in the table.
   */
  public int findFieldName(Variable v) {
    int col_index = 0;
    for (int i = 0; i < reference_list.length; ++i) {
      int col = reference_list[i].findFieldName(v);
      if (col != -1) {
        return col + col_index;
      }
      col_index += reference_list[i].getColumnCount();
    }
    return -1;
  }

  /**
   * Returns the fully resolved name of the given column in this table.
   *
   * @deprecated
   */
  public final String getResolvedColumnName(int column) {
    Table parent_table = reference_list[column_table[column]];
    return parent_table.getResolvedColumnName(column_filter[column]);
  }

  /**
   * Returns a fully qualified Variable object that represents the name of
   * the column at the given index.  For example,
   *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
   */
  public final Variable getResolvedVariable(int column) {
    Table parent_table = reference_list[column_table[column]];
    return parent_table.getResolvedVariable(column_filter[column]);
  }

  /**
   * Sets the rows in this table.  We should search for the
   * 'table' in the 'reference_list' however we don't for efficiency.
   */
  void set(Table table, IntegerVector rows) {
    row_list[0] = new IntegerVector(rows);
    row_count = rows.size();
  }

  /**
   * This is used in a join to set a list or joined rows and tables.  The
   * 'tables' array should be an exact mirror of the 'reference_list'.  The
   * IntegerVector[] array contains the rows to add for each respective table.
   * The given IntegerVector objects should have identical lengths.
   */
  void set(Table[] tables, IntegerVector[] rows) {
    for (int i = 0; i < tables.length; ++i) {
      row_list[i] = new IntegerVector(rows[i]);
    }
    if (rows.length > 0) {
      row_count = rows[0].size();
    }
  }

  /**
   * Sets the rows in this table as above, but uses a BlockIntegerList as an
   * argument instead.
   */
  void set(Table table, BlockIntegerList rows) {
    row_list[0] = new IntegerVector(rows);
    row_count = rows.size();
  }

  /**
   * Sets the rows in this table as above, but uses a BlockIntegerList array
   * as an argument instead.
   */
  void set(Table[] tables, BlockIntegerList[] rows) {
    for (int i = 0; i < tables.length; ++i) {
      row_list[i] = new IntegerVector(rows[i]);
    }
    if (rows.length > 0) {
      row_count = rows[0].size();
    }
  }

  /**
   * Returns the list of Table objects that represent this VirtualTable.
   */
  protected Table[] getReferenceTables() {
    return reference_list;
  }

  /**
   * Returns the list of IntegerVector that represents the rows that this
   * VirtualTable references.  NOTE: This list can be updated.  If it is
   * updated then call 'refreshTable' to update internal state.
   */
  protected IntegerVector[] getReferenceRows() {
    return row_list;
  }

  /**
   * Updates the internal state of this VirtualTable.  This should be called
   * if the information returned by 'getReferenceTables' or 'getReferenceRows'
   * is changed.
   */
  protected void refreshTable() {
    // Update the row count.
    row_count = row_list[0].size();
  }

  /**
   * This is an optimisation that should only be called _after_ a 'set' method
   * has been called.  Because the 'select' operation returns a set that is
   * ordered by the given column, we can very easily generate a
   * SelectableScheme object that can handle this column.
   * So 'column' is the column in which this virtual table is naturally ordered
   * by.
   * NOTE: The internals of this method may be totally commented out and the
   *   database will still operate correctly.  However this greatly speeds up
   *   situations when you perform multiple consequtive operations on the same
   *   column.
   */
  void optimisedPostSet(int column) {
    sorted_against_column = column;
  }

  /**
   * Returns a SelectableScheme for the given column in the given VirtualTable
   * row domain.  This searches down through the tables ancestors until it
   * comes across a table with a SelectableScheme where the given column is
   * fully resolved.  In most cases, this will be the root DataTable.
   */
  SelectableScheme getSelectableSchemeFor(int column, int original_column,
                                          Table table) {

//    System.out.println(this + ".getSelectableSchemaFor(" +
//                       column + ", " + original_column + ", " + table + ")");

//    System.out.println("column: " + column);
//    System.out.println("original_column: " + original_column);

    // First check if the given SelectableScheme is in the column_scheme array
    SelectableScheme scheme = column_scheme[column];
    if (scheme != null) {
      if (table == this) {
        return scheme;
      }
      else {
        return scheme.getSubsetScheme(table, original_column);
      }
    }

    // If it isn't then we need to calculate it
    SelectableScheme ss;

    // Optimization: The table may be naturally ordered by a column.  If it
    // is we don't try to generate an ordered set.
    if (sorted_against_column != -1 &&
        sorted_against_column == column) {
      InsertSearch isop =
                  new InsertSearch(this, column, calculateRowReferenceList());
      isop.RECORD_UID = false;
      ss = isop;
      column_scheme[column] = ss;
      if (table != this) {
        ss = ss.getSubsetScheme(table, original_column);
      }

    }
    else {
      // Otherwise we must generate the ordered set from the information in
      // a parent index.
      Table parent_table = reference_list[column_table[column]];
//      System.out.println("Descending to: " + parent_table);
      ss = parent_table.getSelectableSchemeFor(
                               column_filter[column], original_column, table);
      if (table == this) {
        column_scheme[column] = ss;
      }
    }

//    System.out.println("Selectable Schema: " + ss);

    return ss;
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

//    System.out.println(getClass());
//    System.out.print("VT List: ");
//    for (int i = 0; i < reference_list.length; ++i) {
//      System.out.print(reference_list[i] + ", ");
//    }
//    System.out.println();
//    System.out.println("Looking for: " + ancestor);
//    System.out.println("Column: " + column);

//    System.out.println("VT ancestor: " + ancestor);

    if (ancestor == this) {
      return;
    }
    else {

      int table_num = column_table[column];
      Table parent_table = reference_list[table_num];

//      System.out.println("Chose: " + parent_table);

      // Resolve the rows into the parents indices.  (MANGLES row_set)
      IntegerVector cur_row_list = row_list[table_num];
      for (int n = row_set.size() - 1; n >= 0; --n) {
        int aa = row_set.intAt(n);
        int bb = cur_row_list.intAt(aa);
        row_set.setIntAt(bb, n);
      }
      parent_table.setToRowTableDomain(column_filter[column], row_set, ancestor);
      return;
    }
  }

  /**
   * Returns an object that contains fully resolved, one level only information
   * about the DataTable and the row indices of the data in this table.
   * This information can be used to construct a new VirtualTable.  We need
   * to supply an empty RawTableInformation object.
   */
  RawTableInformation resolveToRawTable(RawTableInformation info,
                                        IntegerVector row_set) {
    for (int i = 0; i < reference_list.length; ++i) {

      IntegerVector new_row_set = new IntegerVector(row_set);

      IntegerVector cur_row_list = row_list[i];
      for (int n = row_set.size() - 1; n >= 0; --n) {
        int aa = row_set.intAt(n);
        int bb = cur_row_list.intAt(aa);
        new_row_set.setIntAt(bb, n);
      }

      Table table = reference_list[i];
      if (table instanceof RootTable) {
        info.add((RootTable) table, new_row_set);
      }
      else {
        ((VirtualTable) table).resolveToRawTable(info, new_row_set);
      }

    }

    return info;
  }

  /**
   * Return the list of DataTable and row sets that make up the raw information
   * in this table.
   */
  RawTableInformation resolveToRawTable(RawTableInformation info) {
    IntegerVector all_list = new IntegerVector();
    int size = getRowCount();
    for (int i = 0; i < size; ++i) {
      all_list.addInt(i);
    }
    return resolveToRawTable(info, all_list);
  }

  /**
   * Resolves the rows in the column into the given tables domain.  The Table
   * object must be an ancestor of this table.  In most cases, the Table object
   * will be the lowest level DataTable object.
   */
  public IntegerVector getResolvedRowSet(int column, Table ancestor) {
    IntegerVector all_list = calculateRowReferenceList();

    setToRowTableDomain(column, all_list, ancestor);
    return all_list;
  }

  /**
   * Returns an object that represents the information in the given cell
   * in the table.
   */
  public DataCell getCellContents(int column, int row) {
    int table_num = column_table[column];
    Table parent_table = reference_list[table_num];
    row = row_list[table_num].intAt(row);
    return parent_table.getCellContents(column_filter[column], row);
  }

  /**
   * Compares the object to the object at the given cell in the table.  The
   * Object may only be one of the types allowed in the database.
   * Returns: LESS_THAN, GREATER_THAN, or EQUAL
   */
  public int compareCellTo(DataCell ob, int column, int row) {
    int table_num = column_table[column];
    Table parent_table = reference_list[table_num];
    row = row_list[table_num].intAt(row);
    return parent_table.compareCellTo(ob, column_filter[column], row);
  }

  /**
   * Returns an Enumeration of the rows in this table.
   * The Enumeration is a fast way of retrieving consequtive rows in the table.
   */
  public RowEnumeration rowEnumeration() {
    return new SimpleRowEnumeration(getRowCount());
  }

//  /**
//   * Returns a boolean array which represents a bit vector for columns that
//   * are valid (not blocked).
//   */
//  boolean[] validColumns() {
//
//    boolean[] out_bit_set = new boolean[getColumnCount()];
//    int index = 0;
//
//    for (int i = 0; i < reference_list.length; ++i) {
//      Table table = reference_list[i];
//      boolean[] col_list = table.validColumns();
//
//      int len = col_list.length;
//      System.arraycopy(col_list, 0, out_bit_set, index, len);
//      index += len;
//
//    }
//
//    if (index != out_bit_set.length) {
//      throw new RuntimeException("Column Count mismatch error.");
//    }
//
//    return out_bit_set;
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
    for (int i = 0; i < reference_list.length; ++i) {
      reference_list[i].addDataTableListener(listener);
    }
  }

  /**
   * Removes a DataTableListener from the DataTable objects at the root of
   * this table tree hierarchy.  If this table represents the join of a
   * number of tables, then the DataTableListener is removed from all the
   * DataTable objects at the root.
   */
  void removeDataTableListener(DataTableListener listener) {
    for (int i = 0; i < reference_list.length; ++i) {
      reference_list[i].removeDataTableListener(listener);
    }
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
    // For each table, recurse.
    roots_locked++;
    for (int i = 0; i < reference_list.length; ++i) {
      reference_list[i].lockRoot(lock_key);
    }
  }

  /**
   * Unlocks the root tables so that the underlying rows may
   * once again be used if they are not locked and have been removed.  This
   * should be called some time after the rows have been locked.
   */
  public void unlockRoot(int lock_key) {
    // For each table, recurse.
    roots_locked--;
    for (int i = 0; i < reference_list.length; ++i) {
      reference_list[i].unlockRoot(lock_key);
    }
  }

  /**
   * Returns true if the table has its row roots locked (via the lockRoot(int)
   * method.
   */
  public boolean hasRootsLocked() {
    return roots_locked != 0;
  }


  /**
   * Prints a graph of the table hierarchy to the stream.
   */
  public void printGraph(java.io.PrintStream out, int indent) {
    for (int i = 0; i < indent; ++i) {
      out.print(' ');
    }
    out.println("C[" + getClass());

    for (int i = 0; i < reference_list.length; ++i) {
      reference_list[i].printGraph(out, indent + 2);
    }

    for (int i = 0; i < indent; ++i) {
      out.print(' ');
    }
    out.println("]");
  }


}
