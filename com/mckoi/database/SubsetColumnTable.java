/**
 * com.mckoi.database.SubsetColumnTable  06 Apr 1998
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
 * This object is a filter that sits atop a Table object.  Its purpose is to
 * only provide a view of the columns that are required.  In a Select
 * query we may create a query with only the subset of columns that were
 * originally in the table set.  This object allows us to provide an
 * interface to only the columns that the Table is allowed to access.
 * <p>
 * This method implements RootTable which means a union operation will not
 * decend further past this table when searching for the roots.
 *
 * @author Tobias Downer
 */

public final class SubsetColumnTable extends FilterTable
                                     implements RootTable {

  /**
   * Maps from the column in this table to the column in the parent table.
   * The number of entries of this should match the number of columns in this
   * table.
   */
  private int[] column_map;

  /**
   * Maps from the column in the parent table, to the column in this table.
   * The size of this should match the number of columns in the parent
   * table.
   */
  private int[] reverse_column_map;

  /**
   * The list of TableField objects in this table (copied from the parent).
   */
  private TableField[] new_field_list;

  /**
   * The resolved Variable aliases for this subset.  These are returned by
   * getResolvedVariable and used in searches for findResolvedVariable.  This
   * can be used to remap the variable names used to match the columns.
   */
  private Variable[] aliases;


  /**
   * The Constructor.
   */
  public SubsetColumnTable(Table parent) {
    super(parent);
  }

  /**
   * Adds a column map into this table.  The int array contains a map to the
   * column in the parent object that we want the column number to reference.
   * For example, to select columns 4, 8, 1, 2 into this new table, the
   * array would be { 4, 8, 1, 2 }.
   */
  public void setColumnMap(int[] mapping, Variable[] aliases) {
    reverse_column_map = new int[parent.getColumnCount()];
    for (int i = 0; i < reverse_column_map.length; ++i) {
      reverse_column_map[i] = -1;
    }
    column_map = mapping;

    this.aliases = aliases;

    new_field_list = new TableField[mapping.length];
    for (int i = 0; i < new_field_list.length; ++i) {
      int map_to = mapping[i];
      new_field_list[i] =
              new TableField(aliases[i].getName(), parent.getFieldAt(map_to));
      reverse_column_map[map_to] = i;
    }
  }




  /**
   * Returns the number of columns in the table.
   */
  public int getColumnCount() {
    return aliases.length;
  }

  /**
   * Returns a list of all the fields within the table.  The list is ordered
   * the same way the fields were added in to the table.  BIG NOTE: The
   * names of the fields returned by this method do not contain any domain
   * information.
   */
  public TableField[] getFields() {
    return new_field_list;
  }

  /**
   * Returns the field at the given column.  BIG NOTE: The names of the fields
   * returned by this method do not contain any domain information.
   */
  public TableField getFieldAt(int column) {
    return new_field_list[column];
  }

  /**
   * Given a field name, ie. 'CUSTOMER.CUSTOMERID' this will return the
   * column number the field is at.  Returns -1 if the field does not exist
   * in this table.
   *
   * @deprecated
   */
  public int findFieldName(String name) {
    throw new Error("Deprecated method not available.");
//    int i = parent.findFieldName(name);
//    if (i == -1) {
//      return -1;
//    }
//    return reverse_column_map[i];
  }

  /**
   * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
   * this will return the column number the field is at.  Returns -1 if the
   * field does not exist in the table.
   */
  public int findFieldName(Variable v) {
    for (int i = 0; i < aliases.length; ++i) {
      if (v.equals(aliases[i])) {
        return i;
      }
    }
    return -1;

//    // First check if this is a column name in our new list
//    boolean go_ahead = false;
//    if (table_alias == null) {
//      if (v.getTableName() == null) {
//        go_ahead = true;
//      }
//    }
//    else {
//      if (v.getTableName() != null && table_alias.equals(v.getTableName())) {
//        go_ahead = true;
//      }
//    }
//
//    if (go_ahead) {
//      String col_name = v.getName();
//      for (int i = 0; i < new_field_list.length; ++i) {
//        if (new_field_list[i].getName().equals(col_name)) {
//          return i;
//        }
//      }
//    }
//
//    return -1;
//
////    // Otherwise roll back to searching in the parent object.
////    int i = parent.findFieldName(v);
////    if (i == -1) {
////      return -1;
////    }
////    return reverse_column_map[i];
  }

  /**
   * Returns the fully resolved name of the given column in this table.  This
   * must return the fully resolved string of the format.
   *
   * @deprecated
   */
  public String getResolvedColumnName(int column) {
    throw new Error("Deprecated method not available.");
//    // We return the straight name and skip any pre table definitions in the
//    // var
//    return aliases[column].toString();
  }

  /**
   * Returns a fully qualified Variable object that represents the name of
   * the column at the given index.  For example,
   *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
   */
  public Variable getResolvedVariable(int column) {
    return aliases[column];
//    if (table_alias == null) {
//      return new Variable(new_field_list[column].getName());
//    }
//    else {
//      return new Variable(table_alias, new_field_list[column].getName());
//    }
  }

  /**
   * Returns a SelectableScheme for the given column in the given VirtualTable
   * row domain.
   */
  final SelectableScheme getSelectableSchemeFor(int column,
                                            int original_column, Table table) {

//    // A SubsetColumnTable doesn't change the row topology in any way.  So
//    // we can directly use the selectable scheme from the parent.  To do this,
//    // we need to pretend the ancestor is the parent if the ancestor is this
//    // table.
//    if (table == this) {
//      return parent.getSelectableSchemeFor(column_map[column],
//                                         column_map[original_column], parent);
//    }

    // We need to map the original_column if the original column is a reference
    // in this subset column table.  Otherwise we leave as is.
    // The reason is because FilterTable pretends the call came from its
    // parent if a request is made on this table.
    int mapped_original_column = original_column;
    if (table == this) {
      mapped_original_column = column_map[original_column];
    }

    return super.getSelectableSchemeFor(column_map[column],
                                        mapped_original_column, table);
  }

  /**
   * Given a set, this trickles down through the Table hierarchy resolving
   * the given row_set to a form that the given ancestor understands.
   * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
   * down three levels and return a new 7 element set with the rows fully
   * resolved to the given ancestors domain.
   */
  final void setToRowTableDomain(int column, IntegerVector row_set,
                                 TableDataSource ancestor) {

//    // A SubsetColumnTable doesn't change the row topology in any way.  So
//    // we can directly use the selectable scheme from the parent.  To do this,
//    // we need to pretend the ancestor is the parent if the ancestor is this
//    // table.
//    if (ancestor == this) {
//      parent.setToRowTableDomain(column_map[column], row_set, parent);
//    }

//    parent.setToRowTableDomain(column_map[column], row_set, ancestor);
    super.setToRowTableDomain(column_map[column], row_set, ancestor);
  }

  /**
   * Return the list of DataTable and row sets that make up the raw information
   * in this table.
   */
  final RawTableInformation resolveToRawTable(RawTableInformation info) {
    throw new Error("Tricky to implement this method!");
    // ( for a SubsetColumnTable that is )
  }

  /**
   * Returns an object that represents the information in the given cell
   * in the table.  This will generally be an expensive algorithm, so calls
   * to it should be kept to a minimum.  Note that the offset between two
   * rows is not necessarily 1.
   */
  public final DataCell getCellContents(int column, int row) {
    return parent.getCellContents(column_map[column], row);
  }

  /**
   * Compares the object to the object at the given cell in the table.  The
   * Object may only be one of the types allowed in the database. This will
   * generally be an expensive algorithm, so calls to it should be kept to a
   * minimum.  See the Note in the above method about row offsets.
   * Returns: LESS_THAN, GREATER_THAN, or EQUAL
   */
  public final int compareCellTo(DataCell ob, int column, int row) {
    return parent.compareCellTo(ob, column_map[column], row);
  }

  // ---------- Implemented from RootTable ----------

  /**
   * This function is used to check that two tables are identical.  This
   * is used in operations like 'union' that need to determine that the
   * roots are infact of the same type.
   */
  public boolean typeEquals(RootTable table) {
    return (this == table);
  }


  /**
   * Returns a string that represents this table.
   */
  public String toString() {
    String name = "SCT" + hashCode();
    return name + "[" + getRowCount() + "]";
  }

}
