/**
 * com.mckoi.database.FunctionTable  12 Jul 2000
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

import com.mckoi.database.global.Types;
import com.mckoi.database.global.ByteLongObject;
import com.mckoi.util.Cache;
import com.mckoi.util.IntegerVector;
import com.mckoi.debug.*;
import java.math.BigDecimal;
import java.util.Date;

/**
 * A table that has a number of columns and as many rows as the refering
 * table.  Tables of this type are used to construct aggregate and function
 * columns based on an expression.  They are joined with the result table in
 * the last part of the query processing.
 * <p>
 * For example, a query like 'select id, id * 2, 8 * 9 from Part' the
 * columns 'id * 2' and '8 * 9' would be formed from this table.
 * <p>
 * SYNCHRONIZATION ISSUE: Instances of this object are NOT thread safe.  The
 *   reason it's not is because if 'getCellContents' is used concurrently it's
 *   possible for the same value to be added into the cache causing an error.
 *   It is not expected that this object will be shared between threads.
 *
 * @author Tobias Downer
 */

public class FunctionTable extends DefaultDataTable {

  /**
   * The key used to make distinct unique ids for FunctionTables.
   * <p>
   * NOTE: This is a thread-safe static mutable variable.
   */
  private static int UNIQUE_KEY_SEQ = 0;

  /**
   * A 1 row table used for function table's that have no reference table.
   * This is provided so we can construct function tables with no reference
   * table (eg SELECT 9 * 40 )
   */
  static final Table SINGLE_ROW_TABLE;

  static {
    TemporaryTable t = null;
    try {
      t = new TemporaryTable(null, "SINGLE_ROW_TABLE", new TableField[0]);
      t.newRow();
    }
    catch (DatabaseException e) {
      e.printStackTrace();
    }
    SINGLE_ROW_TABLE = t;
  }

  /**
   * A unique id given to this FunctionTable when it is created.  No two
   * FunctionTable objects may have the same number.  This number is between
   * 0 and 260 million.
   */
  private int unique_id;

  /**
   * The table that this function table cross references.  This is not a
   * parent table, but more like the table we will eventually be joined with.
   */
  private Table cross_ref_table;

  /**
   * The TableVariableResolver for the table we are cross referencing.
   */
  private TableVariableResolver cr_resolver;

  /**
   * The TableGroupResolver for the table.
   */
  private TableGroupResolver group_resolver;

  /**
   * The list of expressions that are evaluated to form each column.
   */
  private Expression[] exp_list;

  /**
   * The lookup mapping for row->group_index used for grouping.
   */
  private IntegerVector group_lookup;

  /**
   * The group row links.  Iterate through this to find all the rows in a
   * group until bit 31 set.
   */
  private IntegerVector group_links;

  /**
   * Whether the whole table is a group.
   */
  private boolean whole_table_as_group = false;

  /**
   * If the whole table is a group, this is the grouping rows.  This is
   * obtained via 'selectAll' of the reference table.
   */
  private IntegerVector whole_table_group;

  /**
   * The context of this function table.
   */
  private QueryContext context;



  /**
   * Constructs the FunctionTable.
   */
  public FunctionTable(Table cross_ref_table, Expression[] exp_list,
                       String[] col_names, DatabaseQueryContext context) {
    super(context.getDatabase(), "FUNCTIONTABLE", exp_list.length);

    // Make sure we are synchronized over the class.
    synchronized(FunctionTable.class) {
      unique_id = UNIQUE_KEY_SEQ;
      ++UNIQUE_KEY_SEQ;
    }
    unique_id = (unique_id & 0x0FFFFFFF) | 0x010000000;

    this.context = context;

    this.cross_ref_table = cross_ref_table;
    cr_resolver = cross_ref_table.getVariableResolver();
    cr_resolver.setRow(0);
    this.exp_list = exp_list;

    try {
      // Create a new TableField for each expression object.
      for (int i = 0; i < exp_list.length; ++i) {
        Expression e = exp_list[i];
        String name = col_names[i];
        TableField field = new TableField(name,
                typeFor(e.returnClass(cr_resolver, context)),
                Integer.MAX_VALUE, false);
        addField(field);
      }
    }
    catch (DatabaseException e) {
      // This should not happen
      throw new Error("Database Exception: " + e.getMessage());
    }

    // Function tables are the size of the referring table.
    row_count = cross_ref_table.getRowCount();

    // Set schemes to 'blind search'.
    blankSelectableSchemes(1);
  }

  public FunctionTable(Expression[] exp_list, String[] col_names,
                       DatabaseQueryContext context) {
    this(SINGLE_ROW_TABLE, exp_list, col_names, context);
  }



  /**
   * Returns a DB_ Type object that represents the given class.
   */
  private int typeFor(Class cl) {
    if (cl == String.class) {
      return Types.DB_STRING;
    }
    else if (cl == BigDecimal.class) {
      return Types.DB_NUMERIC;
    }
    else if (cl == Date.class) {
      return Types.DB_TIME;
    }
    else if (cl == Boolean.class) {
      return Types.DB_BOOLEAN;
    }
    else if (cl == ByteLongObject.class) {
      return Types.DB_BINARY;
    }
    else {
      return Types.DB_OBJECT;
//      throw new Error("Unrecognised class: " + cl);
    }
  }


  /**
   * Return a DataCell that represents the value of the 'column', 'row' of
   * this table.
   */
  private DataCell calcValue(int column, int row) {
    cr_resolver.setRow(row);
    if (group_resolver != null) {
      group_resolver.setUpGroupForRow(row);
    }
//    System.out.println(group_resolver);
    Object ob = exp_list[column].evaluate(group_resolver, cr_resolver,
                                          context);
    if (ob == null) {
      ob = Expression.NULL_OBJ;
    }
    DataCell cell = DataCellFactory.generateDataCell(getFieldAt(column), ob);
    return cell;
  }

  // ------ Public methods ------

  /**
   * Sets the whole reference table as a single group.
   */
  public void setWholeTableAsGroup() {
    whole_table_as_group = true;

    // Set up 'whole_table_group' to the list of all rows in the reference
    // table.
    RowEnumeration enum = getReferenceTable().rowEnumeration();
    whole_table_group = new IntegerVector(getReferenceTable().getRowCount());
    while (enum.hasMoreRows()) {
      whole_table_group.addInt(enum.nextRowIndex());
    }

    // Set up a group resolver for this method.
    group_resolver = new TableGroupResolver();
  }

  /**
   * Creates a grouping matrix for the given tables.  The grouping matrix
   * is arranged so that each row of the refering table that is in the
   * group is given a number that refers to the top group entry in the
   * group list.  The group list is a linked integer list that chains through
   * each row item in the list.
   */
  public void createGroupMatrix(Variable[] col_list) {
    // If we have zero rows, then don't bother creating the matrix.
    if (getRowCount() <= 0 || col_list.length <= 0) {
      return;
    }

    Table root_table = getReferenceTable();
    int r_count = root_table.getRowCount();
    int[] col_lookup = new int[col_list.length];
    for (int i = col_list.length - 1; i >= 0; --i) {
      col_lookup[i] = root_table.findFieldName(col_list[i]);
    }
    IntegerVector row_list = root_table.orderedRowList(col_lookup);

//    Table work = root_table;
//    // Sort by the column list.
//    for (int i = col_list.length - 1; i >= 0; --i) {
//      col_lookup[i] = root_table.findFieldName(col_list[i]);
//      work = work.orderByColumn(col_lookup[i], true);
//    }
//
//    // A nice post condition to check on.
//    if (root_table.getRowCount() != work.getRowCount()) {
//      throw new Error("Internal Error, row count != sorted row count");
//    }
//
//    // 'work' is now sorted by the columns,
//    // Get the rows in this tables domain,
//    int r_count = root_table.getRowCount();
//    IntegerVector row_list = new IntegerVector(r_count);
//    for (int i = 0; i < r_count; ++i) {
//      row_list.addInt(i);
//    }
//
//    work.setToRowTableDomain(0, row_list, root_table);
//    // Let the GC destroy the work object now if it wants.
//    work = null;

    // 'row_list' now contains rows in this table sorted by the columns to
    // group by.

    // This algorithm will generate two lists.  The group_lookup list maps
    // from rows in this table to the group number the row belongs in.  The
    // group number can be used as an index to the 'group_links' list that
    // contains consequtive links to each row in the group until -1 is reached
    // indicating the end of the group;

    group_lookup = new IntegerVector(r_count);
    group_links = new IntegerVector(r_count);
    int current_group = 0;
    int previous_row = -1;
    for (int i = 0; i < r_count; ++i) {
      int row_index = row_list.intAt(i);

      if (previous_row != -1) {

        boolean equal = true;
        // Compare cell in column in this row with previous row.
        for (int n = 0; n < col_lookup.length && equal; ++n) {
          DataCell c1 = root_table.getCellContents(col_lookup[n], row_index);
          DataCell c2 =
                     root_table.getCellContents(col_lookup[n], previous_row);
          equal = equal && (c1.compareTo(c2) == 0);
        }

        if (!equal) {
          // If end of group, set bit 15
          group_links.addInt(previous_row | 0x040000000);
          current_group = group_links.size();
        }
        else {
          group_links.addInt(previous_row);
        }

      }

      group_lookup.placeIntAt(current_group, row_index);   // (val, pos)

      previous_row = row_index;
    }
    // Add the final row.
    group_links.addInt(previous_row | 0x040000000);

    // Set up a group resolver for this method.
    group_resolver = new TableGroupResolver();

  }



  // ------ Methods intended for use by grouping functions ------

  /**
   * Returns the Table this function is based on.  We need to provide this
   * method for aggregate functions.
   */
  public Table getReferenceTable() {
    return cross_ref_table;
  }

  /**
   * Returns the group of the row at the given index.
   */
  public int rowGroup(int row_index) {
    return group_lookup.intAt(row_index);
  }

  /**
   * The size of the group with the given number.
   */
  public int groupSize(int group_number) {
    int group_size = 1;
    int i = group_links.intAt(group_number);
    while ((i & 0x040000000) == 0) {
      ++group_size;
      ++group_number;
      i = group_links.intAt(group_number);
    }
    return group_size;
  }

  /**
   * Returns an IntegerVector that represents the list of all rows in the
   * group the index is at.
   */
  public IntegerVector groupRows(int group_number) {
    IntegerVector ivec = new IntegerVector();
    int i = group_links.intAt(group_number);
    while ((i & 0x040000000) == 0) {
      ivec.addInt(i);
      ++group_number;
      i = group_links.intAt(group_number);
    }
    ivec.addInt(i & 0x03FFFFFFF);
    return ivec;
  }

  /**
   * Returns a Table that is this function table merged with the cross
   * reference table.  The result table includes only one row from each
   * group.
   * <p>
   * The 'max_column' argument is optional (can be null).  If it's set to a
   * column in the reference table, then the row with the max value from the
   * group is used as the group row.  For example, 'Part.id' will return the
   * row with the maximum part.id from each group.
   */
  public Table mergeWithReference(Variable max_column) {
    Table table = getReferenceTable();

    IntegerVector row_list;

    if (whole_table_as_group) {
      // Whole table is group, so take top entry of table.

      row_list = new IntegerVector(1);
      RowEnumeration row_enum = table.rowEnumeration();
      if (row_enum.hasMoreRows()) {
        row_list.addInt(row_enum.nextRowIndex());
      }
      else {
        // MAJOR HACK: If the referencing table has no elements then we choose
        //   an arbitary index from the reference table to merge so we have
        //   at least one element in the table.
        //   This is to fix the 'SELECT COUNT(*) FROM empty_table' bug.
        row_list.addInt(Integer.MAX_VALUE - 1);
      }
    }
    else if (table.getRowCount() == 0) {
      row_list = new IntegerVector(0);
    }
    else if (group_links != null) {
      // If we are grouping, reduce down to only include one row from each
      // group.
      if (max_column == null) {
        row_list = topFromEachGroup();
      }
      else {
        int col_num = getReferenceTable().findFieldName(max_column);
        row_list = maxFromEachGroup(col_num);
      }
    }
    else {
      // This means there is no grouping, so merge with entire table,
      int r_count = table.getRowCount();
      row_list = new IntegerVector(r_count);
      RowEnumeration enum = table.rowEnumeration();
      while (enum.hasMoreRows()) {
        row_list.addInt(enum.nextRowIndex());
      }
//      return table;
    }

    // Create a virtual table that's the new group table merged with the
    // functions in this...

    Table[] tabs = new Table[] { table, this };
    IntegerVector[] row_sets = new IntegerVector[] { row_list, row_list };

    VirtualTable out_table = new VirtualTable(tabs);
    out_table.set(tabs, row_sets);

    // Output this as debugging information
    if (DEBUG_QUERY) {
      if (Debug().isInterestedIn(Lvl.INFORMATION)) {
        Debug().write(Lvl.INFORMATION, this,
                    out_table + " = " + this + ".mergeWithReference(" +
                    getReferenceTable() + ", " + max_column + " )");
      }
    }

    table = out_table;
    return table;
  }

  // ------ Package protected methods -----

  /**
   * Returns a list of rows that represent one row from each distinct group
   * in this table.  This should be used to construct a virtual table of
   * rows from each distinct group.
   */
  IntegerVector topFromEachGroup() {
    IntegerVector extract_rows = new IntegerVector();
    int size = group_links.size();
    boolean take = true;
    for (int i = 0; i < size; ++i) {
      int r = group_links.intAt(i);
      if (take) {
        extract_rows.addInt(r & 0x03FFFFFFF);
      }
      if ((r & 0x040000000) == 0) {
        take = false;
      }
      else {
        take = true;
      }
    }

    return extract_rows;
  }




  /**
   * Returns a list of rows that represent the maximum row of the given column
   * from each distinct group in this table.  This should be used to construct
   * a virtual table of rows from each distinct group.
   */
  IntegerVector maxFromEachGroup(int col_num) {

    final Table ref_tab = getReferenceTable();

    IntegerVector extract_rows = new IntegerVector();
    int size = group_links.size();

    int to_take_in_group = -1;
    DataCell max = null;

    boolean take = true;
    for (int i = 0; i < size; ++i) {
      int r = group_links.intAt(i);

      int act_r_index = r & 0x03FFFFFFF;
      DataCell cell = ref_tab.getCellContents(col_num, act_r_index);
      if (max == null || cell.compareTo(max) > 0) {
        max = cell;
        to_take_in_group = act_r_index;
      }
      if ((r & 0x040000000) != 0) {
        extract_rows.addInt(to_take_in_group);
        max = null;
      }

    }

    return extract_rows;
  }

  // ------ Methods that are implemented for Table interface ------

  /**
   * Returns an object that represents the information in the given cell
   * in the table.  This can be used to obtain information about the given
   * table cells.
   */
  public DataCell getCellContents(int column, int row) {

    // [ FUNCTION TABLE CACHING NOW USES THE GLOBAL CELL CACHING MECHANISM ]
    // Check if in the cache,
    DataCellCache cache = getDatabase().getDataCellCache();
    // Caching enabled?
    if (cache != null) {
      DataCell cell = cache.get(unique_id, row, column);
      if (cell != null) {
        // In the cache so return the cell.
        return cell;
      }
      else {
        // Not in the cache so calculate the value and put it in the cache.
        cell = calcValue(column, row);
        cache.put(unique_id, row, column, cell);
        return cell;
      }
    }
    else {
      // Caching is not enabled
      return calcValue(column, row);
    }

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
  class FTRowEnumeration implements RowEnumeration {
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
    return new FTRowEnumeration();
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
    // Add a data table listener to the reference table.
    // NOTE: This will cause the reference table to have the same listener
    //   registered twice if the 'mergeWithReference' method is used.  While
    //   this isn't perfect behaviour, it means if 'mergeWithReference' isn't
    //   used, we still will be notified of changes in the reference table
    //   which will alter the values in this table.
    getReferenceTable().addDataTableListener(listener);
  }

  /**
   * Removes a DataTableListener from the DataTable objects at the root of
   * this table tree hierarchy.  If this table represents the join of a
   * number of tables, then the DataTableListener is removed from all the
   * DataTable objects at the root.
   */
  void removeDataTableListener(DataTableListener listener) {
    // Removes a data table listener to the reference table.
    // ( see notes above... )
    getReferenceTable().removeDataTableListener(listener);
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
    // We lock the reference table.
    // NOTE: This cause the reference table to lock twice when we use the
    //  'mergeWithReference' method.  While this isn't perfect behaviour, it
    //  means if 'mergeWithReference' isn't used, we still maintain a safe
    //  level of locking.
    getReferenceTable().lockRoot(lock_key);
  }

  /**
   * Unlocks the root tables so that the underlying rows may
   * once again be used if they are not locked and have been removed.  This
   * should be called some time after the rows have been locked.
   */
  public void unlockRoot(int lock_key) {
    // We unlock the reference table.
    // NOTE: This cause the reference table to unlock twice when we use the
    //  'mergeWithReference' method.  While this isn't perfect behaviour, it
    //  means if 'mergeWithReference' isn't used, we still maintain a safe
    //  level of locking.
    getReferenceTable().unlockRoot(lock_key);
  }

  /**
   * Returns true if the table has its row roots locked (via the lockRoot(int)
   * method.
   */
  public boolean hasRootsLocked() {
    return getReferenceTable().hasRootsLocked();
  }

  // ---------- Convenience statics ----------

  /**
   * Returns a FunctionTable that has a single Expression evaluated in it.
   * The column name is 'result'.
   */
  public static Table resultTable(DatabaseQueryContext context,
                                  Expression expression) {
    Expression[] exp = new Expression[] { expression };
    String[] names = new String[] { "result" };
    Table function_table = new FunctionTable(exp, names, context);
    SubsetColumnTable result = new SubsetColumnTable(function_table);

    int[] map = new int[] { 0 };
    Variable[] vars = new Variable[] { new Variable("result") };
    result.setColumnMap(map, vars);

    return result;
  }

  /**
   * Returns a FunctionTable that has a single Object in it.
   * The column title is 'result'.
   */
  public static Table resultTable(DatabaseQueryContext context, Object ob) {
    Expression result_exp = new Expression();
    result_exp.addElement(ob);
    return resultTable(context, result_exp);
  }

  /**
   * Returns a FunctionTable that has an int value made into a BigDecimal.
   * The column title is 'result'.
   */
  public static Table resultTable(DatabaseQueryContext context,
                                  int result_val) {
    return resultTable(context, new BigDecimal(result_val));
  }



  // ---------- Inner classes ----------

  /**
   * Group resolver.  This is used to resolve group information in the
   * refering table.
   */
  final class TableGroupResolver implements GroupResolver {

    /**
     * The IntegerVector that represents the group we are currently
     * processing.
     */
    private IntegerVector group;

//    /**
//     * The group row index we are current set at.
//     */
//    private int group_row_index;

    /**
     * The current group number.
     */
    private int group_number = -1;

    /**
     * A VariableResolver that can resolve variables within a set of a group.
     */
    private TableGVResolver tgv_resolver;


    /**
     * Creates a resolver that resolves variables within a set of the group.
     */
    private TableGVResolver createVariableResolver() {
      if (tgv_resolver != null) {
        return tgv_resolver;
      }
      tgv_resolver = new TableGVResolver();
      return tgv_resolver;
    }


    /**
     * Ensures that 'group' is set up.
     */
    private void ensureGroup() {
      if (group == null) {
        if (group_number == -2) {
          group = whole_table_group;
//          // ISSUE: Unsafe calls if reference table is a DataTable.
//          group = new IntegerVector(getReferenceTable().getRowCount());
//          RowEnumeration renum = getReferenceTable().rowEnumeration();
//          while (renum.hasMoreRows()) {
//            group.addInt(renum.nextRowIndex());
//          }
        }
        else {
          group = groupRows(group_number);
        }
      }
    }

    /**
     * Given a row index, this will setup the information in this resolver
     * to solve for this group.
     */
    public void setUpGroupForRow(int row_index) {
      if (whole_table_as_group) {
        if (group_number != -2) {
          group_number = -2;
          group = null;
        }
      }
      else {
        int g = rowGroup(row_index);
        if (g != group_number) {
          group_number = g;
          group = null;
        }
      }
    }

    public int groupID() {
      return group_number;
    }

    public int size() {
      if (group_number == -2) {
        return whole_table_group.size();
//        // ISSUE: Unsafe call if reference table is a DataTable.
//        return getReferenceTable().getRowCount();
      }
      else if (group != null) {
        return group.size();
      }
      else {
        return groupSize(group_number);
      }
    }

    public Object resolve(Variable variable, int set_index) {
//      String col_name = variable.getName();

      int col_index = getReferenceTable().fastFindFieldName(variable);
      if (col_index == -1) {
        throw new Error("Can't find column: " + variable);
      }

      ensureGroup();

      DataCell cell =
        getReferenceTable().getCellContents(col_index, group.intAt(set_index));

      Object ob = cell.getCell();
      if (ob == null) {
        return Expression.NULL_OBJ;
      }
      return cell.getCell();
    }

    public VariableResolver getVariableResolver(int set_index) {
      TableGVResolver resolver = createVariableResolver();
      resolver.setIndex(set_index);
      return resolver;
    }

    // ---------- Inner classes ----------

    private class TableGVResolver implements VariableResolver {

      private int set_index;

      void setIndex(int set_index) {
        this.set_index = set_index;
      }

      // ---------- Implemented from VariableResolver ----------

      public int setID() {
        throw new Error("setID not implemented here...");
      }

      public Object resolve(Variable variable) {
        return TableGroupResolver.this.resolve(variable, set_index);
      }

      public Class classType(Variable variable) {
//        String col_name = variable.getName();

        int col_index = getReferenceTable().fastFindFieldName(variable);
        if (col_index == -1) {
          throw new Error("Can't find column: " + variable);
        }

        return getReferenceTable().getFieldAt(col_index).classType();
      }

    }



  }


}
