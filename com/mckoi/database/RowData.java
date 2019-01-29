/**
 * com.mckoi.database.RowData  07 Mar 1998
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

import com.mckoi.database.global.ValueSubstitution;
import com.mckoi.database.global.CastHelper;
import com.mckoi.database.global.Types;
import java.util.Vector;
import java.util.Date;
import java.math.BigDecimal;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.IOException;

/**
 * Represents a row of data to be added into a table.  The row data is linked
 * to a TableField that describes the cell information within a row.
 * <p>
 * There are two types of RowData object.  Those that are empty and contain
 * blank data, and those that contain information to either be inserted
 * into a table, or has be retrieved from a row.
 * <p>
 * NOTE: Any RowData objects that need to be set to 'null' should be done so
 *   explicitly.
 * NOTE: We must call a 'setColumnData' method for _every_ column in the
 *   row to form.
 * NOTE: This method (or derived classes) must only use safe methods in
 *   DataTable.  (ie. getRowCount, etc are out).
 * <p>
 * @author Tobias Downer
 */

public class RowData implements Types {

  /**
   * The TransactionSystem this RowData is a context of.
   */
  private TransactionSystem system;

//  /**
//   * The QueryContext that expressions are to be evaluated against.
//   */
//  private QueryContext context;

  /**
   * The TableDataSource object that this RowData is in, or is destined to be
   * in.
   */
  private TableDataSource table;

  /**
   * The definition of the table.
   */
  private DataTableDef table_def;

  /**
   * A list of DataCell objects in the table.
   */
  private DataCell[] data_cell_list;

  /**
   * The number of columns in the row.
   */
  private int col_count;


  /**
   * To create a RowData object without an underlying table.  This is for
   * copying from one table to a different one.
   */
  public RowData(TransactionSystem system, int col_count) {
    this.system = system;
    this.col_count = col_count;
    data_cell_list = new DataCell[col_count];
  }

  /**
   * The Constructor generates a blank row.
   */
  public RowData(TableDataSource table) {
    this.system = table.getSystem();
    this.table = table;
    table_def = table.getDataTableDef();
    col_count = table_def.columnCount();
    data_cell_list = new DataCell[col_count];
  }

  /**
   * Populates the RowData object with information from a specific row from
   * the underlying DataTable.
   */
  void setFromRow(int row) {
    for (int col = 0; col < col_count; ++col) {
      data_cell_list[col] = table.getCellContents(col, row);
    }
  }

  /**
   * Returns the table object this row data is assigned to.  This is used to
   * ensure we don't try to use a row data in a different table to what it was
   * created from.
   */
  boolean isSameTable(DataTable tab) {
    return table == tab;
  }

  /**
   * Sets up a column from a ValueSubstitution object.  This automatically
   * converts the data to the correct type and passes it to the correct set
   * function.
   */
  public void setColumnData(int column, ValueSubstitution val) {
    DataCell cell =
      DataCellFactory.createDataCell(table_def.columnAt(column), val);
    data_cell_list[column] = cell;
  }

  /**
   * Sets up a column from a DataCell object.  This is only useful when we
   * are copying information from one table to another.
   */
  public void setColumnData(int column, DataCell cell) {
    DataTableColumnDef col = table_def.columnAt(column);
    if (table != null && col.getDBType() != cell.getExtractionType()) {
      // If the data cell is null
      if (cell.isNull()) {
        // Then convert as appropriate.  Null values should always be
        // accepted.
        cell = DataCellFactory.createDataCell(col);
      }
      else {
        throw new Error("Can't set column \"" + col.getName() + "\"(" +
                        col.getSQLTypeString() + ") to value '" +
                        cell.getCell() + "'");
      }
    }
    data_cell_list[column] = cell;
  }

  /**
   * Sets up a column from an Object.
   */
  public void setColumnDataFromObject(int column, Object ob) {
    // Put the object through the cast helper
    DataTableColumnDef col_def = table_def.columnAt(column);
    ob = CastHelper.castObjectToSQLType(ob,
              col_def.getSQLType(), col_def.getSize(), col_def.getScale(),
              col_def.getSQLTypeString());

    // Convert to a DataCell
    DataCell cell = DataCellFactory.fromObject(col_def.getDBType(), ob);
                                  //generateDataCell(col_def, ob);
    data_cell_list[column] = cell;
  }

  /**
   * This is a special case situation for setting the column cell to 'null'.
   */
  public void setColumnToNull(int column) {
    DataCell cell = DataCellFactory.createDataCell(table_def.columnAt(column));
    data_cell_list[column] = cell;
  }

  /**
   * Sets the given column number to the default value for this column.
   */
  public void setColumnToDefault(int column, QueryContext context) {
    if (table != null) {
      DataTableColumnDef column_def = table_def.columnAt(column);
      Expression exp = column_def.getDefaultExpression(system);
      if (exp != null) {
        // NOTE: null QueryContext
        setColumnDataFromObject(column, evaluate(exp, context));
        return;
      }
    }
    setColumnToNull(column);
  }

  /**
   * Returns the DataCell that represents the information in the given column
   * of the row.
   */
  public DataCell getCellData(int column) {
    DataCell cell = data_cell_list[column];
    if (cell == null) {
      // If column not set then return a null DataCell.
      cell = DataCellFactory.createDataCell(table_def.columnAt(column));
    }
    return cell;
  }

  /**
   * Returns the name of the given column number.
   */
  public String getColumnName(int column) {
    return table_def.columnAt(column).getName();
  }

  /**
   * Finds the field in this RowData with the given name.
   */
  public int findFieldName(String column_name) {
    return table_def.findColumnName(column_name);
  }

  /**
   * Returns the number of columns (cells) in this row.
   */
  public int getColumnCount() {
    return col_count;
  }

  /**
   * Evaluates the expression and returns the object it evaluates to using
   * the local VariableResolver to resolve variables in the expression.
   */
  Object evaluate(Expression expression, QueryContext context) {
    boolean ignore_case = system.ignoreIdentifierCase();
    // Resolve any variables to the table_def for this expression.
    table_def.resolveColumns(ignore_case, expression);
    // Get the variable resolver and evaluate over this data.
    VariableResolver vresolver = getVariableResolver();
    return expression.evaluate(null, vresolver, context);
  }

  /**
   * Evaluates a single assignment on this RowData object.  A VariableResolver
   * is made which resolves to variables only within this RowData context.
   */
  void evaluate(Assignment assignment, QueryContext context) {

    // Get the variable resolver and evaluate over this data.
    VariableResolver vresolver = getVariableResolver();
    Object ob = assignment.getExpression().evaluate(null, vresolver, context);

    // Check the variable name is within this row.
    Variable variable = assignment.getVariable();
    int column = findFieldName(variable.getName());

    // Set the column to the resolved value.
    setColumnDataFromObject(column, ob);
  }

  /**
   * Any columns in the row of data that haven't been set yet (they will be
   * 'null') will be set to the default value during this method.  This should
   * be called after the row data has initially been set with values from some
   * source.
   */
  public void setDefaultForRest(QueryContext context)
                                                    throws DatabaseException {
    for (int i = 0; i < col_count; ++i) {
      if (data_cell_list[i] == null) {
        setColumnToDefault(i, context);
      }
    }
  }

  /**
   * Sets up an entire row given the array of assignments.  If any columns are
   * left 'null' then they are filled with the default value.
   */
  public void setupEntire(Assignment[] assignments, QueryContext context)
                                                   throws DatabaseException {
    for (int i = 0; i < assignments.length; ++i) {
      evaluate(assignments[i], context);
    }
    // Any that are left as 'null', set to default value.
    setDefaultForRest(context);
  }

  /**
   * Sets up an entire row given the array of Expressions and a list of indices
   * to the columns to set.  If any columns are left 'null' then that are
   * filled with the default value.
   */
  public void setupEntire(int[] col_indices, Expression[] exps,
                          QueryContext context) throws DatabaseException {
    if (col_indices.length != exps.length) {
      throw new DatabaseException(
                     "Column indices and expression array sizes don't match");
    }
    // Get the variable resolver and evaluate over this data.
    VariableResolver vresolver = getVariableResolver();
    for (int i = 0; i < col_indices.length; ++i) {
      // Evaluate to the object to insert
      Object ob = exps[i].evaluate(null, vresolver, context);

      int table_column = col_indices[i];
//      DataTableColumnDef col_def = table_def.columnAt(table_column);
//      // Put the object through the cast helper
//      ob = CastHelper.castObjectToSQLType(ob,
//              col_def.getSQLType(), col_def.getSize(), col_def.getScale(),
//              col_def.getSQLTypeString());
      // Set the column to the resolved value.
      setColumnDataFromObject(table_column, ob);
    }
    // Any that are left as 'null', set to default value.
    setDefaultForRest(context);
  }

  /**
   * Returns a string representation of this row.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[RowData: ");
    for (int i = 0; i < col_count; ++i) {
      buf.append(data_cell_list[i].getCell());
      buf.append(", ");
    }
    return new String(buf);
  }

  /**
   * Returns a VariableResolver to use within this RowData context.
   */
  private VariableResolver getVariableResolver() {
    if (variable_resolver == null) {
      variable_resolver = new RDVariableResolver();
    }
    else {
      variable_resolver.nextAssignment();
    }
    return variable_resolver;

  }

  private RDVariableResolver variable_resolver = null;

  // ---------- Inner classes ----------

  /**
   * Variable resolver for this context.
   */
  private class RDVariableResolver implements VariableResolver {

    private int assignment_count = 0;

    void nextAssignment() {
      ++assignment_count;
    }

    public int setID() {
      return assignment_count;
    }

    public Object resolve(Variable variable) {
      String col_name = variable.getName();

      int col_index = table_def.findColumnName(col_name);
      if (col_index == -1) {
        throw new Error("Can't find column: " + col_name);
      }

      DataCell cell = data_cell_list[col_index];

      if (cell == null) {
        throw new Error("Column " + col_name + " hasn't been set yet.");
      }

      Object ob = cell.getCell();
      if (ob == null) {
        return Expression.NULL_OBJ;
      }
      return ob;
    }

    public Class classType(Variable variable) {
      String col_name = variable.getName();

      int col_index = table_def.findColumnName(col_name);
      if (col_index == -1) {
        throw new Error("Can't find column: " + col_name);
      }

      return table_def.columnAt(col_index).classType();
    }

  }

}
