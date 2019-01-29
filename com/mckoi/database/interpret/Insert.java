/**
 * com.mckoi.database.interpret.Insert  13 Sep 2001
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

package com.mckoi.database.interpret;

import java.util.*;
import com.mckoi.database.*;
import com.mckoi.database.sql.ParseException;
import com.mckoi.util.IntegerVector;

/**
 * The instance class that stores all the information about an insert
 * statement for processing.
 *
 * @author Tobias Downer
 */

public class Insert extends Statement {

  String table_name;

  ArrayList col_list;

  ArrayList values_list;    //list contains Expression[]

  StatementTree select;

  ArrayList column_sets;

  boolean from_values = false;

  boolean from_select = false;

  boolean from_set = false;

  // -----

  /**
   * The table we are inserting stuff to.
   */
  private DataTable insert_table;

  /**
   * For 'from_values' and 'from_select', this is a list of indices into the
   * 'insert_table' for the columns that we are inserting data into.
   */
  private int[] col_index_list;

  /**
   * The list of Variable objects the represent the list of columns being
   * inserted into in this query.
   */
  private Variable[] col_var_list;

  /**
   * The TableName we are inserting into.
   */
  private TableName tname;

  /**
   * If this is a 'from_select' insert, the prepared Select object.
   */
  private Select prepared_select;

  /**
   * Tables that are relationally linked to the table being inserted into, set
   * after 'prepare'.  This is used to determine the tables we need to read
   * lock because we need to validate relational constraints on the tables.
   */
  private ArrayList relationally_linked_tables;



//  /**
//   * Adds a list of expressions to be inserted by this statement.  This is
//   * used as part of the (a, b, c), (b, c, a), ... type values insert.
//   */
//  void addColumnData(Expression[] expressions) throws ParseException {
//    if (values_list.size() >= 1) {
//      Expression[] first_data = (Expression[]) values_list.get(0);
//      if (expressions.length != first_data.length) {
//        throw new ParseException(
//                "Data being inserted is different size to columns specified.");
//      }
//    }
//    values_list.add(expressions);
//  }

  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {

    // Prepare this object from the StatementTree
    table_name = (String) cmd.getObject("table_name");
    col_list = (ArrayList) cmd.getObject("col_list");
    values_list = (ArrayList) cmd.getObject("data_list");
    select = (StatementTree) cmd.getObject("select");
    column_sets = (ArrayList) cmd.getObject("assignments");
    String type = (String) cmd.getObject("type");
    from_values = type.equals("from_values");
    from_select = type.equals("from_select");
    from_set = type.equals("from_set");

    // ---

    // Check 'values_list' contains all same size Expression[] arrays.
    int first_len = -1;
    for (int n = 0; n < values_list.size(); ++n) {
      Expression[] exp_arr = (Expression[]) values_list.get(n);

//      System.out.println(exp_arr);
//      for (int p = 0; p < exp_arr.length; ++p) {
//        System.out.println(exp_arr[p]);
//      }

      if (first_len == -1 || first_len == exp_arr.length) {
        first_len = exp_arr.length;
      }
      else {
        throw new DatabaseException("The insert data list varies in size.");
      }
    }

//    tname = TableName.resolve(db.getCurrentSchema(), table_name);
    tname = resolveTableName(table_name, database);

    // Get the table we are inserting to
    insert_table = database.getTable(tname);
    addTable(new FromTableDirectSource(database,
                               insert_table, "INSERT_TABLE", tname, tname));

    // If column list is empty, then fill it with all columns from table.
    if (from_values || from_select) {
      // If 'col_list' is empty we must pick every entry from the insert
      // table.
      if (col_list.size() == 0) {
        TableField[] fields = insert_table.getFields();
        for (int i = 0; i < fields.length; ++i) {
          col_list.add(fields[i].getName());
        }
      }
      // Resolve 'col_list' into a list of column indices into the insert
      // table.
      col_index_list = new int[col_list.size()];
      col_var_list = new Variable[col_list.size()];
      for (int i = 0; i < col_list.size(); ++i) {
//        Variable col = Variable.resolve(tname, (String) col_list.get(i));
        Variable in_var = Variable.resolve((String) col_list.get(i));
        Variable col = resolveColumn(in_var);
        int index = insert_table.fastFindFieldName(col);
        if (index == -1) {
          throw new DatabaseException("Can't find column: " + col);
        }
        col_index_list[i] = index;
        col_var_list[i] = col;
      }
    }

    // Make the 'from_values' clause into a 'from_set'
    if (from_values) {

      // If values to insert is different from columns list,
      if (col_list.size() != ((Expression[]) values_list.get(0)).length) {
        throw new DatabaseException("Number of columns to insert is " +
                         "different from columns selected to insert to.");
      }

      // Resolve all expressions in the added list.
      for (int i = 0; i < values_list.size(); ++i) {
        Expression[] exps = (Expression[]) values_list.get(i);
        for (int n = 0; n < exps.length; ++n) {

          Expression exp = exps[n];
          List elem_list = exp.allElements();
          for (int p = 0; p < elem_list.size(); ++p) {
            Object ob = elem_list.get(p);
            if (ob instanceof Select) {
              throw new DatabaseException(
                                 "Illegal to have sub-select in expression.");
            }
          }

          // Resolve the expression.
          resolveExpression(exp);

        }
      }

    }
    else if (from_select) {
      // Prepare the select statement
      prepared_select = new Select();
      prepared_select.init(database, select);
      prepared_select.prepare();
//      // If we are inserting from a select statement,
//      select.doPrepare(db, user);
    }

    // If from a set, then resolve all values,
    else if (from_set) {

      // If there's a sub select in an expression in the 'SET' clause then
      // throw an error.
      for (int i = 0; i < column_sets.size(); ++i) {
        Assignment assignment = (Assignment) column_sets.get(i);
        Expression exp = assignment.getExpression();
        List elem_list = exp.allElements();
        for (int n = 0; n < elem_list.size(); ++n) {
          Object ob = elem_list.get(n);
          if (ob instanceof Select) {
            throw new DatabaseException(
                                 "Illegal to have sub-select in SET clause.");
          }
        }

        // Resolve the column names in the columns set.
        Variable v = assignment.getVariable();
        Variable resolved_v = resolveVariableName(v);
        v.set(resolved_v);
        resolveExpression(assignment.getExpression());
      }

    }

    // Resolve all tables linked to this
    TableName[] linked_tables =
                             database.queryTablesRelationallyLinkedTo(tname);
    relationally_linked_tables = new ArrayList(linked_tables.length);
    for (int i = 0; i < linked_tables.length; ++i) {
      relationally_linked_tables.add(database.getTable(linked_tables[i]));
    }

  }

  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);

    // Check that this user has privs to insert into the table.
    if (!user.canInsertIntoTable(database, tname, col_var_list)) {
      throw new UserAccessException(
         "User not permitted to insert in to table: " + table_name);
    }

    // Are we inserting from a select statement or from a 'set' assignment
    // list?
    int insert_count = 0;

    if (from_values) {
      // Set each row from the VALUES table,
      for (int i = 0; i < values_list.size(); ++i) {
        Expression[] exps = (Expression[]) values_list.get(i);
        RowData row_data = insert_table.createRowDataObject(context);
//        System.out.println(exps);
//        for (int n = 0; n < exps.length; ++n) {
//          System.out.println(exps[n].elementAt(0).getClass());
//        }
        row_data.setupEntire(col_index_list, exps, context);
        insert_table.add(row_data);
        ++insert_count;
      }
    }
    else if (from_select) {
      // Insert rows from the result select table.
      Table result = prepared_select.evaluate();
      if (result.getColumnCount() != col_index_list.length) {
        throw new DatabaseException(
                "Number of columns in result don't match columns to insert.");
      }

      // Copy row list into an intermediate IntegerVector list.
      // (A RowEnumeration for a table being modified is undefined).
      IntegerVector row_list = new IntegerVector();
      RowEnumeration enum = result.rowEnumeration();
      while (enum.hasMoreRows()) {
        row_list.addInt(enum.nextRowIndex());
      }

      // For each row of the select table.
      int sz = row_list.size();
      for (int i = 0; i < sz; ++i) {
        int rindex = row_list.intAt(i);
        RowData row_data = insert_table.createRowDataObject(context);
        for (int n = 0; n < col_index_list.length; ++n) {
          DataCell cell = result.getCellContents(n, rindex);
          row_data.setColumnData(col_index_list[n], cell);
        }
        row_data.setDefaultForRest(context);
        insert_table.add(row_data);
        ++insert_count;
      }
    }
    else if (from_set) {
      // Insert rows from the set assignments.
      RowData row_data = insert_table.createRowDataObject(context);
      Assignment[] assignments = (Assignment[])
                    column_sets.toArray(new Assignment[column_sets.size()]);
      row_data.setupEntire(assignments, context);
      insert_table.add(row_data);
      ++insert_count;
    }

    // Notify TriggerManager that we've just done an update.
    if (insert_count > 0) {
      database.notifyTriggerEvent(new TriggerEvent(
                        TriggerEvent.INSERT, tname.toString(), insert_count));
    }

    // Return the number of rows we inserted.
    return FunctionTable.resultTable(context, insert_count);
  }

  public boolean isExclusive() {
    // If table name starts with 'sUSR' then we need to be exclusive.
    String table_name = (String) cmd.getObject("table_name");
    return table_name.startsWith("sUSR") ||
           table_name.startsWith("SYS_INFO.sUSR");
  }

  public List readsFromTables() {
    ArrayList read_list = new ArrayList();

    // If we are inserting from the 'SET' list,
    if (from_set) {
      // Look at sub-selects in expressions (this is actually illegal syntax
      // to put sub-selects in 'SET' but we check em anyway just incase this
      // changes).
      for (int i = 0; i < column_sets.size(); ++i) {
        Assignment a = (Assignment) column_sets.get(i);
        Expression exp = a.getExpression();
        List elem_list = exp.allElements();
        for (int n = 0; n < elem_list.size(); ++n) {
          Object ob = elem_list.get(n);
          if (ob instanceof Select) {
            read_list.addAll(((Select) ob).readsFromTables());
          }
        }
      }
    }
    else if (from_values) {
      // Look at sub-selects in expressions (this is actually illegal syntax
      // to put sub-selects in the expressions list but we check em anyway
      // just incase this changes).

      for (int i = 0; i < values_list.size(); ++i) {
        Expression[] exps = (Expression[]) values_list.get(i);
        for (int n = 0; n < exps.length; ++n) {

          Expression exp = exps[n];
          List elem_list = exp.allElements();
          for (int p = 0; p < elem_list.size(); ++p) {
            Object ob = elem_list.get(p);
            if (ob instanceof Select) {
              read_list.addAll(((Select) ob).readsFromTables());
            }
          }
        }
      }

    }
    // If we are inserting from the 'SELECT' list,
    else if (from_select) {
      read_list.addAll(prepared_select.readsFromTables());
    }
    else {
      throw new Error("Unable to determine where tables to read from are.");
    }

    // Add tables relationally linked
    read_list.addAll(relationally_linked_tables);

    return read_list;
  }

  public List writesToTables() {
    ArrayList write_list = new ArrayList();
    write_list.add(insert_table);
    return write_list;
  }

}
