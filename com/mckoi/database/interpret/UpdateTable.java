/**
 * com.mckoi.database.interpret.UpdateTable  14 Sep 2001
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

/**
 * The instance class that stores all the information about an update
 * statement for processing.
 *
 * @author Tobias Downer
 */

public class UpdateTable extends Statement {

  /**
   * The name the table that we are to update.
   */
  String table_name;

  /**
   * An array of Assignment objects which represent what we are changing.
   */
  ArrayList column_sets;

  /**
   * If the update statement has a 'where' clause, then this is set here.  If
   * it has no 'where' clause then we apply to the entire table.
   */
  SearchExpression where_condition;

  /**
   * The limit of the number of rows that are updated by this statement.  A
   * limit of -1 means there is no limit.
   */
  int limit = -1;

  /**
   * Tables that are relationally linked to the table being inserted into, set
   * after 'prepare'.  This is used to determine the tables we need to read
   * lock because we need to validate relational constraints on the tables.
   */
  private ArrayList relationally_linked_tables;


  // -----

  /**
   * The DataTable we are updating.
   */
  private DataTable update_table;

//  /**
//   * The TableSet containing the table we are updating.
//   */
//  private TableSet root_table_set;
//
//  /**
//   * The JoiningSet that describes how the tables are joined.
//   */
//  private JoiningSet join_set;

  /**
   * The TableName object set during 'prepare'.
   */
  private TableName tname;

  /**
   * The plan for the set of records we are updating in this query.
   */
  private QueryPlanNode plan;




//  /**
//   * Adds a new column/expression to set in this update.
//   */
//  void addSet(String column, Expression expression) {
//    Assignment assignment = new Assignment(Variable.resolve(column),
//                                           expression);
//    column_sets.add(assignment);
//  }

  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {

    table_name = (String) cmd.getObject("table_name");
    column_sets = (ArrayList) cmd.getObject("assignments");
    where_condition = (SearchExpression) cmd.getObject("where_clause");
    limit = cmd.getInt("limit");

    // ---

    // Resolve the TableName object.
    tname = resolveTableName(table_name, database);
    // Get the table we are updating
    update_table = database.getTable(tname);

    // Form a TableSelectExpression that represents the select on the table
    TableSelectExpression select_expression = new TableSelectExpression();
    // Create the FROM clause
    select_expression.from_clause.addTable(table_name);
    // Set the WHERE clause
    select_expression.where_clause = where_condition;

    // Generate the TableExpressionFromSet hierarchy for the expression,
    TableExpressionFromSet from_set =
                       Planner.generateFromSet(select_expression, database);
    // Form the plan
    plan = Planner.formQueryPlan(database, select_expression, from_set, null);

    // Resolve the variables in the assignments.
    for (int i = 0; i < column_sets.size(); ++i) {
      Assignment assignment = (Assignment) column_sets.get(i);
      Variable orig_var = assignment.getVariable();
      Variable new_var = from_set.resolveReference(orig_var);
      if (new_var == null) {
        throw new StatementException("Reference not found: " + orig_var);
      }
      orig_var.set(new_var);
      assignment.prepareExpressions(from_set.expressionQualifier());
    }

    // Resolve all tables linked to this
    TableName[] linked_tables =
                             database.queryTablesRelationallyLinkedTo(tname);
    relationally_linked_tables = new ArrayList(linked_tables.length);
    for (int i = 0; i < linked_tables.length; ++i) {
      relationally_linked_tables.add(database.getTable(linked_tables[i]));
    }







//    root_table_set = new TableSet();
//    join_set = new JoiningSet();
//
//    // Resolve the TableName object.
//    tname = resolveTableName(table_name, database);
//
//    // Add the table to the set of tables being joined.
//    join_set.addTable(tname);
//
//    // Get the table we are updating
//    update_table = database.getTable(tname);
//    // Mark this table as 'selected from' within the transaction.
//    database.addSelectedFromTable(tname);
//    addTable(new FromTableDirectSource(update_table));
//    root_table_set.addTable(tname, update_table);
//
//    // Resolve the column names in the columns set.
//    for (int i = 0; i < column_sets.size(); ++i) {
//      Assignment assignment = (Assignment) column_sets.get(i);
//      Variable orig_var = assignment.getVariable();
//      orig_var.set(resolveVariableName(orig_var));
//      resolveExpression(assignment.getExpression());
//    }
//
//    // Resolve references in the 'where' condition.
//    where_condition.resolveColumnNames(this);
//
//    // If there's a sub select in an expression in the 'SET' clause then
//    // throw an error.
//    for (int i = 0; i < column_sets.size(); ++i) {
//      Assignment assignment = (Assignment) column_sets.get(i);
//      Expression exp = assignment.getExpression();
//      List elem_list = exp.allElements();
//      for (int n = 0; n < elem_list.size(); ++n) {
//        Object ob = elem_list.get(n);
//        if (ob instanceof Select) {
//          throw new DatabaseException(
//                            "Illegal to have sub-select in SET clause.");
//        }
//      }
//    }
//
//    // Resolve all tables linked to this
//    TableName[] linked_tables =
//                             database.queryTablesRelationallyLinkedTo(tname);
//    relationally_linked_tables = new ArrayList(linked_tables.length);
//    for (int i = 0; i < linked_tables.length; ++i) {
//      relationally_linked_tables.add(database.getTable(linked_tables[i]));
//    }

  }

  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);

    // Generate a list of Variable objects that represent the list of columns
    // being changed.
    Variable[] col_var_list = new Variable[column_sets.size()];
    for (int i = 0; i < col_var_list.length; ++i) {
      Assignment assign = (Assignment) column_sets.get(i);
      col_var_list[i] = assign.getVariable();
    }

    // Check that this user has privs to update the table.
    if (!user.canUpdateTable(database, tname, col_var_list)) {
      throw new UserAccessException(
         "User not permitted to update table: " + table_name);
    }

    // Evaluate the plan to find the update set.
    Table update_set = plan.evaluate(context);

    // Make an array of assignments
    Assignment[] assign_list = new Assignment[column_sets.size()];
    assign_list = (Assignment[]) column_sets.toArray(assign_list);
    // Update the data table.
    int update_count = update_table.update(context,
                                           update_set, assign_list, limit);

    // Notify TriggerManager that we've just done an update.
    if (update_count > 0) {
      database.notifyTriggerEvent(new TriggerEvent(
                        TriggerEvent.UPDATE, tname.toString(), update_count));
    }

    // Return the number of rows we updated.
    return FunctionTable.resultTable(context, update_count);







//    // Generate a list of Variable objects that represent the list of columns
//    // being changed.
//    Variable[] col_var_list = new Variable[column_sets.size()];
//    for (int i = 0; i < col_var_list.length; ++i) {
//      Assignment assign = (Assignment) column_sets.get(i);
//      col_var_list[i] = assign.getVariable();
//    }
//
//    // Check that this user has privs to update the table.
//    if (!user.canUpdateTable(database, tname, col_var_list)) {
//      throw new UserAccessException(
//         "User not permitted to update table: " + table_name);
//    }
//
//    // Apply the 'where' condition on our root table set to get a final result.
//    TableSet table_set = where_condition.evaluate(root_table_set.copy(),
//                                                  join_set);
//    Table update_set = table_set.getTable();
//
//    // Make an array of assignments
//    Assignment[] assign_list = new Assignment[column_sets.size()];
//    assign_list = (Assignment[]) column_sets.toArray(assign_list);
//    // Update the data table.
//    int update_count = update_table.update(update_set, assign_list, limit);
//
//    // Notify TriggerManager that we've just done an update.
//    if (update_count > 0) {
//      database.notifyTriggerEvent(new TriggerEvent(
//                        TriggerEvent.UPDATE, tname.toString(), update_count));
//    }
//
//    // Return the number of rows we updated.
//    return FunctionTable.resultTable(update_count);
  }

  public boolean isExclusive() {
    // If table name starts with 'sUSR' then we need to be exclusive.
    String table_name = (String) cmd.getObject("table_name");
    return table_name.startsWith("sUSR") ||
           table_name.startsWith("SYS_INFO.sUSR");
  }

  public List readsFromTables() {
    ArrayList all_tables = new ArrayList();
    all_tables = plan.discoverTableNames(all_tables);
    int sz = all_tables.size();
    ArrayList result = new ArrayList(sz);
    for (int i = 0; i < sz; ++i) {
      result.add(database.getTable((TableName) all_tables.get(i)));
    }

    // Add tables relationally linked
    result.addAll(relationally_linked_tables);

    return result;

//    ArrayList read_list = new ArrayList();
//
//    // Look at sub-selects in expressions (this is actually illegal syntax
//    // to put sub-selects in 'SET' but we check em anyway just incase this
//    // changes).
//    for (int i = 0; i < column_sets.size(); ++i) {
//      Assignment a = (Assignment) column_sets.get(i);
//      Expression exp = a.getExpression();
//      List elem_list = exp.allElements();
//      for (int n = 0; n < elem_list.size(); ++n) {
//        Object ob = elem_list.get(n);
//        if (ob instanceof Select) {
//          read_list.addAll(((Select) ob).readsFromTables());
//        }
//      }
//    }
//
//    // The list of expression in the where condition to check for sub-selects.
//    List cond_expressions = where_condition.allElements();
//    for (int i = 0; i < cond_expressions.size(); ++i) {
//      Object ob = cond_expressions.get(i);
//      if (ob instanceof Select) {
//        read_list.addAll(((Select) ob).readsFromTables());
//      }
//    }
//
//    // Add tables relationally linked
//    read_list.addAll(relationally_linked_tables);
//
//    return read_list;
  }

  public List writesToTables() {
    ArrayList write_list = new ArrayList();
    write_list.add(update_table);
    return write_list;
  }

}
