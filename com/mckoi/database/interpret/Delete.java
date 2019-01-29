/**
 * com.mckoi.database.interpret.Delete  14 Sep 2001
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
 * Logic for the DELETE FROM SQL statement.
 *
 * @author Tobias Downer
 */

public class Delete extends Statement {

  /**
   * The name the table that we are to delete from.
   */
  String table_name;

  /**
   * If the delete statement has a 'where' clause, then this is set here.  If
   * it has no 'where' clause then we apply to the entire table.
   */
  SearchExpression where_condition;

  /**
   * The limit of the number of rows that are updated by this statement.  A
   * limit of < 0 means there is no limit.
   */
  int limit = -1;

  // -----

  /**
   * The DataTable we are deleting from .
   */
  private DataTable update_table;

//  /**
//   * The TableSet containing the table we are deleting from.
//   */
//  private TableSet root_table_set;
//
//  /**
//   * The JoiningSet that describes how the tables are joined.
//   */
//  private JoiningSet join_set;

  /**
   * The TableName object of the table being created.
   */
  private TableName tname;

  /**
   * Tables that are relationally linked to the table being inserted into, set
   * after 'prepare'.  This is used to determine the tables we need to read
   * lock because we need to validate relational constraints on the tables.
   */
  private ArrayList relationally_linked_tables;

  /**
   * The plan for the set of records we are deleting in this query.
   */
  private QueryPlanNode plan;





  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {

    // Get variables from the model.
    table_name = (String) cmd.getObject("table_name");
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

    // Resolve all tables linked to this
    TableName[] linked_tables =
                             database.queryTablesRelationallyLinkedTo(tname);
    relationally_linked_tables = new ArrayList(linked_tables.length);
    for (int i = 0; i < linked_tables.length; ++i) {
      relationally_linked_tables.add(database.getTable(linked_tables[i]));
    }




//    root_table_set = new TableSet();
//
//    tname = resolveTableName(table_name, database);
//
//    // Set the joining set
//    join_set = new JoiningSet();
//    join_set.addTable(tname);
//
//    // Get the table we are deleting from
//    update_table = database.getTable(tname);
//    // Mark this table as 'selected from' within the transaction.
//    database.addSelectedFromTable(tname);
//    addTable(new FromTableDirectSource(update_table));
//    root_table_set.addTable(tname, update_table);
//
//    // Resolve references in the 'where' condition.
//    where_condition.resolveColumnNames(this);
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

    // Check that this user has privs to delete from the table.
    if (!user.canDeleteFromTable(database, tname)) {
      throw new UserAccessException(
         "User not permitted to delete from table: " + table_name);
    }

    // Evaluates the delete statement...

    // Evaluate the plan to find the update set.
    Table delete_set = plan.evaluate(context);

    // Delete from the data table.
    int delete_count = update_table.delete(delete_set, limit);

    // Notify TriggerManager that we've just done an update.
    if (delete_count > 0) {
      database.notifyTriggerEvent(new TriggerEvent(
                        TriggerEvent.DELETE, tname.toString(), delete_count));
    }

    // Return the number of columns we deleted.
    return FunctionTable.resultTable(context, delete_count);



//    // Check that this user has privs to delete from the table.
//    if (!user.canDeleteFromTable(database, tname)) {
//      throw new UserAccessException(
//         "User not permitted to delete from table: " + table_name);
//    }
//
//    // Evaluates the delete statement...
//
//    // Apply the 'where' condition on our root table set to get a final result.
//    TableSet table_set = where_condition.evaluate(root_table_set.copy(),
//                                                  join_set);
//    Table delete_set = table_set.getTable();
//
//    // Delete from the data table.
//    int delete_count = update_table.delete(delete_set, limit);
//
//    // Notify TriggerManager that we've just done an update.
//    if (delete_count > 0) {
//      database.notifyTriggerEvent(new TriggerEvent(
//                        TriggerEvent.DELETE, tname.toString(), delete_count));
//    }
//
//    // Return the number of columns we deleted.
//    return FunctionTable.resultTable(delete_count);

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
    // Just the table we are deleting from...
    ArrayList write_list = new ArrayList();
    write_list.add(update_table);
    return write_list;
  }

}
