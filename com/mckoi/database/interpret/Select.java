/**
 * com.mckoi.database.interpret.Select  09 Sep 2001
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

import com.mckoi.database.*;
import com.mckoi.debug.*;
import com.mckoi.util.IntegerVector;
import java.util.Set;
import java.util.List;
import java.util.Vector;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Logic for interpreting an SQL SELECT statement.
 *
 * @author Tobias Downer
 */

public class Select extends Statement {

  /**
   * The TableSelectExpression representing the select query itself.
   */
  private TableSelectExpression select_expression;

  /**
   * The list of all columns to order by. (ByColumn)
   */
  private ArrayList order_by;

  /**
   * The list of columns in the 'order_by' clause fully resolved.
   */
  private Variable[] order_cols;

  /**
   * The plan for evaluating this select expression.
   */
  private QueryPlanNode plan;








  /**
   * Prepares the select statement with a Database object.  This sets up
   * internal state so that it correctly maps to a database.  Also, this
   * checks format to ensure there are no run-time syntax problems.  This must
   * be called because we 'evaluate' the statement.
   * <p>
   * NOTE: Care must be taken to ensure that all methods called here are safe
   *   in as far as modifications to the data occuring.  The rules for
   *   safety should be as follows.  If the database is in EXCLUSIVE mode,
   *   then we need to wait until it's switched back to SHARED mode
   *   before this method is called.
   *   All collection of information done here should not involve any table
   *   state info. except for column count, column names, column types, etc.
   *   Queries such as obtaining the row count, selectable scheme information,
   *   and certainly 'getCellContents' must never be called during prepare.
   *   When prepare finishes, the affected tables are locked and the query ia
   *   safe to 'evaluate' at which time table state is safe to inspect.
   * <p>
   *   No locking is performed by this class.  It must be performed by the
   *   callee.
   */
  public void prepare() throws DatabaseException {
    DatabaseConnection db = database;

    // Prepare this object from the StatementTree,
    // The select expression itself
    select_expression =
                  (TableSelectExpression) cmd.getObject("table_expression");
    // The order by information
    order_by = (ArrayList) cmd.getObject("order_by");

//    // Does the 'base' table note have a composite table?
//    if (select_expression.next_composite == null) {
//      // No, so it's fairly simple to resolve this,
    // Generate the TableExpressionFromSet hierarchy for the expression,
    TableExpressionFromSet from_set =
                             Planner.generateFromSet(select_expression, db);

    // Form the plan
    plan = Planner.formQueryPlan(db, select_expression, from_set, order_by);
//    }
//    else {
//      // Generate the TableExpressionFromSet hierarchy for the expression,
//      TableExpressionFromSet from_set =
//                             Planner.generateFromSet(select_expression, db);
//
//      // Form the left plan
//      QueryPlanNode left_plan =
//                Planner.formQueryPlan(db, select_expression, from_set, null);
//
//      // Do we have a composite to this table?
//      while (select_expression.next_composite != null) {
//
//        TableSelectExpression old_expr = select_expression;
//
//        select_expression = select_expression.next_composite;
//        // Generate the TableExpressionFromSet hierarchy for the expression,
//        from_set = Planner.generateFromSet(select_expression, db);
//
//        // Form the right plan
//        QueryPlanNode right_plan =
//                Planner.formQueryPlan(db, select_expression, from_set, null);
//
//        // Merge into a new left plan
//        left_plan = new QueryPlan.CompositeNode(left_plan, right_plan,
//                     old_expr.composite_function, old_expr.is_composite_all);
//      }
//
//      plan = left_plan;
//    }





//    TableName FUNCTION_TABLE = new TableName("FUNCTIONTABLE");
//
//    // Sort on the ORDER BY clause
//    if (order_by.size() > 0) {
//      int sz = order_by.size();
//      Variable[] order_list = new Variable[sz];
//      boolean[] ascending_list = new boolean[sz];
//
//      ArrayList function_orders = new ArrayList();
//
//      for (int i = 0; i < sz; ++i) {
//        ByColumn column = (ByColumn) order_by.get(i);
//        Expression exp = column.exp;
//        ascending_list[i] = column.ascending;
//        Variable v = exp.getVariable();
//        if (v != null) {
//          Variable new_v = from_set.resolveReference(v);
//          if (new_v == null) {
//            throw new StatementException(
//                                   "Can not resolve ORDER BY variable: " + v);
//          }
//          order_list[i] = new_v;
//        }
//        else {
//          // Otherwise we must be ordering by an expression such as
//          // '0 - a'.
//
//          // Resolve the expression,
//          exp.prepare(from_set.expressionQualifier());
//
//          // The new ordering functions are called 'FUNCTIONTABLE.#ORDER-n'
//          // where n is the number of the ordering expression.
//          order_list[i] =
//              new Variable(FUNCTION_TABLE, "#ORDER-" + function_orders.size());
//          function_orders.add(exp);
//        }
//      }
//
//      // If there are functional orderings,
//      // For this we must define a new FunctionTable with the expressions,
//      // then order by those columns, and then use another SubsetNode
//      // query node.
//      int fsz = function_orders.size();
//      if (fsz > 0) {
//        Expression[] funs = new Expression[fsz];
//        String[] fnames = new String[fsz];
//        for (int n = 0; n < fsz; ++n) {
//          funs[n] = (Expression) function_orders.get(n);
//          fnames[n] = "#ORDER-" + n;
//        }
//
//        // HACK: We assume that the top plan is a QueryPlan.SubsetNode and
//        //   we use the information from it to create a new SubsetNode that
//        //   doesn't include the functional orders we have attached here.
//        QueryPlan.SubsetNode top_subset_node = (QueryPlan.SubsetNode) plan;
//        Variable[] mapped_names = top_subset_node.getNewColumnNames();
//
//        // Defines the sort functions
//        plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
//        // Then plan the sort
//        plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
//        // Then plan the subset
//        plan = new QueryPlan.SubsetNode(plan, mapped_names, mapped_names);
//
//      }
//      else {
//        // No functional orders so we only need to sort by the columns
//        // defined.
//        plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
//      }
//
//    }




//
//    // Create the SelectQueriable object
//    select_queriable = new SelectQueriable(select_expression);
//
//    // First phase prepare
//    select_queriable.prepare1(db);
//    // Second phase prepare
//    select_queriable.prepare2(db);
//
//    // Set up the order columns and resolve them via aliasing and table lookup.
//    order_cols = new Variable[order_by.size()];
//    for (int i = 0; i < order_by.size(); ++i) {
//      ByColumn col = (ByColumn) order_by.get(i);
//      Expression exp = col.exp;
//      Variable v = exp.getVariable();
//      // If expression is not a variable then we need to create a function
//      // column
//      if (v == null) {
//        SelectColumn nscol = new SelectColumn();
//        nscol.expression = exp;
//        select_queriable.prepareSelectColumn(nscol);
//        col.name = nscol.internal_name;
//      }
//      else {
//        // A variable so use variable as our order by name,
//        col.name = select_queriable.qualifyVariable(v);
//      }
//      order_cols[i] = col.name;
//    }
//
//    // PENDING: Any aggregate functions in the HAVING clause must be resolved
//    // to a function and an alias.
//



  }


  /**
   * Evaluates the select statement with the given Database context.
   */
  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);

    boolean error = true;
    try {
      Table t = plan.evaluate(context);
      error = false;
      return t;
    }
    finally {
      // If an error occured, dump the query plan to the debug log.
      // Or just dump the query plan if debug level = INFORMATION
      if (Debug().isInterestedIn(Lvl.INFORMATION) ||
          (error && Debug().isInterestedIn(Lvl.WARNING))) {
        StringBuffer buf = new StringBuffer();
        plan.debugString(0, buf);

        Debug().write(Lvl.WARNING, this,
                    "Query Plan debug:\n" +
                    buf.toString());
      }
    }




//    // The list of tables touched by the query
//    List all_tables = select_queriable.getAllTableNamesTouched();
//
//    // Check the connection has privs to select from the table(s).
//    for (int i = 0; i < all_tables.size(); ++i) {
//      // This needs to be implemented to contain the columns read in a table.
//      Variable[] cols_read_in_table = new Variable[0];
//      TableName tname = (TableName) all_tables.get(i);
//      if (!user.canSelectFromTable(database, tname, cols_read_in_table)) {
//          throw new UserAccessException(
//             "User not permitted to select from table: " + tname);
//      }
//    }
//
//    // ---- Execute the query ----
//
//    // Run the query.  The VariableResolver is 'null' because there can't
//    // possibly be any corellated queries from the root query.
//    Table result_table = select_queriable.evaluateQuery(null);
//
//    // Order the result as appropriate
//
//    // How we handle ascending/descending order
//    // ----------------------------------------
//    // Internally to the database, all columns are naturally ordered in
//    // ascending order (start at lowest and end on highest).  When a column
//    // is ordered in descending order, a fast way to achieve this is to take
//    // the ascending set and reverse it.  This works for single columns,
//    // however some thought is required for handling multiple column.  We
//    // order columns from RHS to LHS.  If LHS is descending then this will
//    // order the RHS incorrectly if we leave as is.  Therefore, we must do some
//    // pre-processing that looks ahead on any descending orders and reverses
//    // the order of the columns to the right.  This pre-processing is done in
//    // the first pass.
//
//    for (int n = 0; n < order_by.size() - 1; ++n) {
//      ByColumn column = (ByColumn) order_by.get(n);
//      if (!column.ascending) {    // if descending...
//        // Reverse order of all columns to the right...
//        for (int p = n + 1; p < order_by.size(); ++p) {
//          column = (ByColumn) order_by.get(p);
//          column.ascending = !column.ascending;
//        }
//      }
//    }
//
//    // Sort the results by the 'order_by' clause in reverse-safe order.
//    for (int n = order_by.size() - 1; n >= 0; --n) {
//      ByColumn column = (ByColumn) order_by.get(n);
//
//      result_table = result_table.orderByColumn(column.name, column.ascending);
//    }
//
//    // Return the result table.
//    return result_table;

  }

  /**
   * Select statement doesn't write exclusive lock on database.
   */
  public boolean isExclusive() {
    return false;
  }

  /**
   * Returns the name of all tables that this select touches including any
   * sub-select statements.  Must be called after the 'prepare' method.
   * The list contains objects of type 'DataTable'
   */
  public List readsFromTables() {
    ArrayList all_tables = new ArrayList();
    all_tables = plan.discoverTableNames(all_tables);
//    System.out.println(all_tables);
    int sz = all_tables.size();
    ArrayList result = new ArrayList(sz);
    for (int i = 0; i < sz; ++i) {
      result.add(database.getTable((TableName) all_tables.get(i)));
    }
    return result;
  }

  /**
   * Select statements don't write to any tables.
   */
  public List writesToTables() {
    return Collections.EMPTY_LIST;
  }

  /**
   * Outputs information for debugging.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[ SELECT: expression=");
    buf.append(select_expression.toString());
    buf.append(" ORDER_BY=");
    buf.append(order_by);
    buf.append(" ]");
    return new String(buf);
  }

}
