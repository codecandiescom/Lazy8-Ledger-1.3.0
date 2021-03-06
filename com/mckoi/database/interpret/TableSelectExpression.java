/**
 * com.mckoi.database.interpret.TableSelectExpression  30 Oct 2001
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
import java.util.*;

/**
 * A container object for the a table select expression, eg.
 * <p><pre>
 *               SELECT [columns]
 *                 FROM [tables]
 *                WHERE [search_clause]
 *             GROUP BY [column]
 *               HAVING [search_clause]
 * [composite_function] [table_select_expression]
 * </pre><p>
 * Note that a TableSelectExpression can be nested in the various clauses of
 * this object.
 *
 * @author Tobias Downer
 */

public class TableSelectExpression
            implements java.io.Serializable, StatementTreeObject, Cloneable {

  /**
   * True if we only search for distinct elements.
   */
  public boolean distinct = false;

  /**
   * The list of columns to select from.
   * (SelectColumn)
   */
  public ArrayList columns = new ArrayList();

  /**
   * The from clause.
   */
  public FromClause from_clause = new FromClause();

  /**
   * The where clause.
   */
  public SearchExpression where_clause = new SearchExpression();



  /**
   * The list of columns to group by.
   * (ByColumn)
   */
  public ArrayList group_by = new ArrayList();

  /**
   * The group max variable or null if no group max.
   */
  public Variable group_max = null;

  /**
   * The having clause.
   */
  public SearchExpression having_clause = new SearchExpression();


  /**
   * If there is a composite function this is set to the composite enumeration
   * from CompositeTable.
   */
  int composite_function = -1;  // (None)

  /**
   * If this is an ALL composite (no removal of duplicate rows) it is true.
   */
  boolean is_composite_all;

  /**
   * The composite table itself.
   */
  TableSelectExpression next_composite;

  /**
   * Constructor.
   */
  public TableSelectExpression() {
  }

  /**
   * Chains a new composite function to this expression.  For example, if
   * this expression is a UNION ALL with another expression it would be
   * set through this method.
   */
  public void chainComposite(TableSelectExpression expression,
                             String composite, boolean is_all) {
    this.next_composite = expression;
    composite = composite.toLowerCase();
    if (composite.equals("union")) {
      composite_function = CompositeTable.UNION;
    }
    else if (composite.equals("intersect")) {
      composite_function = CompositeTable.INTERSECT;
    }
    else if (composite.equals("except")) {
      composite_function = CompositeTable.EXCEPT;
    }
    else {
      throw new Error("Don't understand composite function '" +
                      composite + "'");
    }
    is_composite_all = is_all;
  }




  // ---------- Implemented from StatementTreeObject ----------

  /**
   * Prepares all the expressions in the list.
   */
  private static void prepareAllInList(
          List list, ExpressionPreparer preparer) throws DatabaseException {
    for (int n = 0; n < list.size(); ++n) {
      StatementTreeObject ob = (StatementTreeObject) list.get(n);
      ob.prepareExpressions(preparer);
    }
  }


  public void prepareExpressions(ExpressionPreparer preparer)
                                               throws DatabaseException {
    prepareAllInList(columns, preparer);
    from_clause.prepareExpressions(preparer);
    where_clause.prepareExpressions(preparer);
    prepareAllInList(group_by, preparer);
    having_clause.prepareExpressions(preparer);

    // Go to the next chain
    if (next_composite != null) {
      next_composite.prepareExpressions(preparer);
    }
  }

  public Object clone() throws CloneNotSupportedException {
    TableSelectExpression v = (TableSelectExpression) super.clone();
    if (columns != null) {
      v.columns = (ArrayList) StatementTree.cloneSingleObject(columns);
    }
    if (from_clause != null) {
      v.from_clause = (FromClause) from_clause.clone();
    }
    if (where_clause != null) {
      v.where_clause = (SearchExpression) where_clause.clone();
    }
    if (group_by != null) {
      v.group_by = (ArrayList) StatementTree.cloneSingleObject(group_by);
    }
    if (group_max != null) {
      v.group_max = (Variable) group_max.clone();
    }
    if (having_clause != null) {
      v.having_clause = (SearchExpression) having_clause.clone();
    }
    if (next_composite != null) {
      v.next_composite = (TableSelectExpression) next_composite.clone();
    }
    return v;
  }

}
