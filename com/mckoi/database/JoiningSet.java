/**
 * com.mckoi.database.JoiningSet  20 Sep 2000
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

import java.util.ArrayList;

/**
 * Used in TableSet to describe how we naturally join the tables together.
 * This is used when the TableSet has evaluated the search condition and it
 * is required for any straggling tables to be naturally joined.  In SQL,
 * these joining types are specified in the FROM clause.
 * <p>
 * For example,<br><pre>
 *   FROM table_a LEFT OUTER JOIN table_b ON ( table_a.id = table_b.id ), ...
 * </pre><p>
 * A ',' should donate an INNER_JOIN in an SQL FROM clause.
 *
 * @author Tobias Downer
 */

public class JoiningSet implements java.io.Serializable, Cloneable {

  /**
   * Statics for Join Types.
   */
  // Donates a standard inner join (in SQL, this is ',' in the FROM clause)
  public final static int INNER_JOIN       = 1;
  // Left Outer Join,
  public final static int LEFT_OUTER_JOIN  = 2;
  // Right Outer Join,
  public final static int RIGHT_OUTER_JOIN = 3;
  // Full Outer Join
  public final static int FULL_OUTER_JOIN  = 4;

  /**
   * The list of tables we are joining together a JoinPart object that
   * represents how the tables are joined.
   */
  private ArrayList join_set;

  /**
   * Constructs the JoiningSet.
   */
  public JoiningSet() {
    join_set = new ArrayList();
  }

  /**
   * Resolves the schema of tables in this joining set.  This runs through
   * each table in the joining set and if the schema has not been set for the
   * table then it attempts to resolve it against the given DatabaseConnection
   * object.  This would typically be called in the preparation of a statement.
   */
  public void prepare(DatabaseConnection connection) {
  }

  /**
   * Adds a new table into the set being joined.  The table name should be the
   * unique name that distinguishes this table in the TableSet.
   */
  public void addTable(TableName table_name) {
    join_set.add(table_name);
  }

  /**
   * Hack, add a joining type to the previous entry from the end.  This is
   * an artifact of how joins are parsed.
   */
  public void addPreviousJoin(int type, Expression on_expression) {
    join_set.add(join_set.size() - 1, new JoinPart(type, on_expression));
  }

  /**
   * Adds a joining type to the set, and an 'on' expression.
   */
  public void addJoin(int type, Expression on_expression) {
    join_set.add(new JoinPart(type, on_expression));
  }

  /**
   * Adds a joining type to the set with no 'on' expression.
   */
  public void addJoin(int type) {
    join_set.add(new JoinPart(type));
  }

  /**
   * Returns the number of tables that are in this set.
   */
  public int getTableCount() {
    return (join_set.size() + 1) / 2;
  }

  /**
   * Returns the first table in the join set.
   */
  public TableName getFirstTable() {
    return getTable(0);
  }

  /**
   * Returns table 'n' in the result set where table 0 is the first table in
   * the join set.
   */
  public TableName getTable(int n) {
    return (TableName) join_set.get(n * 2);
  }

  /**
   * Sets the table at the given position in this joining set.
   */
  private void setTable(int n, TableName table) {
    join_set.set(n * 2, table);
  }

  /**
   * Returns the type of join after table 'n' in the set.  An example
   * of using this;<p><pre>
   *
   * String table1 = joins.getFirstTable();
   * for (int i = 0; i < joins.getTableCount() - 1; ++i) {
   *   int type = joins.getJoinType(i);
   *   String table2 = getTable(i + 1);
   *   // ... Join table1 and table2 ...
   *   table1 = table2;
   * }
   *
   * </pre>
   */
  public int getJoinType(int n) {
    return ((JoinPart) join_set.get((n * 2) + 1)).type;
  }

  /**
   * Returns the ON Expression for the type of join after table 'n' in the
   * set.
   */
  public Expression getOnExpression(int n) {
    return ((JoinPart) join_set.get((n * 2) + 1)).on_expression;
  }

  /**
   * Performs a deep clone on this object.
   */
  public Object clone() throws CloneNotSupportedException {
    JoiningSet v = (JoiningSet) super.clone();
    int size = join_set.size();
    ArrayList cloned_join_set = new ArrayList(size);
    v.join_set = cloned_join_set;

    for (int i = 0; i < size; ++i) {
      Object element = join_set.get(i);
      if (element instanceof TableName) {
        // immutable so leave alone
      }
      else if (element instanceof JoinPart) {
        element = ((JoinPart) element).clone();
      }
      else {
        throw new CloneNotSupportedException(element.getClass().toString());
      }
      cloned_join_set.add(element);
    }

    return v;
  }



  // ---------- Inner classes ----------

  public static class JoinPart implements java.io.Serializable, Cloneable {

    /**
     * The type of join.  Either LEFT_OUTER_JOIN,
     * RIGHT_OUTER_JOIN, FULL_OUTER_JOIN, INNER_JOIN.
     */
    int type;

    /**
     * The expression that we are joining on (eg. ON clause in SQL).  If there
     * is no ON expression (such as in the case of natural joins) then this is
     * null.
     */
    Expression on_expression;

    /**
     * Constructs the JoinPart.
     */
    public JoinPart(int type, Expression on_expression) {
      this.type = type;
      this.on_expression = on_expression;
    }

    public JoinPart(int type) {
      this(type, null);
    }

    public Object clone() throws CloneNotSupportedException {
      JoinPart v = (JoinPart) super.clone();
      if (on_expression != null) {
        v.on_expression = (Expression) on_expression.clone();
      }
      return v;
    }

  }

}
