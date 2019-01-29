/**
 * com.mckoi.database.global.AggregateFunction  11 May 1998
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

package com.mckoi.database.global;

/**
 * This object encapsulates the information to represents an aggregate
 * function.  An aggregate function is a function to perform on a table.  The
 * functions are:
 * <p>
 *   count(*)      -- finds the number of rows in the table.
 *   sum(column)   -- finds the value of the sum the column.
 *   avg(column)   -- finds the average value from the sum of the column.
 *   min(column)   -- finds the minimum value in the column.
 *   max(column)   -- finds the maximum value in the column.
 * <p>
 * @author Tobias Downer
 * -@deprecated aggregates are handled within expressions.
 */

public final class AggregateFunction implements java.io.Serializable {

  static final long serialVersionUID = 3599578616216083055L;

  /**
   * These statics represent the possible aggregate functions.
   */
  public static final int COUNT_COLUMN = 1;
  public static final int SUM_COLUMN   = 2;
  public static final int AVG_COLUMN   = 3;
  public static final int MIN_COLUMN   = 4;
  public static final int MAX_COLUMN   = 5;

  /**
   * These are the actual string tokens for each of the functions.
   */
  private static final String[] af_tokens =
    { "count", "sum", "avg", "min", "max" };

  /**
   * The type of aggregate function. (eg. COUNT_ALL)
   */
  private int function_type;

  /**
   * The column name to apply the function to.  Use '*' for all columns.
   */
  private String column_name;

  /**
   * The Constructors.
   */
  public AggregateFunction(int fun_type, String col_name) {
    function_type = fun_type;
    column_name = col_name;
  }

  public AggregateFunction(String fun, String col_name) {
    for (int i = 0; i < af_tokens.length; ++i) {
      if (fun.equals(af_tokens[i])) {
        function_type = i + 1;
        column_name = col_name;
        return;
      }
    }
    throw new RuntimeException("Couldn't find aggregate token.");
  }

  /**
   * The Query functions.
   */
  /**
   * Returns the type of the function.
   */
  public int getType() {
    return function_type;
  }

  /**
   * Returns the name of the column to apply the function to.
   * '*' means apply to all columns.
   */
  public String getColumnName() {
    return column_name;
  }

  /**
   * Returns a name that describes this aggregate function.
   */
  public String getName() {
    String str;
    switch (function_type) {
      case COUNT_COLUMN:
        str = "count";
        break;
      case SUM_COLUMN:
        str = "sum";
        break;
      case AVG_COLUMN:
        str = "avg";
        break;
      case MIN_COLUMN:
        str = "min";
        break;
      case MAX_COLUMN:
        str = "max";
        break;
      default:
        throw new RuntimeException("Unknown aggregate function.");
    }
    StringBuffer buf = new StringBuffer();
    buf.append(str);
    buf.append('(');
    buf.append(column_name);
    buf.append(')');
    return new String(buf);
  }

}
