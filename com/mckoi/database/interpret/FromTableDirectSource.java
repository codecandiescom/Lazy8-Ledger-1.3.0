/**
 * com.mckoi.database.sql.FromTableDirectSource  20 Jul 2001
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
import java.util.List;
import java.util.Collections;

/**
 * An implementation of FromTableInterface that wraps around an
 * TableName/AbstractDataTable object.  The handles case insensitive
 * resolution.
 *
 * @author Tobias Downer
 */

public class FromTableDirectSource implements FromTableInterface {

  /**
   * The wrapped object.
   */
  private AbstractDataTable adtable;

  /**
   * The unique name given to this source.
   */
  private String unique_name;

  /**
   * The given TableName of this table.
   */
  private TableName table_name;

  /**
   * The root name of the table.  For example, if this table is 'Part P' the
   * root name is 'Part' and 'P' is the aliased name.
   */
  private TableName root_name;

  /**
   * Set to true if this should do case insensitive resolutions.
   */
  private boolean case_insensitive = false;

  /**
   * Constructs the source.
   */
  public FromTableDirectSource(DatabaseConnection connection,
                               AbstractDataTable table, String unique_name,
                               TableName given_name, TableName root_name) {
    this.unique_name = unique_name;
    this.adtable = table;
    this.root_name = root_name;
    if (given_name != null) {
      this.table_name = given_name;
    }
    else {
      this.table_name = root_name;
    }
    // Is the database case insensitive?
    this.case_insensitive = connection.isInCaseInsensitiveMode();
  }

  /**
   * Returns the given name of the table.  For example, if the Part table is
   * aliased as P this returns P.  If there is no given name, returns the
   * root table name.
   */
  public TableName getGivenTableName() {
    return table_name;
  }

  /**
   * Returns the root name of the table.  This TableName can always be used as
   * a direct reference to a table in the database.
   */
  public TableName getRootTableName() {
    return root_name;
  }

  /**
   * Toggle the case sensitivity flag.
   */
  public void setCaseInsensitive(boolean status) {
    case_insensitive = status;
  }

  private boolean stringCompare(String str1, String str2) {
    if (!case_insensitive) {
      return str1.equals(str2);
    }
    return str1.equalsIgnoreCase(str2);
  }


  // ---------- Implemented from FromTableInterface ----------

  public String getUniqueName() {
    return unique_name;
  }

  public boolean matchesReference(String catalog,
                                  String schema, String table) {
//    System.out.println("Matches reference: " + schema + " " + table);
//    System.out.println(table_name.getName());

    // Does this table name represent the correct schema?
    if (schema != null &&
        !stringCompare(schema, table_name.getSchema())) {
      // If schema is present and we can't resolve to this schema then false
      return false;
    }
    if (table != null &&
        !stringCompare(table, table_name.getName())) {
      // If table name is present and we can't resolve to this table name
      // then return false
      return false;
    }
//    System.out.println("MATCHED!");
    // Match was successful,
    return true;
  }

  public int resolveColumnCount(String catalog, String schema,
                                String table, String column) {
    // NOTE: With this type, we can only ever return either 1 or 0 because
    //   it's impossible to have an ambiguous reference

    // NOTE: Currently 'catalog' is ignored.

    // Does this table name represent the correct schema?
    if (schema != null &&
        !stringCompare(schema, table_name.getSchema())) {
      // If schema is present and we can't resolve to this schema then return 0
      return 0;
    }
    if (table != null &&
        !stringCompare(table, table_name.getName())) {
      // If table name is present and we can't resolve to this table name then
      // return 0
      return 0;
    }

    if (column != null) {
      if (!case_insensitive) {
        // Can we resolve the column in this table?
        int i = adtable.fastFindFieldName(new Variable(table_name, column));
        // If i doesn't equal -1 then we've found our column
        return i == -1 ? 0 : 1;
      }
      else {
        // Case insensitive search (this is slower than case sensitive).
        int resolve_count = 0;
        int col_count = adtable.getColumnCount();
        for (int i = 0; i < col_count; ++i) {
          Variable comp_v = adtable.getResolvedVariable(i);
          if (comp_v.getName().equalsIgnoreCase(column)) {
            ++resolve_count;
          }
        }
        return resolve_count;
      }
    }
    else {  // if (column == null)
      // Return the column count
      return adtable.getColumnCount();
    }
  }

  public Variable resolveColumn(String catalog, String schema,
                                String table, String column) {

    // Does this table name represent the correct schema?
    if (schema != null &&
        !stringCompare(schema, table_name.getSchema())) {
      // If schema is present and we can't resolve to this schema
      throw new Error("Incorrect schema.");
    }
    if (table != null &&
        !stringCompare(table, table_name.getName())) {
      // If table name is present and we can't resolve to this table name
      throw new Error("Incorrect table.");
    }

    if (column != null) {
      if (!case_insensitive) {
        // Can we resolve the column in this table?
        int i = adtable.fastFindFieldName(new Variable(table_name, column));
        if (i == -1) {
          throw new Error("Could not resolve '" + column + "'");
        }
        return adtable.getResolvedVariable(i);
      }
      else {
        // Case insensitive search (this is slower than case sensitive).
        int col_count = adtable.getColumnCount();
        for (int i = 0; i < col_count; ++i) {
          Variable comp_v = adtable.getResolvedVariable(i);
          if (comp_v.getName().equalsIgnoreCase(column)) {
            return comp_v;
          }
        }
        throw new Error("Could not resolve '" + column + "'");
      }
    }
    else {  // if (column == null)
      // Return the first column in the table
      return adtable.getResolvedVariable(0);
    }

  }

  public Variable[] allColumns() {
    int col_count = adtable.getColumnCount();
    Variable[] vars = new Variable[col_count];
    for (int i = 0; i < col_count; ++i) {
      vars[i] = adtable.getResolvedVariable(i);
    }
    return vars;
  }

//  public Queriable getQueriable() {
//    // Create a queriable that has no correlated parts, and when evaluated
//    // returns the direct table.
//    return new Queriable() {
//      public List allCorrelatedVariables() {
//        return Collections.EMPTY_LIST;
//      }
//      public Table evaluateQuery(VariableResolver resolver) {
//        return adtable;
//      }
//    };
//  }

}
