/**
 * com.mckoi.database.interpret.Set  14 Sep 2001
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

import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;
import com.mckoi.database.*;

/**
 * The SQL SET statement.
 *
 * @author Tobias Downer
 */

public class Set extends Statement {

  /**
   * The type of set this is.
   */
  String type;

  /**
   * The variable name of this set statement.
   */
  String var_name;

  /**
   * The Expression that is the value to assign the variable to
   * (if applicable).
   */
  Expression exp;

  /**
   * The value to assign the value to (if applicable).
   */
  String value;



  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {
    type = (String) cmd.getObject("type");
    var_name = (String) cmd.getObject("var_name");
    exp = (Expression) cmd.getObject("exp");
    value = (String) cmd.getObject("value");
  }

  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);

    String com = type.toLowerCase();

    if (com.equals("varset")) {
//      if (var_name.equals("ERROR_ON_DIRTY_SELECT")) {
//        Object ob = exp.evaluate(null, null);
//        if (ob.equals(new BigDecimal(1))) {
//          // Turn on
//
//        }
//        else if (ob.equals(new BigDecimal(0))) {
//          // Turn off
//        }
//      }
//      else {
      database.setVar(var_name, exp);
//      }
    }
    else if (com.equals("isolationset")) {
      value = value.toLowerCase();
      database.setTransactionIsolation(value);
    }
    else if (com.equals("autocommit")) {
      value = value.toLowerCase();
      if (value.equals("on") ||
          value.equals("1")) {
        database.setAutoCommit(true);
      }
      else if (value.equals("off") ||
               value.equals("0")) {
        database.setAutoCommit(false);
      }
      else {
        throw new DatabaseException("Unrecognised value for SET AUTO COMMIT");
      }
    }
    else if (com.equals("schema")) {
      // It's particularly important that this is done during exclusive
      // lock because SELECT requires the schema name doesn't change in
      // mid-process.

      // Change the connection to the schema
      database.setDefaultSchema(value);
//      String schema_type = schema_manager.getSchemaType(value);
//      if (schema_type == null) {
//        throw new DatabaseException("Schema '" + value + "' doesn't exist.");
//      }
//      else {
//        database.setCurrentSchema(value);
//      }
    }
    else {
      throw new DatabaseException("Unrecognised set command.");
    }

    return FunctionTable.resultTable(context, 0);

  }

  public boolean isExclusive() {
    // Yes, all set operations are exclusive (within the transaction)
    return true;
  }

  public List readsFromTables() {
    // The information we are retrieving doesn't require write or read locks.
    ArrayList read_list = new ArrayList();
    return read_list;
  }

  public List writesToTables() {
    // The information we are retrieving doesn't require write or read locks.
    ArrayList write_list = new ArrayList();
    return write_list;
  }

}
