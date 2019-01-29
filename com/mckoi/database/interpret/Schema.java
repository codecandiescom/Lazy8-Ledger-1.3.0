/**
 * com.mckoi.database.interpret.Schema  14 Sep 2001
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
import com.mckoi.database.*;

/**
 * Statement container that handles the CREATE SCHEMA and DROP SCHEMA
 * statements.
 *
 * @author Tobias Downer
 */

public class Schema extends Statement {

  /**
   * The type (either 'create' or 'drop').
   */
  String type;

  /**
   * The name of the schema.
   */
  String schema_name;

  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {
    type = (String) cmd.getObject("type");
    schema_name = (String) cmd.getObject("schema_name");
  }

  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);

    String com = type.toLowerCase();

    if (!user.canCreateAndDropSchema(database)) {
      throw new UserAccessException(
                         "User not permitted to create or drop schema.");
    }

//    SchemaManager schema_manager = database.getSchemaManager();

    if (com.equals("create")) {
      boolean ignore_case = database.isInCaseInsensitiveMode();
      SchemaDef schema =
                  database.resolveSchemaCase(schema_name, ignore_case);
      if (schema == null) {
        database.createSchema(schema_name, "USER");
      }
      else {
        throw new DatabaseException("Schema '" + schema_name +
                                    "' already exists.");
      }
    }
    else if (com.equals("drop")) {
      boolean ignore_case = database.isInCaseInsensitiveMode();
      SchemaDef schema =
                  database.resolveSchemaCase(schema_name, ignore_case);
//      String sc_type = schema_manager.getSchemaType(schema_name);
      // Only allow user to drop USER typed schemas
      if (schema == null) {
        throw new DatabaseException(
                 "Schema '" + schema_name + "' does not exist.");
      }
      else if (schema.getType().equals("USER")) {
        // Check if the schema is empty.
        TableName[] all_tables = database.getTableList();
        String resolved_schema_name = schema.getName();
        for (int i = 0; i < all_tables.length; ++i) {
          if (all_tables[i].getSchema().equals(resolved_schema_name)) {
            throw new DatabaseException(
                          "Schema '" + schema_name + "' is not empty.");
          }
        }
        database.dropSchema(schema.getName());
      }
      else {
        throw new DatabaseException(
                   "User is forbidden to drop schema '" + schema_name + "'");
      }
    }
    else {
      throw new DatabaseException("Unrecognised schema command.");
    }

    return FunctionTable.resultTable(context, 0);

  }

  public boolean isExclusive() {
    // Yes, all set operations are exclusive (within the transaction)
    return true;
  }

  public List readsFromTables() {
    // The information we are retrieving doesn't require write or read locks.
    return new ArrayList();
  }

  public List writesToTables() {
    // The information we are retrieving doesn't require write or read locks.
    return new ArrayList();
  }

}
