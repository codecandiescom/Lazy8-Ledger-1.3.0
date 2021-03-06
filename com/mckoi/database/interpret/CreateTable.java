/**
 * com.mckoi.database.interpret.CreateTable  14 Sep 2001
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
import com.mckoi.util.IntegerVector;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;

/**
 * A parsed state container for the 'create' statement.
 *
 * @author Tobias Downer
 */

public class CreateTable extends Statement {

  /**
   * Set to true if this create statement is for a temporary table.
   */
  boolean temporary = false;

  /**
   * Only create if table doesn't exist.
   */
  boolean only_if_not_exists = false;

  /**
   * The name of the table to create.
   */
  String table_name;

  /**
   * List of column declarations (ColumnDef)
   */
  ArrayList columns;

  /**
   * List of table constraints (ConstraintDef)
   */
  ArrayList constraints;

//  /**
//   * The expression that must be evaluated to true for this row to be
//   * added to the table.
//   */
//  Expression check_exp;

  /**
   * The TableName object.
   */
  private TableName tname;


//  /**
//   * Adds a new ColumnDef object to this create statement.  A ColumnDef
//   * object describes a column for the new table we are creating.  The column's
//   * must be added in the order they are to be in the created table.
//   */
//  void addColumnDef(ColumnDef column) {
//    columns.addElement(column);
//  }

  /**
   * Adds a new ConstraintDef object to this create statement.  A ConstraintDef
   * object describes any constraints for the new table we are creating.
   */
  void addConstraintDef(ConstraintDef constraint) {
    constraints.add(constraint);
  }

//  /**
//   * Handles the create statement 'CHECK' expression for compatibility.
//   */
//  void addCheckConstraint(Expression check_expression) {
//    ConstraintDef constraint = new ConstraintDef();
//    constraint.setCheck(check_expression);
//    constraints.addElement(constraint);
//  }

  /**
   * Creates a DataTableDef that describes the table that was defined by
   * this create statement.  This is used by the 'alter' statement.
   */
  DataTableDef createDataTableDef() throws DatabaseException {
    // Make all this information into a DataTableDef object...
    DataTableDef table_def = new DataTableDef();
    table_def.setName(tname.getName());
    table_def.setSchema(tname.getSchema());
    table_def.setTableClass("com.mckoi.database.VariableSizeDataTableFile");

    // Add the columns.
    // NOTE: Any duplicate column names will be found here...
    for (int i = 0; i < columns.size(); ++i) {
      DataTableColumnDef cd = (DataTableColumnDef) columns.get(i);
      table_def.addColumn(cd);
    }

    return table_def;
  }


  /**
   * Adds a schema constraint to the rules for the schema represented by the
   * manager.
   */
  static void addSchemaConstraint(DatabaseConnection manager,
                                  TableName table, ConstraintDef constraint)
                                                  throws DatabaseException {
    if (constraint.type == ConstraintDef.PRIMARY_KEY) {
      manager.addPrimaryKeyConstraint(table,
          constraint.getColumnList(), constraint.deferred, constraint.name);
    }
    else if (constraint.type == ConstraintDef.FOREIGN_KEY) {
      // Currently we forbid referencing a table in another schema
      TableName ref_table =
                          TableName.resolve(constraint.reference_table_name);
      String update_rule = constraint.getUpdateRule().toUpperCase();
      String delete_rule = constraint.getDeleteRule().toUpperCase();
      if (table.getSchema().equals(ref_table.getSchema())) {
        manager.addForeignKeyConstraint(
             table, constraint.getColumnList(),
             ref_table, constraint.getColumnList2(),
             delete_rule, update_rule, constraint.deferred, constraint.name);
      }
      else {
        throw new DatabaseException("Foreign key reference error: " +
                "Not permitted to reference a table outside of the schema: " +
                table + " -> " + ref_table);
      }
    }
    else if (constraint.type == ConstraintDef.UNIQUE) {
      manager.addUniqueConstraint(table, constraint.getColumnList(),
                                  constraint.deferred, constraint.name);
    }
    else if (constraint.type == ConstraintDef.CHECK) {
      manager.addCheckConstraint(table, constraint.original_check_expression,
                                 constraint.deferred, constraint.name);
    }
    else {
      throw new DatabaseException("Unrecognized constraint type.");
    }
  }

  /**
   * Returns a com.mckoi.database.interpret.ColumnDef object a a
   * com.mckoi.database.DataTableColumnDef object.
   */
  static DataTableColumnDef convertColumnDef(ColumnDef cdef) {
    DataTableColumnDef dtcdef = new DataTableColumnDef();
    dtcdef.setName(cdef.name);
    dtcdef.setNotNull(cdef.isNotNull());
    dtcdef.setSQLType(cdef.sql_type);
    dtcdef.setSize(cdef.size);
    dtcdef.setScale(cdef.scale);
    if (cdef.class_constraint != null) {
      dtcdef.setClassConstraint(cdef.class_constraint);
    }
    if (cdef.index_str != null) {
      dtcdef.setIndexScheme(cdef.index_str);
    }
    if (cdef.default_expression != null) {
      dtcdef.setDefaultExpression(cdef.original_default_expression);
    }
    return dtcdef;
  }

  /**
   * Sets up all constraints specified in this create statement.
   */
  void setupAllConstraints() throws DatabaseException {
    for (int i = 0; i < constraints.size(); ++i) {
      ConstraintDef constraint = (ConstraintDef) constraints.get(i);

      // Add this to the schema manager tables
      addSchemaConstraint(database, tname, constraint);
    }
  }




  // ---------- Implemented from Statement ----------

  public void prepare() throws DatabaseException {

    // Get the state from the model
    temporary = cmd.getBoolean("temporary");
    only_if_not_exists = cmd.getBoolean("only_if_not_exists");
    table_name = (String) cmd.getObject("table_name");
    ArrayList column_list = (ArrayList) cmd.getObject("column_list");
    constraints = (ArrayList) cmd.getObject("constraint_list");

    // Convert column_list to list of com.mckoi.database.DataTableColumnDef
    int size = column_list.size();
    columns = new ArrayList(size);
    for (int i = 0; i < size; ++i) {
      ColumnDef cdef = (ColumnDef) column_list.get(i);
      columns.add(convertColumnDef(cdef));
    }

    // ----

    String schema_name = database.getCurrentSchema();
    tname = TableName.resolve(schema_name, table_name);

    String name_strip = tname.getName();

    if (name_strip.indexOf('.') != -1) {
      throw new DatabaseException("Table name can not contain '.' character.");
    }

    final boolean ignores_case = database.isInCaseInsensitiveMode();

    // Implement the checker class for this statement.
    ColumnChecker checker = new ColumnChecker() {

      String resolveColumnName(String col_name) throws DatabaseException {
        // We need to do case sensitive and case insensitive resolution,
        String found_col = null;
        for (int n = 0; n < columns.size(); ++n) {
          DataTableColumnDef col = (DataTableColumnDef) columns.get(n);
          if (!ignores_case) {
            if (col.getName().equals(col_name)) {
              return col_name;
            }
          }
          else {
            if (col.getName().equalsIgnoreCase(col_name)) {
              if (found_col != null) {
                throw new DatabaseException("Ambiguous column name '" +
                                            col_name + "'");
              }
              found_col = col.getName();
            }
          }
        }
        return found_col;
      }

    };

    ArrayList unique_column_list = new ArrayList();
    ArrayList primary_key_column_list = new ArrayList();

    // Check the expressions that represent the default values for the columns.
    // Also check each column name
    for (int i = 0; i < columns.size(); ++i) {
      DataTableColumnDef cdef = (DataTableColumnDef) columns.get(i);
      ColumnDef model_cdef = (ColumnDef) column_list.get(i);
      checker.checkExpression(cdef.getDefaultExpression(database.getSystem()));
      String col_name = cdef.getName();
      // If column name starts with [table_name]. then strip it off
      cdef.setName(checker.stripTableName(name_strip, col_name));
      // If unique then add to unique columns
      if (model_cdef.isUnique()) {
        unique_column_list.add(col_name);
      }
      // If primary key then add to primary key columns
      if (model_cdef.isPrimaryKey()) {
        primary_key_column_list.add(col_name);
      }
    }

    // Add the unique and primary key constraints.
    if (unique_column_list.size() > 0) {
      ConstraintDef constraint = new ConstraintDef();
      constraint.setUnique(unique_column_list);
      addConstraintDef(constraint);
    }
    if (primary_key_column_list.size() > 0) {
      ConstraintDef constraint = new ConstraintDef();
      constraint.setPrimaryKey(primary_key_column_list);
      addConstraintDef(constraint);
    }

    // Strip the column names and set the expression in all the constraints.
    for (int i = 0; i < constraints.size(); ++i) {
      ConstraintDef constraint = (ConstraintDef) constraints.get(i);
      checker.stripColumnList(name_strip, constraint.column_list);
      // Check the referencing table for foreign keys
      if (constraint.type == ConstraintDef.FOREIGN_KEY) {
        checker.stripColumnList(constraint.reference_table_name,
                                constraint.column_list2);
        TableName ref_tname =
                 resolveTableName(constraint.reference_table_name, database);
        if (database.isInCaseInsensitiveMode()) {
          ref_tname = database.tryResolveCase(ref_tname);
        }
        constraint.reference_table_name = ref_tname.toString();

        DataTableDef ref_table_def;
        if (database.tableExists(ref_tname)) {
          // Get the DataTableDef for the table we are referencing
          ref_table_def = database.getDataTableDef(ref_tname);
        }
        else if (ref_tname.equals(tname)) {
          // We are referencing the table we are creating
          ref_table_def = createDataTableDef();
        }
        else {
          throw new DatabaseException(
                "Referenced table '" + ref_tname + "' in constraint '" +
                constraint.name + "' does not exist.");
        }
        // Resolve columns against the given table def
        ref_table_def.resolveColumnsInArray(database, constraint.column_list2);

      }
      checker.checkExpression(constraint.check_expression);
      checker.checkColumnList(constraint.column_list);
    }

  }

  public Table evaluate() throws DatabaseException {

    DatabaseQueryContext context = new DatabaseQueryContext(database);

    // Does the user have privs to create this tables?
    if (!user.canCreateTable(database, tname)) {
      throw new UserAccessException(
         "User not permitted to create table: " + table_name);
    }

    // Does the schema exist?
    boolean ignore_case = database.isInCaseInsensitiveMode();
    SchemaDef schema =
            database.resolveSchemaCase(tname.getSchema(), ignore_case);
    if (schema == null) {
      throw new DatabaseException("Schema '" + tname.getSchema() +
                                  "' doesn't exist.");
    }
    else {
      tname = new TableName(schema.getName(), tname.getName());
    }

    // PENDING: Creation of temporary tables...




    // Does the table already exist?
    if (!database.tableExists(tname)) {

      // Create the data table definition and tell the database to create
      // it.
      DataTableDef table_def = createDataTableDef();
      database.createTable(table_def);

      // The initial grants for a table is to give the user who created it
      // full access.
      database.getGrantManager().addGrant(
           Privileges.ALL_PRIVS, GrantManager.TABLE, tname.toString(),
           user.getUserName(), true, "@SYSTEM");

      // Set the constraints in the schema.
      setupAllConstraints();

      // Return '0' if we created the table.  (0 rows affected)
      return FunctionTable.resultTable(context, 0);
    }

    // Report error unless 'if not exists' command is in the statement.
    if (only_if_not_exists == false) {
      throw new DatabaseException("Table '" + tname + "' already exists.");
    }

    // Return '0' (0 rows affected).  This happens when we don't create a
    // table (because it exists) and the 'IF NOT EXISTS' clause is present.
    return FunctionTable.resultTable(context, 0);

  }

  public boolean isExclusive() {
    // Yes, create table operations are exclusive.
    return true;
  }

  public List readsFromTables() {
    // Create doesn't read from other tables...
    return new ArrayList();
  }

  public List writesToTables() {
    // We don't return any tables here because the create operation is on a
    // table that doesn't exist yet so it can't be write locked.
    return new ArrayList();
  }

}
