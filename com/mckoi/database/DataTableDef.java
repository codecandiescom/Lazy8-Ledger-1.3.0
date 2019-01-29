/**
 * com.mckoi.database.DataTableDef  27 Jul 2000
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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A definition of a table.  Every table in the database has a definition
 * that describes how it is stored on disk, the column definitions, primary
 * keys/foreign keys, and any check constraints.
 *
 * @author Tobias Downer
 */

public class DataTableDef {

  /**
   * The name of the schema this table is defined in.
   */
  private String schema;

  /**
   * The name of the table.
   */
  private String name;

//  /**
//   * The table name object.
//   */
//  private TableName table_name_ob;

  /**
   * The type of table this is (this is the class name of the object that
   * maintains the underlying database files).
   */
  private String table_type_class;

  /**
   * The list of DataTableColumnDef objects that are the definitions of each
   * column in the table.
   */
  private ArrayList column_list;

//  /**
//   * The check expression that must parse to true for a new column to be
//   * added to this table.
//   * (Legacy from version 1 of this format)
//   */
//  private Expression legacy_check_expression;


  /**
   * Set to true if this data table def is immutable.
   */
  private boolean immutable;

  /**
   * Constructs this DataTableDef file.
   */
  public DataTableDef() {
    column_list = new ArrayList();
    table_type_class = "";
    schema = "";
    immutable = false;
  }

  /**
   * Copy constructor.
   */
  public DataTableDef(DataTableDef table_def) {
    schema = table_def.schema;
    name = table_def.name;
    table_type_class = table_def.table_type_class;
    column_list = (ArrayList) table_def.column_list.clone();
//    if (table_def.legacy_check_expression != null) {
//      legacy_check_expression =
//                        new Expression(table_def.legacy_check_expression);
//    }

    // Copy is not immutable
    immutable = false;
  }

  /**
   * Sets this DataTableDef to immutable which means nothing is able to
   * change it.
   */
  public void setImmutable() {
    immutable = true;
  }

  /**
   * Returns true if this is immutable.
   */
  public boolean immutable() {
    return immutable;
  }

  /**
   * Checks that this object is mutable.  If it isn't an exception is thrown.
   */
  private void checkMutable() {
    if (immutable()) {
      throw new Error("Tried to mutable immutable object.");
    }
  }

  /**
   * Resolves variables in a column so that any unresolved column names point
   * to this table.  Used to resolve columns in the 'check_expression'.
   */
  void resolveColumns(boolean ignore_case, Expression exp) {
//    checkMutable();

    // For each variable, determine if the column correctly resolves to a
    // column in this table.  If the database is in identifier case insensitive
    // mode attempt to resolve the column name to a valid column in this
    // def.
    if (exp != null) {
      List list = exp.allVariables();
//      boolean ignore_case = connection.isInCaseInsensitiveMode();
      for (int i = 0; i < list.size(); ++i) {
        Variable v = (Variable) list.get(i);
        String col_name = v.getName();
        // Can we resolve this to a variable in the table?
        if (ignore_case) {
          int size = columnCount();
          for (int n = 0; n < size; ++n) {
            // If this is a column name (case ignored) then set the variable
            // to the correct cased name.
            if (columnAt(n).getName().equalsIgnoreCase(col_name)) {
              v.setColumnName(columnAt(n).getName());
            }
          }
        }
      }

//      for (int i = 0; i < list.size(); ++i) {
//        Variable v = (Variable) list.get(i);
//        String str = v.getName();
//        int delind = str.indexOf('.');
//        String col_name;
//        if (delind == -1) {
//          col_name = str;
//        }
//        else {
//          col_name = str.substring(delind + 1);
//        }
//        v.set(new Variable(getTableName(), col_ind));
//      }
    }
  }

  /**
   * Resolves a single column name to its correct form.  For example, if
   * the database is in case insensitive mode it'll resolve ID to 'id' if
   * 'id' is in this table.  Throws a database exception if a column couldn't
   * be resolved (ambiguous or not found).
   */
  public String resolveColumnName(String col_name, boolean ignore_case)
                                                   throws DatabaseException {
    // Can we resolve this to a column in the table?
    int size = columnCount();
    int found = -1;
    for (int n = 0; n < size; ++n) {
      // If this is a column name (case ignored) then set the column
      // to the correct cased name.
      String this_col_name = columnAt(n).getName();
      if (ignore_case && this_col_name.equalsIgnoreCase(col_name)) {
        if (found == -1) {
          found = n;
        }
        else {
          throw new DatabaseException(
                      "Ambiguous reference to column '" + col_name + "'");
        }
      }
      else if (!ignore_case && this_col_name.equals(col_name)) {
        found = n;
      }
    }
    if (found != -1) {
      return columnAt(found).getName();
    }
    else {
      throw new DatabaseException("Column '" + col_name + "' not found");
    }
  }

  /**
   * Given a list of column names referencing entries in this table, this will
   * resolve each one to its correct form.  Throws a database exception if
   * a column couldn't be resolved.
   */
  public void resolveColumnsInArray(DatabaseConnection connection,
                                    ArrayList list) throws DatabaseException {
    boolean ignore_case = connection.isInCaseInsensitiveMode();
    for (int i = 0; i < list.size(); ++i) {
      String col_name = (String) list.get(i);
      list.set(i, resolveColumnName((String) list.get(i), ignore_case));
    }
  }

  // ---------- Set methods ----------

  public void setTableName(TableName name) {
    setSchema(name.getSchema());
    setName(name.getName());
  }

  public void setSchema(String name) {
    checkMutable();
    this.schema = name;
//    table_name_ob = new TableName(getSchema(), getName());
  }

  public void setName(String name) {
    checkMutable();
    this.name = name;
//    table_name_ob = new TableName(getSchema(), getName());
  }

  public void setTableClass(String clazz) {
    checkMutable();
    if (clazz.equals("com.mckoi.database.VariableSizeDataTableFile")) {
      table_type_class = clazz;
    }
    else {
      throw new Error("Unrecognised table class: " + clazz);
    }
  }

  public void addColumn(DataTableColumnDef col_def) {
    checkMutable();
    // Is there already a column with this name in the table def?
    for (int i = 0; i < column_list.size(); ++i) {
      DataTableColumnDef cd = (DataTableColumnDef) column_list.get(i);
      if (cd.getName().equals(col_def.getName())) {
        throw new Error("Duplicated columns found.");
      }
    }
    column_list.add(col_def);
  }

  public void addColumn(TableField field) throws DatabaseException {
    checkMutable();
    DataTableColumnDef col_def = new DataTableColumnDef();
    col_def.setName(field.getName());
    col_def.setDBType(field.getType());
    int size = field.getSize();
    if (size == 0) size = -1;
    col_def.setSize(size);
    int scale = field.getScale();
    if (scale == 0) scale = -1;
    col_def.setScale(scale);
    col_def.setNotNull(field.isNotNull());
//    col_def.setUnique(field.isUnique());

    addColumn(col_def);
  }

//  public void setCheckExpression(Expression expression) {
//    this.check_expression = expression;
//    resolveColumns(expression);
//  }

  // ---------- Get methods ----------

  public String getSchema() {
    return schema;
  }

  public String getName() {
    return name;
  }

  public TableName getTableName() {
    return new TableName(getSchema(), getName());
//    return table_name_ob;
  }

  public String getTableClass() {
    return table_type_class;
  }

  public int columnCount() {
    return column_list.size();
  }

  public DataTableColumnDef columnAt(int column) {
    return (DataTableColumnDef) column_list.get(column);
  }

  public int findColumnName(String column_name) {
    int size = columnCount();
    for (int i = 0; i < size; ++i) {
      if (columnAt(i).getName().equals(column_name)) {
        return i;
      }
    }
    return -1;
  }

//  public Expression getCheckExpression() {
//    return check_expression;
//  }

  /**
   * Creates a TableField[] array from the columns in this def.
   * <p>
   * NOTE: We must deprecate this at some point.
   */
  public TableField[] toTableFieldArray() {
    TableField[] list = new TableField[columnCount()];
    for (int i = 0; i < columnCount(); ++i) {
      list[i] = columnAt(i).tableFieldValue();
    }
    return list;
  }

  /**
   * Returns a copy of this object, except with no columns or constraints.
   */
  public DataTableDef noColumnCopy() {
    DataTableDef def = new DataTableDef();
    def.setSchema(schema);
    def.setName(name);

    def.table_type_class = table_type_class;
//    if (check_expression != null) {
//      def.check_expression = new Expression(check_expression);
//    }
    return def;
  }

//  /**
//   * For converting from old formats, this is the check expression set for
//   * this table or null if nothing set.
//   */
//  Expression compatCheckExpression() {
//    return legacy_check_expression;
//  }



  // ---------- In/Out methods ----------

  /**
   * Writes this DataTableDef file to the data output stream.
   */
  void write(DataOutputStream out) throws IOException {
    out.writeInt(2);  // Version number

    out.writeUTF(name);
    out.writeUTF(schema);   // Added in version 2
    out.writeUTF(table_type_class);
    out.writeInt(column_list.size());
    for (int i = 0; i < column_list.size(); ++i) {
      ((DataTableColumnDef) column_list.get(i)).write(out);
    }

//    // -- Added in version 2 --
//    // Write the constraint list.
//    out.writeInt(constraint_list.size());
//    for (int i = 0; i < constraint_list.size(); ++i) {
//      ((DataTableConstraintDef) constraint_list.get(i)).write(out);
//    }

//    [ this is removed from version 1 ]
//    if (check_expression != null) {
//      out.writeBoolean(true);
//      // Write the text version of the expression to the stream.
//      out.writeUTF(new String(check_expression.text()));
//    }
//    else {
//      out.writeBoolean(false);
//    }

  }

  /**
   * Reads this DataTableDef file from the data input stream.
   */
  static DataTableDef read(DataInputStream in) throws IOException {
    DataTableDef dtf = new DataTableDef();
    int ver = in.readInt();
    if (ver == 1) {

      throw new IOException("Version 1 DataTableDef no longer supported.");

//      // Lists that are generated from the unique and primary key column lists
//      // from old data.  This is used to convert to the new constraint list.
//      ArrayList unique_column_list = new ArrayList();
//      ArrayList primary_key_col_list = new ArrayList();
//
//      // For backward compatibility with version 1
//      dtf.name = in.readUTF();
//      // --- Conversion from version 1 ---
//      // If name starts with 'sUSR' then move table to SYS_INFO schema
//      if (dtf.name.startsWith("sUSR")) {
//        dtf.schema = Database.SYSTEM_SCHEMA;
//      }
//      else {
//        dtf.schema = Database.DEFAULT_SCHEMA;      // No schema in version 1
//      }
//      // Hack to set the TableName object
//      dtf.setName(dtf.name);
//      dtf.table_type_class = in.readUTF();
//      int size = in.readInt();
//      for (int i = 0; i < size; ++i) {
//        DataTableColumnDef col_def = DataTableColumnDef.read(in);
//        dtf.column_list.add(col_def);
//        // If this column was defined as unique then add to unique constraint.
//        if (col_def.compatIsUnique()) {
//          unique_column_list.add(col_def.getName());
//        }
//        // If this column was defined as a primary key then add to primary
//        // key constraint.
//        if (col_def.compatIsPrimaryKey()) {
//          primary_key_col_list.add(col_def.getName());
//        }
//      }
//
//      boolean b = in.readBoolean();
//      if (b) {
//        dtf.legacy_check_expression = Expression.parse(in.readUTF());
//        dtf.resolveColumns(dtf.legacy_check_expression);
//
//        // -- Conversion from version 1 --
//        // In version 2 we now add this as a check constraint
////        Expression check_expression = Expression.parse(in.readUTF());
////        dtf.resolveColumns(check_expression);
//
////        DataTableConstraintDef check_constraint =
////                                           new DataTableConstraintDef(dtf);
////        check_constraint.setType(DataTableConstraintDef.CHECK);
////        check_constraint.setName(DataTableConstraintDef.SYS_TABLE_CHECK);
////        check_constraint.setCheckExpression(check_expression);
////        dtf.addConstraint(check_constraint);
//      }
//
////      // -- Conversion from version 1 --
////      // If applicable, add the unique and primary_key constraints,
////      if (unique_column_list.size() > 0) {
////        DataTableConstraintDef constraint = new DataTableConstraintDef(dtf);
////        constraint.setType(DataTableConstraintDef.UNIQUE);
////        constraint.setName(DataTableConstraintDef.SYS_UNIQUE);
////        constraint.setColumnList(unique_column_list);
////        dtf.addConstraint(constraint);
////      }
////      if (primary_key_col_list.size() > 0) {
////        DataTableConstraintDef constraint = new DataTableConstraintDef(dtf);
////        constraint.setType(DataTableConstraintDef.UNIQUE);
////        constraint.setName(DataTableConstraintDef.SYS_PRIMARY_KEY);
////        constraint.setColumnList(primary_key_col_list);
////        dtf.addConstraint(constraint);
////      }

    }
    else if (ver == 2) {

      dtf.name = in.readUTF();
      dtf.schema = in.readUTF();
      // Hack to set the TableName object
      dtf.setName(dtf.name);
      dtf.table_type_class = in.readUTF();
      int size = in.readInt();
      for (int i = 0; i < size; ++i) {
        DataTableColumnDef col_def = DataTableColumnDef.read(in);
        dtf.column_list.add(col_def);
      }
//      size = in.readInt();
//      for (int i = 0; i < size; ++i) {
//        DataTableConstraintDef col_def =
//                                 DataTableConstraintDef.read(dtf, in);
//        dtf.constraint_list.add(col_def);
//      }

    }
    else {
      throw new Error("Unrecognized DataTableDef version (" + ver + ")");
    }

    dtf.setImmutable();
    return dtf;
  }

}
