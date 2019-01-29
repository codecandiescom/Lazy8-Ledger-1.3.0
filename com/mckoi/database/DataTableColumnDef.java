/**
 * com.mckoi.database.DataTableColumnDef  27 Jul 2000
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
import com.mckoi.database.global.SQLTypes;

/**
 * All the information regarding a column in a table.
 *
 * @author Tobias Downer
 */

public class DataTableColumnDef {

  /**
   * A string that contains some constraints.  This string contains
   * information about whether the column is not null, unique, primary key,
   * etc.
   */
  private byte[] constraints_format = new byte[16];

  /**
   * The name of the column.
   */
  private String name;

  /**
   * The sql column type (as defined in java.sql.Types).
   */
  private int sql_type;

  /**
   * The actual column type in the database (as defined in
   * com.mckoi.database.global.Types).
   */
  private int db_type;

  /**
   * The size of the data.
   */
  private int size;

  /**
   * The scale of the data.
   */
  private int scale;

  /**
   * The default expression string.
   */
  private String default_expression_string;

//  /**
//   * The expression that is executed to set the default value.
//   */
//  private Expression default_exp;

  /**
   * If this is a foreign key, the table.column that this foreign key
   * refers to.
   * @deprecated
   */
  private String foreign_key = "";

  /**
   * The type of index to use on this column.
   */
  private String index_desc = "";

  /**
   * If this is a Java Object column, this is a constraint that the object
   * must be derived from to be added to this column.  If not specified,
   * it defaults to 'java.lang.Object'.
   */
  private String class_constraint = "";

  /**
   * The constraining Class object itself.
   */
  private Class constraining_class;

  /**
   * Any other information about this column can be encoded into this string.
   */
  private String other = "";


  /**
   * Constructs the column definition.
   */
  public DataTableColumnDef() {
  }

  /**
   * Creates a copy of the given column definition.
   */
  public DataTableColumnDef(DataTableColumnDef column_def) {
    System.arraycopy(column_def.constraints_format, 0,
                     constraints_format, 0, constraints_format.length);
    name = column_def.name;
    sql_type = column_def.sql_type;
    db_type = column_def.db_type;
    size = column_def.size;
    scale = column_def.scale;
    if (column_def.default_expression_string != null) {
      default_expression_string = column_def.default_expression_string;
//      default_exp = new Expression(column_def.default_exp);
    }
    foreign_key = column_def.foreign_key;
    index_desc = column_def.index_desc;
    class_constraint = column_def.class_constraint;
    other = column_def.other;
  }

  // ---------- Set methods ----------

  public void setName(String name) {
    this.name = name;
  }

  public void setNotNull(boolean status) {
    constraints_format[0] = (byte) (status ? 1 : 0);
  }

//  public void setUnique(boolean status) {
//    constraints_format[1] = (byte) (status ? 1 : 0);
//  }
//
//  public void setPrimaryKey(boolean status) {
//    constraints_format[2] = (byte) (status ? 1 : 0);
//  }

  public void setSQLType(int sql_type) {
    this.sql_type = sql_type;
    if (sql_type == SQLTypes.BIT) {
      db_type = com.mckoi.database.global.Types.DB_BOOLEAN;
    }
    else if (sql_type == SQLTypes.TINYINT ||
             sql_type == SQLTypes.SMALLINT ||
             sql_type == SQLTypes.INTEGER ||
             sql_type == SQLTypes.BIGINT ||
             sql_type == SQLTypes.FLOAT ||
             sql_type == SQLTypes.REAL ||
             sql_type == SQLTypes.DOUBLE ||
             sql_type == SQLTypes.NUMERIC ||
             sql_type == SQLTypes.DECIMAL) {
      db_type = com.mckoi.database.global.Types.DB_NUMERIC;
    }
    else if (sql_type == SQLTypes.CHAR ||
             sql_type == SQLTypes.VARCHAR ||
             sql_type == SQLTypes.LONGVARCHAR) {
      db_type = com.mckoi.database.global.Types.DB_STRING;
    }
    else if (sql_type == SQLTypes.DATE ||
             sql_type == SQLTypes.TIME ||
             sql_type == SQLTypes.TIMESTAMP) {
      db_type = com.mckoi.database.global.Types.DB_TIME;
    }
    else if (sql_type == SQLTypes.BINARY ||
             sql_type == SQLTypes.VARBINARY ||
             sql_type == SQLTypes.LONGVARBINARY) {
      db_type = com.mckoi.database.global.Types.DB_BLOB;
    }
    else if (sql_type == SQLTypes.JAVA_OBJECT) {
      db_type = com.mckoi.database.global.Types.DB_OBJECT;
    }
    else {
      throw new Error("Unrecognised sql type");
    }
  }

  public void setDBType(int db_type) {
    this.db_type = db_type;
    if (db_type == com.mckoi.database.global.Types.DB_NUMERIC) {
      sql_type = SQLTypes.NUMERIC;
    }
    else if (db_type == com.mckoi.database.global.Types.DB_STRING) {
      sql_type = SQLTypes.LONGVARCHAR;
    }
    else if (db_type == com.mckoi.database.global.Types.DB_BOOLEAN) {
      sql_type = SQLTypes.BIT;
    }
    else if (db_type == com.mckoi.database.global.Types.DB_TIME) {
      sql_type = SQLTypes.TIMESTAMP;
    }
    else if (db_type == com.mckoi.database.global.Types.DB_BLOB) {
      sql_type = SQLTypes.LONGVARBINARY;
    }
    else if (db_type == com.mckoi.database.global.Types.DB_OBJECT) {
      sql_type = SQLTypes.JAVA_OBJECT;
    }
    else {
      throw new Error("Unrecognised internal type.");
    }
  }

  public void setSize(int size) {
    this.size = size;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public void setDefaultExpression(Expression expression) {
    this.default_expression_string = new String(expression.text().toString());
  }

  /**
   * @deprecated
   */
  public void setForeignKey(String foreign_key) {
    this.foreign_key = foreign_key;
  }

  /**
   * Sets the indexing scheme for this column.  Either 'InsertSearch' or
   * 'BlindSearch'.  If not set, then default to insert search.
   */
  public void setIndexScheme(String index_scheme) {
    index_desc = index_scheme;
  }

  /**
   * If this column represents a Java object, this must be a class the object
   * is derived from to be added to this column.
   */
  public void setClassConstraint(String class_constraint) {
    this.class_constraint = class_constraint;
    try {
      // Denotes an array
      if (class_constraint.endsWith("[]")) {
        String array_class =
                 class_constraint.substring(0, class_constraint.length() - 2);
        Class ac;
        // Arrays of primitive types,
        if (array_class.equals("boolean")) {
          ac = boolean.class;
        }
        else if (array_class.equals("byte")) {
          ac = byte.class;
        }
        else if (array_class.equals("char")) {
          ac = char.class;
        }
        else if (array_class.equals("short")) {
          ac = short.class;
        }
        else if (array_class.equals("int")) {
          ac = int.class;
        }
        else if (array_class.equals("long")) {
          ac = long.class;
        }
        else if (array_class.equals("float")) {
          ac = float.class;
        }
        else if (array_class.equals("double")) {
          ac = double.class;
        }
        else {
          // Otherwise a standard array.
          ac = Class.forName(array_class);
        }
        // Make it into an array
        constraining_class =
                       java.lang.reflect.Array.newInstance(ac, 0).getClass();
      }
      else {
        // Not an array
        constraining_class = Class.forName(class_constraint);
      }
    }
    catch (ClassNotFoundException e) {
      throw new Error("Unable to resolve class: " + class_constraint);
    }
  }

  // ---------- Get methods ----------

  public String getName() {
    return name;
  }

  public boolean isNotNull() {
    return constraints_format[0] != 0;
  }

  public int getSQLType() {
    return sql_type;
  }

  /**
   * Returns the type as a String.
   */
  public String getSQLTypeString() {
    switch (getSQLType()) {
      case SQLTypes.BIT:
        return "BIT";
      case SQLTypes.TINYINT:
        return "TINYINT";
      case SQLTypes.SMALLINT:
        return "SMALLINT";
      case SQLTypes.INTEGER:
        return "INTEGER";
      case SQLTypes.BIGINT:
        return "BIGINT";
      case SQLTypes.FLOAT:
        return "FLOAT";
      case SQLTypes.REAL:
        return "REAL";
      case SQLTypes.DOUBLE:
        return "DOUBLE";
      case SQLTypes.NUMERIC:
        return "NUMERIC";
      case SQLTypes.DECIMAL:
        return "DECIMAL";
      case SQLTypes.CHAR:
        return "CHAR";
      case SQLTypes.VARCHAR:
        return "VARCHAR";
      case SQLTypes.LONGVARCHAR:
        return "LONGVARCHAR";
      case SQLTypes.DATE:
        return "DATE";
      case SQLTypes.TIME:
        return "TIME";
      case SQLTypes.TIMESTAMP:
        return "TIMESTAMP";
      case SQLTypes.BINARY:
        return "BINARY";
      case SQLTypes.VARBINARY:
        return "VARBINARY";
      case SQLTypes.LONGVARBINARY:
        return "LONGVARBINARY";
      case SQLTypes.JAVA_OBJECT:
        return "JAVA_OBJECT";
      default:
        return "UNKNOWN(" + getSQLType() + ")";
    }
  }

  /**
   * Returns the type as a String.
   */
  public String getDBTypeString() {
    switch (getDBType()) {
      case com.mckoi.database.global.Types.DB_NUMERIC:
        return "DB_NUMERIC";
      case com.mckoi.database.global.Types.DB_STRING:
        return "DB_STRING";
      case com.mckoi.database.global.Types.DB_BOOLEAN:
        return "DB_BOOLEAN";
      case com.mckoi.database.global.Types.DB_TIME:
        return "DB_TIME";
      case com.mckoi.database.global.Types.DB_BLOB:
        return "DB_BLOB";
      case com.mckoi.database.global.Types.DB_OBJECT:
        return "DB_OBJECT";
      default:
        return "UNKNOWN(" + getDBType() + ")";
    }
  }

  /**
   * Returns the Class of Java object that represents this column.
   */
  public Class classType() {
    return com.mckoi.database.global.TypeUtil.toClass(getDBType());
  }

  public int getDBType() {
    return db_type;
  }

  public int getSize() {
    return size;
  }

  public int getScale() {
    return scale;
  }

  public Expression getDefaultExpression(TransactionSystem system) {
    if (default_expression_string == null) {
      return null;
    }
    Expression exp = Expression.parse(default_expression_string);
    // Prepare the functions in the expression
    try {
      exp.prepare(system.getFunctionExpressionPreparer());
    }
    catch (DatabaseException e) {
      throw new RuntimeException(e.getMessage());
    }
    return exp;
//    return default_exp;
  }

  public String getDefaultExpressionString() {
    return default_expression_string;
  }

  /**
   * @deprecated
   */
  public String getForeignKey() {
    return foreign_key;
  }

  /**
   * Returns the name of the scheme we use to index this column.  It will
   * be either 'InsertSearch' or 'BlindSearch'.
   */
  public String getIndexScheme() {
    if (index_desc.equals("")) {
      return "InsertSearch";
    }
    return index_desc;
  }

  /**
   * Returns true if this type of column is able to be indexed.
   */
  public boolean isIndexableType() {
    if (getDBType() == com.mckoi.database.global.Types.DB_BLOB ||
        getDBType() == com.mckoi.database.global.Types.DB_OBJECT) {
      return false;
    }
    return true;
  }

  /**
   * If this column represents a Java Object, this returns the name of the
   * class the objects stored in the column must be derived from.
   */
  public String getClassConstraint() {
    return class_constraint;
  }

  /**
   * If this column represents a Java Object, this returns the class object
   * that is the constraining class for the column.
   */
  public Class getClassConstraintAsClass() {
    return constraining_class;
  }

  /**
   * Returns this column as a TableField object.
   */
  public TableField tableFieldValue() {
    TableField field =
               new TableField(getName(), getDBType(), getSize(), isNotNull());
//    if (isUnique()) {
//      field.setUnique();
//    }
    field.setScale(getScale());
    field.setSQLType(getSQLType());

    return field;
  }


  // ---------- For compatibility with older versions only --------
  // These are made available only because we need to convert from the
  // pre table constraint versions.

  boolean compatIsUnique() {
    return constraints_format[1] != 0;
  }

  boolean compatIsPrimaryKey() {
    return constraints_format[2] != 0;
  }

  // ---------- Convenient static methods ----------

  /**
   * Convenience helper - creates a DataTableColumnDef that
   * holds a numeric value.
   */
  public static DataTableColumnDef createNumericColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setSQLType(java.sql.Types.NUMERIC);
    return column;
  }

  /**
   * Convenience helper - creates a DataTableColumnDef that
   * holds a boolean value.
   */
  public static DataTableColumnDef createBooleanColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setSQLType(java.sql.Types.BIT);
    return column;
  }

  /**
   * Convenience helper - creates a DataTableColumnDef that
   * holds a string value.
   */
  public static DataTableColumnDef createStringColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setSQLType(java.sql.Types.VARCHAR);
    column.setSize(Integer.MAX_VALUE);
    return column;
  }

  /**
   * Convenience helper - creates a DataTableColumnDef that
   * holds a binary value.
   */
  public static DataTableColumnDef createBinaryColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setSQLType(java.sql.Types.LONGVARBINARY);
    column.setSize(Integer.MAX_VALUE);
    column.setIndexScheme("BlindSearch");
    return column;
  }



  // ---------- IO Methods ----------

  /**
   * Writes this column information out to a DataOutputStream.
   */
  void write(DataOutputStream out) throws IOException {

    out.writeInt(2);    // The version

    out.writeUTF(name);
    out.writeInt(constraints_format.length);
    out.write(constraints_format);
    out.writeInt(sql_type);
    out.writeInt(db_type);
    out.writeInt(size);
    out.writeInt(scale);

    if (default_expression_string != null) {
      out.writeBoolean(true);
      out.writeUTF(default_expression_string);
                           //new String(default_exp.text().toString()));
    }
    else {
      out.writeBoolean(false);
    }

    out.writeUTF(foreign_key);
    out.writeUTF(index_desc);
    out.writeUTF(class_constraint);    // Introduced in version 2.
    out.writeUTF(other);
  }

  /**
   * Reads this column from a DataInputStream.
   */
  static DataTableColumnDef read(DataInputStream in) throws IOException {

    int ver = in.readInt();

    DataTableColumnDef cd = new DataTableColumnDef();
    cd.name = in.readUTF();
    int len = in.readInt();
    in.readFully(cd.constraints_format, 0, len);
    cd.sql_type = in.readInt();
    cd.db_type = in.readInt();
    cd.size = in.readInt();
    cd.scale = in.readInt();

    boolean b = in.readBoolean();
    if (b) {
      cd.default_expression_string = in.readUTF();
//      cd.default_exp = Expression.parse(in.readUTF());
    }
    cd.foreign_key = in.readUTF();
    cd.index_desc = in.readUTF();
    if (ver > 1) {
      String cc = in.readUTF();
      if (!cc.equals("")) {
        cd.setClassConstraint(cc);
      }
    }
    else {
      cd.class_constraint = "";
    }
    cd.other = in.readUTF();

    return cd;
  }

}
