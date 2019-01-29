/**
 * com.mckoi.database.ReferenceTable  03 Apr 1998
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

import com.mckoi.util.IntegerVector;

/**
 * This is an implementation of a Table that references a DataTable as its
 * parent.  This is a one-to-one relationship unlike the VirtualTable class
 * which is a one-to-many relationship.
 * <p>
 * The entire purpose of this class is as a filter.  We can use it to rename
 * a DataTable class to any domain we feel like.  This allows us to generate
 * unique column names.
 * <p>
 * For example, say we need to join the same table.  We can use this method
 * to ensure that the newly joined table won't have duplicate column names.
 * <p>
 * @author Tobias Downer
 */

public final class ReferenceTable extends DataTableFilter {

  /**
   * The string represents the new name of the table.
   */
  private String table_name;

  /**
   * The modified DataTableDef object for this reference.
   */
  private DataTableDef modified_table_def;

  /**
   * The Constructor.
   */
  ReferenceTable(DataTable table, String name) {
    super(table);
    table_name = name;

    modified_table_def = new DataTableDef(table.getDataTableDef());
    modified_table_def.setName(name);
    modified_table_def.setImmutable();

  }

  /**
   * Filters the name of the table.  This returns the declared name of the
   * table.
   */
  public String getName() {
    return table_name;
  }

  /**
   * Returns a TableName for this referenced table.
   */
  public TableName getTableName() {
    // We inherit the schema from the referenced table.
    return getDataTableDef().getTableName();
  }

  /**
   * Returns the 'modified' DataTableDef object for this reference.
   */
  public DataTableDef getDataTableDef() {
    return modified_table_def;
  }

  /**
   * Given a field name, ie. 'CUSTOMER.CUSTOMERID' this will return the
   * column number the field is at.  Returns -1 if the field does not exist
   * in this table.  NOTE: We resolve the string into the parents domain
   * before we hand it to the parent.
   *
   * @deprecated
   */
  public final int findFieldName(String name) {
    int point_index = name.indexOf('.');
    String type = name.substring(0, point_index);

    if (type.equals(table_name)) {
      String col_name = name.substring(point_index + 1);

      StringBuffer out = new StringBuffer();
      out.append(parent.getName());
      out.append('.');
      out.append(col_name);
      return parent.findFieldName(new String(out));
    }
    return -1;
  }

  /**
   * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
   * this will return the column number the field is at.  Returns -1 if the
   * field does not exist in the table.
   */
  public int findFieldName(Variable v) {
    TableName table_name = v.getTableName();
    if (table_name != null && table_name.getName().equals(getName())) {
      String col_name = v.getName();
      return parent.findFieldName(
                             new Variable(parent.getTableName(), col_name));
    }
    return -1;
  }

  /**
   * Returns the fully resolved name of the given column in this table.  This
   * must return the fully resolved string of the format
   * '[Table Name].[Column Name]'.  We need to replace the parents domain with
   * this referenced domain.
   *
   * @deprecated
   */
  public final String getResolvedColumnName(int column) {
    String name = parent.getResolvedColumnName(column);

    int point_index = name.indexOf('.');
    String type = name.substring(0, point_index);
    String col_name = name.substring(point_index + 1);

    StringBuffer out = new StringBuffer();
    out.append(table_name);
    out.append('.');
    out.append(col_name);
    return new String(out);
  }

  /**
   * Returns a fully qualified Variable object that represents the name of
   * the column at the given index.  For example,
   *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
   */
  public Variable getResolvedVariable(int column) {
    Variable v = parent.getResolvedVariable(column);
    return new Variable(new TableName(getName()), v.getName());
  }

}
