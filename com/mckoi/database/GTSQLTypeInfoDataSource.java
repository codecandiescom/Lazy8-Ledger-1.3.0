/**
 * com.mckoi.database.GTSQLTypeInfoDataSource  23 Mar 2002
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
import java.math.BigDecimal;
import com.mckoi.database.global.SQLTypes;

/**
 * A GTDataSource that models all SQL types that are available.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

public class GTSQLTypeInfoDataSource extends GTDataSource {

  /**
   * The DatabaseConnection object.  Currently this is not used, but it may
   * be needed in the future if user-defined SQL types are supported.
   */
  private DatabaseConnection database;

  /**
   * The list of info keys/values in this object.
   */
  private ArrayList key_value_pairs;

  /**
   * Constructor.
   */
  public GTSQLTypeInfoDataSource(DatabaseConnection connection) {
    super(connection.getSystem());
    this.database = connection;
    this.key_value_pairs = new ArrayList();
  }

  /**
   * Adds a type description.
   */
  private void addType(String name, int type, int precision,
                       String prefix, String suffix, String oops,
                       boolean searchable) {
    key_value_pairs.add(name);
    key_value_pairs.add(BigDecimal.valueOf(type));
    key_value_pairs.add(BigDecimal.valueOf(precision));
    key_value_pairs.add(prefix);
    key_value_pairs.add(suffix);
    key_value_pairs.add(searchable ? BigDecimal.valueOf(3) :
                                     BigDecimal.valueOf(0));
  }

  /**
   * Initialize the data source.
   */
  public GTSQLTypeInfoDataSource init() {

    addType("BIT", SQLTypes.BIT, 1, null, null, null, true);
    addType("BOOLEAN", SQLTypes.BIT, 1, null, null, null, true);
    addType("TINYINT", SQLTypes.TINYINT, 9, null, null, null, true);
    addType("SMALLINT", SQLTypes.SMALLINT, 9, null, null, null, true);
    addType("INTEGER", SQLTypes.INTEGER, 9, null, null, null, true);
    addType("BIGINT", SQLTypes.BIGINT, 9, null, null, null, true);
    addType("FLOAT", SQLTypes.FLOAT, 9, null, null, null, true);
    addType("REAL", SQLTypes.REAL, 9, null, null, null, true);
    addType("DOUBLE", SQLTypes.DOUBLE, 9, null, null, null, true);
    addType("NUMERIC", SQLTypes.NUMERIC, 9, null, null, null, true);
    addType("DECIMAL", SQLTypes.DECIMAL, 9, null, null, null, true);
    addType("CHAR", SQLTypes.CHAR, 9, "'", "'", null, true);
    addType("VARCHAR", SQLTypes.VARCHAR, 9, "'", "'", null, true);
    addType("LONGVARCHAR", SQLTypes.LONGVARCHAR, 9, "'", "'", null, true);
    addType("DATE", SQLTypes.DATE, 9, null, null, null, true);
    addType("TIME", SQLTypes.TIME, 9, null, null, null, true);
    addType("TIMESTAMP", SQLTypes.TIMESTAMP, 9, null, null, null, true);
    addType("BINARY", SQLTypes.BINARY, 9, null, null, null, false);
    addType("VARBINARY", SQLTypes.VARBINARY, 9, null, null, null, false);
    addType("LONGVARBINARY", SQLTypes.LONGVARBINARY,
                             9, null, null, null, false);
    addType("JAVA_OBJECT", SQLTypes.JAVA_OBJECT, 9, null, null, null, false);

    return this;
  }

  // ---------- Implemented from GTDataSource ----------

  public DataTableDef getDataTableDef() {
    return DEF_DATA_TABLE_DEF;
  }

  public int getRowCount() {
    return key_value_pairs.size() / 6;
  }

  public DataCell getCellContents(final int column, final int row) {
    int i = (row * 6);
    switch (column) {
      case 0:  // type_name
        return new StringDataCell(1500, (String) key_value_pairs.get(i));
      case 1:  // data_type
        return new DecimalDataCell((BigDecimal) key_value_pairs.get(i + 1));
      case 2:  // precision
        return new DecimalDataCell((BigDecimal) key_value_pairs.get(i + 2));
      case 3:  // literal_prefix
        return new StringDataCell(5, (String) key_value_pairs.get(i + 3));
      case 4:  // literal_suffix
        return new StringDataCell(5, (String) key_value_pairs.get(i + 4));
      case 5:  // create_params
        return DataCellFactory.NULL_STRING_CELL;
      case 6:  // nullable
        return DataCellFactory.TRUE_BOOLEAN_CELL;
      case 7:  // case_sensitive
        return DataCellFactory.TRUE_BOOLEAN_CELL;
      case 8:  // searchable
        return new DecimalDataCell((BigDecimal) key_value_pairs.get(i + 5));
      case 9:  // unsigned_attribute
        return DataCellFactory.FALSE_BOOLEAN_CELL;
      case 10:  // fixed_prec_scale
        return DataCellFactory.FALSE_BOOLEAN_CELL;
      case 11:  // auto_increment
        return DataCellFactory.FALSE_BOOLEAN_CELL;
      case 12:  // local_type_name
        return DataCellFactory.NULL_STRING_CELL;
      case 13:  // minimum_scale
        return new DecimalDataCell(BigDecimal.valueOf(0));
      case 14:  // maximum_scale
        return new DecimalDataCell(BigDecimal.valueOf(10000000));
      case 15:  // sql_data_type
        return DataCellFactory.NULL_STRING_CELL;
      case 16:  // sql_datetype_sub
        return DataCellFactory.NULL_STRING_CELL;
      case 17:  // num_prec_radix
        return new DecimalDataCell(BigDecimal.valueOf(10));
      default:
        throw new Error("Column out of bounds.");
    }
  }

  // ---------- Overwritten from GTDataSource ----------

  public void dispose() {
    super.dispose();
    key_value_pairs = null;
    database = null;
  }

  // ---------- Static ----------

  /**
   * The data table def that describes this table of data source.
   */
  static final DataTableDef DEF_DATA_TABLE_DEF;

  static {

    DataTableDef def = new DataTableDef();
    def.setSchema(Database.SYSTEM_SCHEMA);
    def.setName("sUSRSQLTypeInfo");

    // Add column definitions
    def.addColumn( stringColumn("TYPE_NAME"));
    def.addColumn(numericColumn("DATA_TYPE"));
    def.addColumn(numericColumn("PRECISION"));
    def.addColumn( stringColumn("LITERAL_PREFIX"));
    def.addColumn( stringColumn("LITERAL_SUFFIX"));
    def.addColumn( stringColumn("CREATE_PARAMS"));
    def.addColumn(booleanColumn("NULLABLE"));
    def.addColumn(booleanColumn("CASE_SENSITIVE"));
    def.addColumn(numericColumn("SEARCHABLE"));
    def.addColumn(booleanColumn("UNSIGNED_ATTRIBUTE"));
    def.addColumn(booleanColumn("FIXED_PREC_SCALE"));
    def.addColumn(booleanColumn("AUTO_INCREMENT"));
    def.addColumn( stringColumn("LOCAL_TYPE_NAME"));
    def.addColumn(numericColumn("MINIMUM_SCALE"));
    def.addColumn(numericColumn("MAXIMUM_SCALE"));
    def.addColumn( stringColumn("SQL_DATA_TYPE"));
    def.addColumn( stringColumn("SQL_DATETIME_SUB"));
    def.addColumn(numericColumn("NUM_PREC_RADIX"));

    // Set to immutable
    def.setImmutable();

    DEF_DATA_TABLE_DEF = def;

  }

}
