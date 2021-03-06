/**
 * com.mckoi.database.GTTableColumnsDataSource  27 Apr 2001
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

import java.math.BigDecimal;
import com.mckoi.database.global.SQLTypes;

/**
 * An implementation of MutableTableDataSource that presents information
 * about the columns of all tables in all schema.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTTableColumnsDataSource extends GTDataSource {

  /**
   * The transaction that is the view of this information.
   */
  private Transaction transaction;

  /**
   * The list of all DataTableDef visible to the transaction.
   */
  private DataTableDef[] visible_tables;

  /**
   * The number of rows in this table.
   */
  private int row_count;

  /**
   * Constructor.
   */
  public GTTableColumnsDataSource(Transaction transaction) {
    super(transaction.getSystem());
    this.transaction = transaction;
  }

  /**
   * Initialize the data source.
   */
  public GTTableColumnsDataSource init() {
    // All the tables
    TableName[] list = transaction.getTableList();
    visible_tables = new DataTableDef[list.length];
    row_count = 0;
    for (int i = 0; i < list.length; ++i) {
      DataTableDef def = transaction.getDataTableDef(list[i]);
      row_count += def.columnCount();
      visible_tables[i] = def;
    }
    return this;
  }

  // ---------- Implemented from GTDataSource ----------

  public DataTableDef getDataTableDef() {
    return DEF_DATA_TABLE_DEF;
  }

  public int getRowCount() {
    return row_count;
  }

  public DataCell getCellContents(final int column, final int row) {

    final int sz = visible_tables.length;
    int rs = 0;
    for (int n = 0; n < sz; ++n) {
      final DataTableDef def = visible_tables[n];
      final int b = rs;
      rs += def.columnCount();
      if (row >= b && row < rs) {
        // This is the column that was requested,
        int seq_no = row - b;
        DataTableColumnDef col_def = def.columnAt(seq_no);
        switch (column) {
          case 0:  // schema
            return new StringDataCell(200, def.getSchema());
          case 1:  // table
            return new StringDataCell(200, def.getName());
          case 2:  // column
            return new StringDataCell(200, col_def.getName());
          case 3:  // sql_type
            return new DecimalDataCell(new BigDecimal(col_def.getSQLType()));
          case 4:  // type_desc
            return new StringDataCell(200, col_def.getSQLTypeString());
          case 5:  // size
            return new DecimalDataCell(new BigDecimal(col_def.getSize()));
          case 6:  // scale
            return new DecimalDataCell(new BigDecimal(col_def.getScale()));
          case 7:  // not_null
            return new BooleanDataCell(new Boolean(col_def.isNotNull()));
          case 8:  // default
            return new StringDataCell(65536,
                                      col_def.getDefaultExpressionString());
          case 9:  // index_str
            return new StringDataCell(65536,
                                      col_def.getIndexScheme());
          case 10:  // seq_no
            return new DecimalDataCell(new BigDecimal(seq_no));
          default:
            throw new Error("Column out of bounds.");
        }
      }

    }  // for each visible table

    throw new Error("Row out of bounds.");
  }

  // ---------- Overwritten ----------

  public void dispose() {
    super.dispose();
    visible_tables = null;
    transaction = null;
  }

  // ---------- Static ----------

  /**
   * The data table def that describes this table of data source.
   */
  static final DataTableDef DEF_DATA_TABLE_DEF;

  static {

    DataTableDef def = new DataTableDef();
    def.setSchema(Database.SYSTEM_SCHEMA);
    def.setName("sUSRTableColumns");

    // Add column definitions
    def.addColumn(stringColumn("schema"));
    def.addColumn(stringColumn("table"));
    def.addColumn(stringColumn("column"));
    def.addColumn(numericColumn("sql_type"));
    def.addColumn(stringColumn("type_desc"));
    def.addColumn(numericColumn("size"));
    def.addColumn(numericColumn("scale"));
    def.addColumn(booleanColumn("not_null"));
    def.addColumn(stringColumn("default"));
    def.addColumn(stringColumn("index_str"));
    def.addColumn(numericColumn("seq_no"));

    // Set to immutable
    def.setImmutable();

    DEF_DATA_TABLE_DEF = def;

  }

}
