/**
 * com.mckoi.database.GTTableInfoDataSource  27 Apr 2001
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

/**
 * An implementation of MutableTableDataSource that presents information
 * about the tables in all schema.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTTableInfoDataSource extends GTDataSource {

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
  public GTTableInfoDataSource(Transaction transaction) {
    super(transaction.getSystem());
    this.transaction = transaction;
  }

  /**
   * Initialize the data source.
   */
  public GTTableInfoDataSource init() {
    // All the tables
    TableName[] list = transaction.getTableList();
    visible_tables = new DataTableDef[list.length];
    row_count = list.length;
    for (int i = 0; i < list.length; ++i) {
      DataTableDef def = transaction.getDataTableDef(list[i]);
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
    final DataTableDef def = visible_tables[row];
    switch (column) {
      case 0:  // schema
        return new StringDataCell(200, def.getSchema());
      case 1:  // name
        return new StringDataCell(200, def.getName());
      case 2:  // type
        // PENDING: Handling of views/etc.
        String type = "TABLE";
        if (def.getSchema().equals("SYS_INFO")) {
          type = "SYSTEM TABLE";
        }
        return new StringDataCell(200, type);
      case 3:  // other
        // Table notes, etc.  (future enhancement)
        return new StringDataCell(65536, "");
      default:
        throw new Error("Column out of bounds.");
    }
  }

  // ---------- Overwritten from GTDataSource ----------

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
    def.setName("sUSRTableInfo");

    // Add column definitions
    def.addColumn(stringColumn("schema"));
    def.addColumn(stringColumn("name"));
    def.addColumn(stringColumn("type"));
    def.addColumn(stringColumn("other"));

    // Set to immutable
    def.setImmutable();

    DEF_DATA_TABLE_DEF = def;

  }

}
