/**
 * com.mckoi.database.GTDataSource  27 Apr 2001
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

import com.mckoi.database.global.SQLTypes;

/**
 * A base class for a dynamically generated data source.  While this inherits
 * MutableTableDataSource (so we can make a DataTable out of it) a GTDataSource
 * derived class may not be mutable.  For example, an implementation of this
 * class may produce a list of a columns in all tables.  You would typically
 * not want a user to change this information unless they run a DML command.
 *
 * @author Tobias Downer
 */

abstract class GTDataSource implements MutableTableDataSource {

  /**
   * The TransactionSystem object for this table.
   */
  private TransactionSystem system;


  /**
   * Constructor.
   */
  public GTDataSource(TransactionSystem system) {
    this.system = system;
  }

  /**
   * This inner class represents a Row Enumeration for this table.
   */
  final class VTRowEnumeration implements RowEnumeration {
    int index = 0;
    final int row_count_store;

    public VTRowEnumeration() {
      row_count_store = getRowCount();
    }

    public final boolean hasMoreRows() {
      return (index < row_count_store);
    }

    public final int nextRowIndex() {
      ++index;
      return index - 1;
    }
  }

  // ---------- Implemented from TableDataSource ----------

  public TransactionSystem getSystem() {
    return system;
  }

  public abstract DataTableDef getDataTableDef();

  public abstract int getRowCount();

  public RowEnumeration rowEnumeration() {
    return new VTRowEnumeration();
  }

  public SelectableScheme getColumnScheme(int column) {
    return new BlindSearch(this, column);
  }

  public abstract DataCell getCellContents(final int column, final int row);

  public int compareCellTo(DataCell ob, int column, int row) {
    DataCell cell = getCellContents(column, row);
    int result = ob.compareTo(cell);
    if (result > 0) {
      return Table.GREATER_THAN;
    }
    else if (result == 0) {
      return Table.EQUAL;
    }
    return Table.LESS_THAN;
  }

  // ---------- Implemented from MutableTableDataSource ----------

  public int addRow(RowData row_data) {
    throw new Error("Functionality not available.");
  }

  public void removeRow(int row_index) {
    throw new Error("Functionality not available.");
  }

  public int updateRow(int row_index, RowData row_data) {
    throw new Error("Functionality not available.");
  }

  public MasterTableJournal getJournal() {
    throw new Error("Functionality not available.");
  }

  public void dispose() {
  }

  public void addRootLock() {
    // No need to root locks
  }

  public void removeRootLock() {
    // No need to row locks
  }

  // ---------- Static ----------

  /**
   * Convenience methods for constructing a DataTableDef for the dynamically
   * generated table.
   */
  protected static DataTableColumnDef stringColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setNotNull(true);
    column.setSQLType(SQLTypes.VARCHAR);
    column.setSize(Integer.MAX_VALUE);
    column.setScale(-1);
    column.setIndexScheme("BlindSearch");
    return column;
  }

  protected static DataTableColumnDef booleanColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setNotNull(true);
    column.setSQLType(SQLTypes.BIT);
    column.setSize(-1);
    column.setScale(-1);
    column.setIndexScheme("BlindSearch");
    return column;
  }

  protected static DataTableColumnDef numericColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setNotNull(true);
    column.setSQLType(SQLTypes.NUMERIC);
    column.setSize(-1);
    column.setScale(-1);
    column.setIndexScheme("BlindSearch");
    return column;
  }

  protected static DataTableColumnDef dateColumn(String name) {
    DataTableColumnDef column = new DataTableColumnDef();
    column.setName(name);
    column.setNotNull(true);
    column.setSQLType(SQLTypes.TIMESTAMP);
    column.setSize(-1);
    column.setScale(-1);
    column.setIndexScheme("BlindSearch");
    return column;
  }

}
