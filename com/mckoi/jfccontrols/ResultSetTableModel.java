/**
 * com.mckoi.jfccontrols.ResultSetTableModel  02 Aug 2000
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

package com.mckoi.jfccontrols;

import javax.swing.table.*;
import java.sql.*;

/**
 * An implementation of a javax.swing.table.TableModel that updates itself from
 * a scrollable java.sql.ResultSet source.  This directly maps columns from a
 * query to columns in the table model.  If you wish to filter information
 * from the result set before it is output as a table use
 * FilteredResultSetTableModel.
 *
 * @author Tobias Downer
 */

public class ResultSetTableModel extends AbstractTableModel {

  /**
   * The scrollable ResultSet source.
   */
  private ResultSet result_set;

  /**
   * The ResultSetMetaData object for this result set.
   */
  private ResultSetMetaData meta_data;

  /**
   * The number of rows in the result set.
   */
  private int row_count;

  /**
   * Constructs the model.
   */
  public ResultSetTableModel(ResultSet result_set) {
    super();
    if (result_set != null) {
      updateResultSet(result_set);
    }
    else {
      clear();
    }
  }

  public ResultSetTableModel() {
    this(null);
  }

  /**
   * Updates the result set in this model with the given ResultSet object.
   */
  public void updateResultSet(ResultSet result_set) {
    try {
      if (this.result_set != null) {
        clear();
      }

      this.result_set = result_set;
      this.meta_data = result_set.getMetaData();

      if (result_set.last()) {
        row_count = result_set.getRow();
      }
      else {
        row_count = 0;
      }

      fireTableStructureChanged();
    }
    catch (SQLException e) {
      throw new Error("SQL Exception: " + e.getMessage());
    }
  }

  /**
   * Clears the model of the current result set.
   */
  public void clear() {
    // Close the old result set if needed.
    if (result_set != null) {
      try {
        result_set.close();
      }
      catch (SQLException e) {
        // Just incase the JDBC driver can't close a result set twice.
        e.printStackTrace();
      }
    }
    result_set = null;
    meta_data = null;
    row_count = 0;
    fireTableStructureChanged();
  }

  // ---------- Implemented from AbstractTableModel ----------

  public int getRowCount() {
    return row_count;
  }

  public int getColumnCount() {
    if (meta_data != null) {
      try {
        return meta_data.getColumnCount();
      }
      catch (SQLException e) {
        throw new Error("SQL Exception: " + e.getMessage());
      }
    }
    return 0;
  }

  public String getColumnName(int column) {
    if (meta_data != null) {
      try {
        return meta_data.getColumnLabel(column + 1);
      }
      catch (SQLException e) {
        throw new Error("SQL Exception: " + e.getMessage());
      }
    }
    throw new Error("No columns!");
  }

  public Object getValueAt(int row, int column) {
    if (result_set != null) {
      try {
        result_set.absolute(row + 1);
        return result_set.getObject(column + 1);
      }
      catch (SQLException e) {
        throw new Error("SQL Exception: " + e.getMessage());
      }
    }
    throw new Error("No contents!");
  }

}
