/*
 *  Copyright (C) 2002 Lazy Eight Data HB, Thomas Dilts This program is free
 *  software; you can redistribute it and/or modify it under the terms of the
 *  GNU General Public License as published by the Free Software Foundation;
 *  either version 2 of the License, or (at your option) any later version. This
 *  program is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 *  details. You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For more
 *  information, surf to www.lazy8.nu or email lazy8@telia.com
 */
package org.lazy8.nu.ledger.jdbc;

import java.util.Vector;
import java.sql.*;
import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.event.TableModelEvent;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.util.help.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class JDBCAdapter extends AbstractTableModel {
  Connection connection;
  Statement statement;
  ResultSet resultSet;
  String[] columnNames = {};
  Vector rows = new Vector();
  ResultSetMetaData metaData;

  /**
   *  Constructor for the JDBCAdapter object
   *
   *@param  conIn  Description of the Parameter
   */
  public JDBCAdapter(Connection conIn) {
    connection = conIn;
    try {
      statement = connection.createStatement();
    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("Error:" + e.getMessage());
    }
  }

  /**
   *  Description of the Method
   *
   *@param  query  Description of the Parameter
   *@param  jf     Description of the Parameter
   */
  public void executeQuery(String query, JFrame jf) {
    if (connection == null || statement == null) {
      SystemLog.ErrorPrintln("There is no database to execute the query.");
      return;
    }
    try {
      resultSet = statement.executeQuery(query);
      metaData = resultSet.getMetaData();

      int numberOfColumns = metaData.getColumnCount();
      columnNames = new String[numberOfColumns];
      // Get the column names and cache them.
      // Then we can close the connection.
      for (int column = 0; column < numberOfColumns; column++)
        columnNames[column] = metaData.getColumnLabel(column + 1);

      // Get all rows.
      rows = new Vector();
      while (resultSet.next()) {
        Vector newRow = new Vector();
        for (int i = 1; i <= getColumnCount(); i++)
          newRow.addElement(resultSet.getObject(i));

        rows.addElement(newRow);
      }
      //  close(); Need to copy the metaData, bug in jdbc:odbc driver.
      fireTableChanged(null);
      // Tell the listeners a new table has arrived.
    }
    catch (SQLException ex) {

      JOptionPane.showMessageDialog(jf,
          ex.toString(),
          Translator.getTranslation("Cannot process report"),
          JOptionPane.PLAIN_MESSAGE);

      SystemLog.ErrorPrintln(ex);
    }
  }

  /**
   *  Description of the Method
   *
   *@exception  SQLException  Description of the Exception
   */
  public void close()
    throws SQLException {
    resultSet.close();
    statement.close();
  }

  /**
   *  Description of the Method
   *
   *@exception  Throwable  Description of the Exception
   */
  protected void finalize()
    throws Throwable {
    close();
    super.finalize();
  }

  //////////////////////////////////////////////////////////////////////////
  //
  //             Implementation of the TableModel Interface
  //
  //////////////////////////////////////////////////////////////////////////

  // MetaData

  /**
   *  Gets the columnName attribute of the JDBCAdapter object
   *
   *@param  column  Description of the Parameter
   *@return         The columnName value
   */
  public String getColumnName(int column) {
    if (columnNames[column] != null)
      return columnNames[column];
    else
      return "";
  }

  /**
   *  Gets the columnClass attribute of the JDBCAdapter object
   *
   *@param  column  Description of the Parameter
   *@return         The columnClass value
   */
  public Class getColumnClass(int column) {
    return String.class;
    /*
     *  int type;
     *  try {
     *  type = metaData.getColumnType(column+1);
     *  }
     *  catch (SQLException e) {
     *  return super.getColumnClass(column);
     *  }
     *  switch(type) {
     *  case Types.CHAR:
     *  case Types.VARCHAR:
     *  case Types.LONGVARCHAR:
     *  return String.class;
     *  case Types.BIT:
     *  return Boolean.class;
     *  case Types.TINYINT:
     *  case Types.SMALLINT:
     *  case Types.INTEGER:
     *  return Integer.class;
     *  case Types.BIGINT:
     *  return Long.class;
     *  case Types.FLOAT:
     *  case Types.DOUBLE:
     *  return Double.class;
     *  case Types.DATE:
     *  return java.sql.Date.class;
     *  default:
     *  return Object.class;
     *  }
     */
  }

  /**
   *  Gets the cellEditable attribute of the JDBCAdapter object
   *
   *@param  row     Description of the Parameter
   *@param  column  Description of the Parameter
   *@return         The cellEditable value
   */
  public boolean isCellEditable(int row, int column) {
    try {
      return metaData.isWritable(column + 1);
    }
    catch (SQLException e) {
      return false;
    }
  }

  /**
   *  Gets the columnCount attribute of the JDBCAdapter object
   *
   *@return    The columnCount value
   */
  public int getColumnCount() {
    return columnNames.length;
  }

  // Data methods

  /**
   *  Gets the rowCount attribute of the JDBCAdapter object
   *
   *@return    The rowCount value
   */
  public int getRowCount() {
    return rows.size();
  }

  /**
   *  Gets the valueAt attribute of the JDBCAdapter object
   *
   *@param  aRow     Description of the Parameter
   *@param  aColumn  Description of the Parameter
   *@return          The valueAt value
   */
  public Object getValueAt(int aRow, int aColumn) {
    Vector row = (Vector) rows.elementAt(aRow);
    return dbRepresentation(aColumn, row.elementAt(aColumn));
  }

  /**
   *  Description of the Method
   *
   *@param  column  Description of the Parameter
   *@param  value   Description of the Parameter
   *@return         Description of the Return Value
   */
  public String dbRepresentation(int column, Object value) {
    int type;

    if (value == null)
      return "null";

    try {
      type = metaData.getColumnType(column + 1);
    }
    catch (SQLException e) {
      return value.toString();
    }
    switch (type) {
      case Types.INTEGER:
        return IntegerField.ConvertIntToLocalizedString((Integer) value);
      case Types.DOUBLE:
        return DoubleField.ConvertDoubleToLocalizedString((Double) value);
      case Types.FLOAT:
        return DoubleField.ConvertDoubleToLocalizedString(
            new Double(((Float) value).doubleValue()));
      //    return value.toString();
      case Types.BIT:
        return ((Boolean) value).booleanValue() ? "1" : "0";
      case Types.VARCHAR:
        return value.toString();
      case Types.DATE:
        return DateField.ConvertDateToLocalizedString((Date) value);
      //            return value.toString(); // This will need some conversion.
      default:
        return StringBinaryConverter.BinaryToString((byte[]) value);
      //return "\""+value.toString()+"\"";
    }

  }

  /**
   *  Sets the valueAt attribute of the JDBCAdapter object
   *
   *@param  value   The new valueAt value
   *@param  row     The new valueAt value
   *@param  column  The new valueAt value
   */
  public void setValueAt(Object value, int row, int column) {
    try {
      String tableName = metaData.getTableName(column + 1);
      // Some of the drivers seem buggy, tableName should not be null.
      if (tableName == null)
        SystemLog.ProblemPrintln("Table name returned null.");

      String columnName = getColumnName(column);
      String query =
          "update " + tableName +
          " set " + columnName + " = " + dbRepresentation(column, value) +
          " where ";
      // We don't have a model of the schema so we don't know the
      // primary keys or which columns to lock on. To demonstrate
      // that editing is possible, we'll just lock on everything.
      for (int col = 0; col < getColumnCount(); col++) {
        String colName = getColumnName(col);
        if (colName.equals(""))
          continue;
        if (col != 0)
          query = query + " and ";

        query = query + colName + " = " +
            dbRepresentation(col, getValueAt(row, col));
      }
      SystemLog.ProblemPrintln(query);
      SystemLog.ProblemPrintln("Not sending update to database");
      // statement.executeQuery(query);
    }
    catch (SQLException e) {
      //     e.printStackTrace();
      SystemLog.ErrorPrintln("Update failed");
    }
    Vector dataRow = (Vector) rows.elementAt(row);
    dataRow.setElementAt(value, column);

  }
}

