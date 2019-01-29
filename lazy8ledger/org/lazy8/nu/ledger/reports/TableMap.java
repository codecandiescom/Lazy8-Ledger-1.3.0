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
package org.lazy8.nu.ledger.reports;



import javax.swing.table.*;
import javax.swing.event.TableModelListener;
import javax.swing.event.TableModelEvent;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class TableMap extends AbstractTableModel implements TableModelListener {
  /**
   *  Description of the Field
   */
  protected TableModel model;

  /**
   *  Gets the model attribute of the TableMap object
   *
   *@return    The model value
   */
  public TableModel getModel() {
    return model;
  }

  /**
   *  Sets the model attribute of the TableMap object
   *
   *@param  model  The new model value
   */
  public void setModel(TableModel model) {
    this.model = model;
    model.addTableModelListener(this);
  }

  // By default, Implement TableModel by forwarding all messages
  // to the model.

  /**
   *  Gets the valueAt attribute of the TableMap object
   *
   *@param  aRow     Description of the Parameter
   *@param  aColumn  Description of the Parameter
   *@return          The valueAt value
   */
  public Object getValueAt(int aRow, int aColumn) {
    return model.getValueAt(aRow, aColumn);
  }

  /**
   *  Sets the valueAt attribute of the TableMap object
   *
   *@param  aValue   The new valueAt value
   *@param  aRow     The new valueAt value
   *@param  aColumn  The new valueAt value
   */
  public void setValueAt(Object aValue, int aRow, int aColumn) {
    model.setValueAt(aValue, aRow, aColumn);
  }

  /**
   *  Gets the rowCount attribute of the TableMap object
   *
   *@return    The rowCount value
   */
  public int getRowCount() {
    return (model == null) ? 0 : model.getRowCount();
  }

  /**
   *  Gets the columnCount attribute of the TableMap object
   *
   *@return    The columnCount value
   */
  public int getColumnCount() {
    return (model == null) ? 0 : model.getColumnCount();
  }

  /**
   *  Gets the columnName attribute of the TableMap object
   *
   *@param  aColumn  Description of the Parameter
   *@return          The columnName value
   */
  public String getColumnName(int aColumn) {
    return model.getColumnName(aColumn);
  }

  /**
   *  Gets the columnClass attribute of the TableMap object
   *
   *@param  aColumn  Description of the Parameter
   *@return          The columnClass value
   */
  public Class getColumnClass(int aColumn) {
    return model.getColumnClass(aColumn);
  }

  /**
   *  Gets the cellEditable attribute of the TableMap object
   *
   *@param  row     Description of the Parameter
   *@param  column  Description of the Parameter
   *@return         The cellEditable value
   */
  public boolean isCellEditable(int row, int column) {
    return model.isCellEditable(row, column);
  }

  //
  // Implementation of the TableModelListener interface,
  //

  // By default forward all events to all the listeners.
  /**
   *  Description of the Method
   *
   *@param  e  Description of the Parameter
   */
  public void tableChanged(TableModelEvent e) {
    fireTableChanged(e);
  }
}

