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

import java.applet.Applet;
import java.awt.*;
import java.util.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.table.*;
import javax.swing.event.*;
import javax.swing.border.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.*;
import org.lazy8.nu.ledger.main.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class SQLReport extends TextReport {

  JFrame frame;
  // The query/results window.

  JTextArea queryTextArea;
  JComponent queryAggregate;
  JTable table;
  JPanel mainPanel;

  TableSorter sorter;
  JDBCAdapter dataBase;
  JScrollPane tableAggregate;

  /**
   *  Constructor for the SQLReport object
   *
   *@param  helpViewerin  Description of the Parameter
   */
  public SQLReport(View view ) {
    super(view, Translator.getTranslation("SQL report"),"sqlrep","lazy8ledger-sqlreport");


    JPanel jPanel1 = new JPanel();

    jPanel1.setLayout(new BoxLayout(jPanel1, BoxLayout.X_AXIS));

    JPanel jPanel4 = new JPanel();
    jPanel4.setLayout(new GridLayout(6, 2));
    jPanel1.add(jPanel4);

    AddSaveFilePane(jPanel4, "sqlrep");
    jPanel4.add(new JPanel());
    jPanel4.add(new JPanel());
    AddButtonComponents(jPanel4, "sqlrep");

    // Create the query text area and label.
    queryTextArea = new JTextArea();
    queryTextArea.setLineWrap(true);
    MouseListener l =
      new MouseListener() {
        public void mousePressed(MouseEvent e) {
        }

        public void mouseReleased(MouseEvent e) {
        }

        public void mouseEntered(MouseEvent e) {
          Lazy8Ledger.ShowContextHelp(SQLReport.this.view, "sqlrep" ,"Select");
        }

        public void mouseExited(MouseEvent e) {
        }

        public void mouseClicked(MouseEvent e) {
        }
      };
    queryTextArea.addMouseListener(l);
    queryAggregate = new JScrollPane(queryTextArea);
    queryAggregate.setBorder(new BevelBorder(BevelBorder.LOWERED));

    // Create the table.
    tableAggregate = createTable();
    tableAggregate.setBorder(new BevelBorder(BevelBorder.LOWERED));

    // Add all the components to the main panel.
    jPanel1.add(queryAggregate);
    //we are letting this table be invisable for now
    //        add(tableAggregate,BorderLayout.CENTER);

    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      buttonExit();
      return ;
    }
    dataBase = new JDBCAdapter(dc.con);
    sorter.setModel(dataBase);

    loadReport();
    JPanel horizontalSpacePanel = new JPanel();
    horizontalSpacePanel.setLayout(new BoxLayout(horizontalSpacePanel, BoxLayout.X_AXIS));
    horizontalSpacePanel.add(Box.createHorizontalGlue());
    horizontalSpacePanel.add(jPanel1);
    horizontalSpacePanel.add(Box.createHorizontalGlue());

    setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
    add(Box.createVerticalGlue());
    add(horizontalSpacePanel);
    add(Box.createVerticalGlue());
  }

  /**
   *  Description of the Method
   */
  public void ClearReport() {
    if (queryTextArea != null)
      queryTextArea.setText("");
  }

  /**
   *  Description of the Method
   *
   *@param  pReport  Description of the Parameter
   */
  public void SaveReport(Properties pReport) {
    pReport.setProperty("SQLstatement", queryTextArea.getText());

  }

  /**
   *  Description of the Method
   *
   *@param  pReport  Description of the Parameter
   */
  public void LoadReport(Properties pReport) {
    if (queryTextArea != null)
      queryTextArea.setText(pReport.getProperty("SQLstatement"));
  }

  /**
   *  Description of the Method
   */
  public void buttonExit() {
    super.buttonExit();
  }

  /**
   *  Description of the Method
   *
   *@param  e  Description of the Parameter
   */
  public void itemStateChanged(ItemEvent e) {
    if (e.getStateChange() != ItemEvent.SELECTED)
      return;
    if (jReports != null)
      loadReport();
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public String buttonGetReport() {
    dataBase.executeQuery(queryTextArea.getText(), view);
    //get the largest size for each column by checking the
    //contents of the columns
    int[] iColumnSizes = new int[sorter.getColumnCount() + 1];
    for (int i = 0; i < sorter.getColumnCount(); i++) {
      iColumnSizes[i + 1] = sorter.getColumnName(i).length();
      for (int j = 0; j < sorter.getRowCount(); j++)
        if (sorter.getValueAt(j, i).toString().length() > iColumnSizes[i + 1])
          iColumnSizes[i + 1] = sorter.getValueAt(j, i).toString().length();

      iColumnSizes[i + 1] += 1;
      //need one space between the columns
    }

    //get the column headers
    initializeRow(iColumnSizes, sorter.getColumnCount());
    for (int i = 0; i < sorter.getColumnCount(); i++)
      addField(sorter.getColumnName(i), iColumnSizes, i + 1,
          -1, false);

    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);

    //write a line under headers
    String sLotsOfLines = new String("-----------------------------------------------------------------------------------------------------------------");
    initializeRow(iColumnSizes, sorter.getColumnCount());
    for (int i = 0; i < sorter.getColumnCount(); i++)
      addField(sLotsOfLines, iColumnSizes, i + 1,
          -1, false);

    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);

    //write all the rows
    for (int j = 0; j < sorter.getRowCount(); j++) {
      initializeRow(iColumnSizes, sorter.getColumnCount());
      for (int i = 0; i < sorter.getColumnCount(); i++)
        addField(sorter.getValueAt(j, i).toString(), iColumnSizes, i + 1,
            -1, false);

      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
    }
    return (String) jReports.getSelectedItem();
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public JScrollPane createTable() {
    sorter = new TableSorter();

    //connect();
    //fetch();

    // Create the table
    table = new JTable(sorter);
    // Use a scrollbar, in case there are many columns.
    table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

    // Install a mouse listener in the TableHeader as the sorter UI.
    sorter.addMouseListenerToHeaderInTable(table);

    JScrollPane scrollpane = new JScrollPane(table);

    return scrollpane;
  }

}


