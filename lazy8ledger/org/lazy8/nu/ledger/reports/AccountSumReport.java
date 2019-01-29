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

import java.awt.event.*;
import javax.swing.*;
import java.awt.*;
import java.text.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import javax.swing.table.*;
import javax.swing.border.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.util.help.*;
import org.gjt.sp.jedit.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class AccountSumReport extends TextReport {
  final static int MAX_ROWS = 100;
  ArrayList[] columnsAccount;
  ArrayList[] columnsSecondLevel;
  final String[] namesAccountColumns = {
      Translator.getTranslation("Account from"),
      Translator.getTranslation("Account to"),
      Translator.getTranslation("Commentary"),
      Translator.getTranslation("Incomming"),
      Translator.getTranslation("Minus"),
      Translator.getTranslation("First level group")};
  final String[] namesSecondLevelColumns = {
      Translator.getTranslation("First level group"),
      Translator.getTranslation("Commentary"),
      Translator.getTranslation("Minus"),
      Translator.getTranslation("Second level group")};
  JComboBox SecondLevelComboBox;
  private final static int ACC_COL_ACC_NUM_FROM = 0;
  private final static int ACC_COL_ACC_NUM_TO = 1;
  private final static int ACC_COL_COMMENT = 2;
  private final static int ACC_COL_BEFORESUM = 3;
  private final static int ACC_COL_MINUS = 4;
  private final static int ACC_COL_GROUP = 5;
  private final static int ACC_COL_SUM = 6;
  private final static int ACC_COL_ACC_NAME = 7;
  private final static int ACC_COL_GROUP2 = 8;

  private final static int GRP_COL_GRP1 = 0;
  private final static int GRP_COL_COMMENT = 1;
  private final static int GRP_COL_MINUS = 2;
  private final static int GRP_COL_GRP2 = 3;
  JTable tableSecondLevelView;
  JTable tableAccountView;
  AbstractTableModel dataAccountModel;
  AbstractTableModel dataSecondLevelModel;
  DataComboBox accountComboBox;

  /**
   *  Constructor for the AccountSumReport object
   *
   *@param  sReportName   Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   *@param  desktop       Description of the Parameter
   *@param  parent        Description of the Parameter
   */
  public AccountSumReport(View view ,String sReportName,String frameName) {
    super(view, Translator.getTranslation("Account summary"),"sumrep",frameName);
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      buttonExit();
      return ;
    }


    JPanel reportDefinitions = new JPanel();
    createReportDefinitionPane(reportDefinitions);
    jReports.setSelectedItem(sReportName + "." + SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE));
    loadReport();

    //put up a simple fromdate/todate/reporttitle frame
    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(7, 2));

    AddMiscComponents(jPanel1, Translator.getTranslation(sReportName), "sumrep");
    //      jPanel1.add(new JLabel(""));
    //      jPanel1.add(new JLabel(""));
    AddButtonComponents(jPanel1);
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
   *  Constructor for the AccountSumReport object
   *
   *@param  helpViewerin  Description of the Parameter
   *@param  desktop       Description of the Parameter
   *@param  parent        Description of the Parameter
   */
  public AccountSumReport(View view) {
    super(view, Translator.getTranslation("Account summary"),"accountsum","lazy8ledger-specialreport");


    createReportDefinitionPane(this);
    loadReport();
    //Rectangle rvMain;
    //rvMain = desktop.getBounds();
    //setSize(rvMain.width, rvMain.height);
    //setLocation(0, 0);
  }

  /**
   *  Description of the Method
   *
   *@param  frame       Description of the Parameter
   *@param  helpViewer  Description of the Parameter
   */
  public void createReportDefinitionPane(Container frame) {
    columnsSecondLevel = new ArrayList[namesSecondLevelColumns.length];
    columnsAccount = new ArrayList[namesAccountColumns.length];

    frame.setLayout(new BorderLayout());

    JPanel jPanel2 = new JPanel();
    jPanel2.setLayout(new GridLayout(7, 2));

    jPanel2.add(new JPanel());
    jPanel2.add(new JPanel());
    jPanel2.add(new JPanel());
    jPanel2.add(new JPanel());
    AddSaveFilePane(jPanel2, "spcrep");
    jPanel2.add(new JPanel());
    jPanel2.add(new JPanel());
    HelpedButton b1 = new HelpedButton(Translator.getTranslation("Get report"),
        "GetReport", "sumrep",view);
    jPanel2.add(b1);
    b1.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonGetReportPreperation(false);
        }
      }
        );
    HelpedButton b10 = new HelpedButton(Translator.getTranslation("Get report on clipboard"),
        "GetReportOntoClipboard", "sumrep",view);
    jPanel2.add(b10);
    b10.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonGetReportPreperation(true);
        }
      }
        );

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(7, 2));

    AddMiscComponents(jPanel1, "", "sumrep");
    jPanel1.add(new JPanel());
    jPanel1.add(new JPanel());

    HelpedButton b11 = new HelpedButton(Translator.getTranslation("Exit"),
        "Exit", "sumrep",view);
    jPanel1.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonExit();
        }
      }
        );
    HelpedButton b12 = new HelpedButton(Translator.getTranslation("Help"),
        "Help", "sumrep",view);
    jPanel1.add(b12);
    b12.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonHelp();
        }
      }
        );

    JPanel jPanel3 = new JPanel();
    jPanel3.setLayout(new GridLayout(1, 2));

    jPanel3.add(jPanel1);
    jPanel3.add(jPanel2);

    frame.add(jPanel3, BorderLayout.SOUTH);

    JPanel jPanel11 = new JPanel();
    jPanel11.setLayout(new GridLayout(1, 2));
    frame.add(jPanel11, BorderLayout.CENTER);

    JPanel jPanel5 = new JPanel();
    jPanel11.add(jPanel5);
    jPanel5.setBorder(new javax.swing.border.TitledBorder(
        Translator.getTranslation("Account display and grouping")));
    JPanel jPanel6 = new JPanel();
    jPanel11.add(jPanel6);
    jPanel6.setBorder(new javax.swing.border.TitledBorder(
        Translator.getTranslation("Second level grouping")));

    createTables(jPanel5, jPanel6);
  }

  /**
   *  Description of the Method
   */
  public void ClearReport() {
    for (int i = 0; i < namesAccountColumns.length; i++)
      columnsAccount[i] = new ArrayList();

    for (int i = 0; i < namesSecondLevelColumns.length; i++)
      columnsSecondLevel[i] = new ArrayList();

    if (dataSecondLevelModel == null || dataAccountModel == null)
      return;
    dataSecondLevelModel.fireTableChanged(null);
    dataAccountModel.fireTableChanged(null);

  }

  /**
   *  Description of the Method
   *
   *@param  pReport  Description of the Parameter
   */
  public void SaveReport(Properties pReport) {
    for (int i = 0; i < namesAccountColumns.length; i++)
      for (int j = 0; j < columnsAccount[0].size(); j++)
        if (i == ACC_COL_ACC_NUM_FROM || i == ACC_COL_ACC_NUM_TO)
          pReport.setProperty("AccountRow" + j + "Col" + i,
              IntegerField.ConvertLocalizedStringToInt(
              columnsAccount[i].get(j).toString()).toString());
        else
          pReport.setProperty("AccountRow" + j + "Col" + i,
              columnsAccount[i].get(j).toString());

    for (int i = 0; i < namesSecondLevelColumns.length; i++)
      for (int j = 0; j < columnsSecondLevel[0].size(); j++)
        pReport.setProperty("SecondLevelRow" + j + "Col" + i,
            columnsSecondLevel[i].get(j).toString());

    pReport.setProperty("AccountRows", Integer.toString(
        columnsAccount[0].size()));
    pReport.setProperty("SecondLevelRows", Integer.toString(
        columnsSecondLevel[0].size()));
    pReport.setProperty("ReportName", jTextField3.getText());
  }

  /**
   *  Description of the Method
   *
   *@param  pReport  Description of the Parameter
   */
  public void LoadReport(Properties pReport) {
    if (namesSecondLevelColumns == null || namesSecondLevelColumns[0] == null)
      return;
    ClearReport();
    int rows = Integer.parseInt(pReport.getProperty("AccountRows"));
    for (int i = 0; i < namesAccountColumns.length; i++)
      for (int j = 0; j < rows; j++)
        switch (i) {
          case ACC_COL_ACC_NUM_FROM:
          case ACC_COL_ACC_NUM_TO:
            columnsAccount[i].add(
                IntegerField.ConvertIntToLocalizedString(
                new Integer(Integer.parseInt(
                pReport.getProperty("AccountRow" + j + "Col" + i).toString()))));
            break;
          case ACC_COL_BEFORESUM:
          case ACC_COL_MINUS:
            columnsAccount[i].add(new Boolean(
                pReport.getProperty("AccountRow" + j + "Col" + i)));
            break;
          default:
            columnsAccount[i].add(
                pReport.getProperty("AccountRow" + j + "Col" + i));
            break;
        }

    rows = Integer.parseInt(pReport.getProperty("SecondLevelRows"));
    for (int i = 0; i < namesSecondLevelColumns.length; i++)
      for (int j = 0; j < rows; j++)
        if (i == GRP_COL_MINUS)
          columnsSecondLevel[i].add(new Boolean(
              pReport.getProperty("SecondLevelRow" + j + "Col" + i)));
        else
          columnsSecondLevel[i].add(
              pReport.getProperty("SecondLevelRow" + j + "Col" + i));

    if (jTextField3 == null || dataSecondLevelModel == null || dataAccountModel == null)
      return;

    jTextField3.setText(pReport.getProperty("ReportName"));
    fillSecondLevelComboBox();

    dataSecondLevelModel.fireTableChanged(null);
    dataAccountModel.fireTableChanged(null);
    updateTables();
  }

  /**
   *  Description of the Method
   */
  public void buttonExit() {
    super.buttonExit();
  }

  /**
   *  Description of the Method
   */
  public void fillSecondLevelComboBox() {
    SecondLevelComboBox.removeAllItems();
    SecondLevelComboBox.addItem(new String(""));
    //we need to fill the SecondLevelComboBox.
    //Items entered must be uniq and in ascending order.
    //Go through the list and if not equal and less than, insert
    for (int i = 0; i < columnsAccount[0].size(); i++) {
      String sTest = (String) columnsAccount[ACC_COL_GROUP].get(i);
      if (sTest.length() == 0)
        continue;
      int iTestResult = 1;
      int j;
      for (j = 1; j < SecondLevelComboBox.getItemCount(); j++) {
        iTestResult = sTest.compareTo
            ((String) SecondLevelComboBox.getItemAt(j));
        if (iTestResult == 0)
          break;
        //its an exact copy, uniq allowed only

        if (iTestResult < 0) {
          //we need to insert it here.
          SecondLevelComboBox.insertItemAt(sTest, j);
          break;
        }
      }
      if (j == SecondLevelComboBox.getItemCount() && iTestResult > 0)
        SecondLevelComboBox.addItem(sTest);
      //just put it at the end

    }
    //take away any numbers already used in the group list
    for (int i = 0; i < columnsSecondLevel[0].size(); i++) {
      String sTest = (String) columnsSecondLevel[GRP_COL_GRP1].get(i);
      if (sTest == null || sTest.length() == 0)
        continue;
      int j;
      for (j = 1; j < SecondLevelComboBox.getItemCount(); j++) {
        int iTestResult = sTest.compareTo
            ((String) SecondLevelComboBox.getItemAt(j));
        if (iTestResult == 0) {
          SecondLevelComboBox.removeItemAt(j);
          break;
        }
        if (iTestResult < 0)
          break;
        //its not there
      }

    }
  }

  /**
   *  Description of the Method
   */
  public void updateTables() {
    // Turn off auto-resizing so that we can set column sizes programmatically.
    // In this mode, all columns will get their preferred widths, as set blow.
    tableAccountView.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
    //tableAccountView.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    tableAccountView.doLayout();

    // Set the width of the "Minus" column.
    //        TableColumn minusColumn = tableAccountView.getColumn(
    //               Translator.getTranslation("Minus"));
    //        minusColumn.setPreferredWidth(100);

    // Finish setting up the table.




    // Turn off auto-resizing so that we can set column sizes programmatically.
    // In this mode, all columns will get their preferred widths, as set blow.
    tableSecondLevelView.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
    // tableSecondLevelView.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);


    TableColumn SecondLevelColumn =
        tableSecondLevelView.getColumn(Translator.getTranslation("First level group"));
    SecondLevelColumn.setCellEditor(new DefaultCellEditor(SecondLevelComboBox));

    // Set a Color column renderer.
    DefaultTableCellRenderer SecondLevelColumnRenderer = new DefaultTableCellRenderer();
    SecondLevelColumn.setCellRenderer(SecondLevelColumnRenderer);

    // Set the width of the "Minus" column.
    //        TableColumn minusColumn = tableSecondLevelView.getColumn(
    //               Translator.getTranslation("Minus"));
    //        minusColumn.setPreferredWidth(100);

    // Finish setting up the table.


  }

  /**
   *  Description of the Method
   *
   *@param  jp1         Description of the Parameter
   *@param  jp2         Description of the Parameter
   *@param  helpViewer  Description of the Parameter
   */
  public void createTables(JPanel jp1, JPanel jp2) {
    SecondLevelComboBox = new JComboBox();

    // Create a model of the account level data.
    dataAccountModel =
      new AbstractTableModel() {
        // These methods always need to be implemented.
        public int getColumnCount() {
          return namesAccountColumns.length;
        }

        public int getRowCount() {
          return columnsAccount[0].size();
        }

        public Object getValueAt(int row, int col) {
          if (columnsAccount[col].get(row) == null) {
            if (col == ACC_COL_MINUS || col == ACC_COL_BEFORESUM)
              return new Boolean(false);
            else
              return new String("");
          }
          else
            return columnsAccount[col].get(row);
        }

        // The default implementations of these methods in
        // AbstractTableModel would work, but we can refine them.
        public String getColumnName(int column) {
          return namesAccountColumns[column];
        }

        public Class getColumnClass(int c) {
          if (c == ACC_COL_MINUS || c == ACC_COL_BEFORESUM)
            return new Boolean(true).getClass();
          else
            return new String().getClass();
          // return getValueAt(0, c).getClass();
        }

        public boolean isCellEditable(int row, int col) {
          return true;
        }

        public void setValueAt(Object aValue, int row, int col) {
          columnsAccount[col].set(row, aValue);
          if (col == ACC_COL_GROUP)
            fillSecondLevelComboBox();

        }
      };

    // do the second level tabel

    // Create a model of the SecondLevel level data.
    dataSecondLevelModel =
      new AbstractTableModel() {
        // These methods always need to be implemented.
        public int getColumnCount() {
          return namesSecondLevelColumns.length;
        }

        public int getRowCount() {
          return columnsSecondLevel[0].size();
        }

        public Object getValueAt(int row, int col) {
          if (columnsSecondLevel[col].get(row) == null) {
            if (col == GRP_COL_MINUS)
              return new Boolean(false);
            else
              return new String("");
          }
          else
            return columnsSecondLevel[col].get(row);
        }

        // The default implementations of these methods in
        // AbstractTableModel would work, but we can refine them.
        public String getColumnName(int column) {
          return namesSecondLevelColumns[column];
        }

        public Class getColumnClass(int c) {
          if (c == GRP_COL_MINUS)
            return new Boolean(true).getClass();
          else
            return new String().getClass();
          //              return getValueAt(0, c).getClass();
        }

        public boolean isCellEditable(int row, int col) {
          return true;
        }

        public void setValueAt(Object aValue, int row, int col) {
          columnsSecondLevel[col].set(row, aValue);
          if (col == GRP_COL_GRP1)
            fillSecondLevelComboBox();

        }
      };

    ClearReport();

    // Create the table
    tableAccountView = new JTable(dataAccountModel);
    // Create the table
    tableSecondLevelView = new JTable(dataSecondLevelModel);

    // Create a combo box to show that you can use one in a table.
    accountComboBox = new DataComboBox(new JdbcTable("Account", 2,view), true, "Account", "sumrep",view);
    accountComboBox.loadComboBox("AccDesc", "Account", (Integer) cc.comboBox.getSelectedItemsKey());

    JScrollPane scrollpane = new JScrollPane(tableAccountView);
    //scrollpane.setBorder(new BevelBorder(BevelBorder.LOWERED));
    scrollpane.setPreferredSize(new Dimension(200, 200));
    jp1.setLayout(new BorderLayout());
    jp1.add(scrollpane, BorderLayout.CENTER);

    JPanel jp11 = new JPanel();
    jp1.add(jp11, BorderLayout.SOUTH);
    jp11.setLayout(new GridLayout(1, 2));

    HelpedButton b11 = new HelpedButton(Translator.getTranslation("Add row"),
        "AddRow", "sumrep",view);
    jp11.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonAddRow1Report();
        }
      }
        );
    b11 = new HelpedButton(Translator.getTranslation("Delete row"),
        "DeleteRow", "sumrep",view);
    jp11.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonDeleteRow1Report();
        }
      }
        );

    JScrollPane scrollpane2 = new JScrollPane(tableSecondLevelView);
    //scrollpane2.setBorder(new BevelBorder(BevelBorder.LOWERED));
    scrollpane2.setPreferredSize(new Dimension(200, 200));
    jp2.setLayout(new BorderLayout());
    jp2.add(scrollpane2, BorderLayout.CENTER);

    jp11 = new JPanel();
    jp2.add(jp11, BorderLayout.SOUTH);
    jp11.setLayout(new GridLayout(1, 2));

    b11 = new HelpedButton(Translator.getTranslation("Add row"),
        "AddRowSecondary", "sumrep",view);
    jp11.add(b11, BorderLayout.SOUTH);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonAddRow2Report();
        }
      }
        );
    b11 = new HelpedButton(Translator.getTranslation("Delete row"),
        "DeleteRowSecondary", "sumrep",view);
    jp11.add(b11, BorderLayout.SOUTH);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonDeleteRow2Report();
        }
      }
        );

    updateTables();
  }

  /**
   *  Description of the Method
   */
  private void buttonAddRow1Report() {
    int j = tableAccountView.getSelectedRow();
    for (int i = 0; i < namesAccountColumns.length; i++)
      if (j < 0) {
        if (i == ACC_COL_SUM)
          columnsAccount[i].add(new Double(0));
        else if (i == ACC_COL_MINUS || i == ACC_COL_BEFORESUM)
          columnsAccount[i].add(new Boolean(false));
        else
          columnsAccount[i].add(new String(""));
      }
      else
          if (i == ACC_COL_SUM)
        columnsAccount[i].add(j, new Double(0));
      else if (i == ACC_COL_MINUS || i == ACC_COL_BEFORESUM)
        columnsAccount[i].add(j, new Boolean(false));
      else
        columnsAccount[i].add(j, new String(""));

    updateTables();
  }

  /**
   *  Description of the Method
   */
  private void buttonDeleteRow1Report() {
    int j = tableAccountView.getSelectedRow();
    if (j < 0)
      return;
    for (int i = 0; i < namesAccountColumns.length; i++)

      columnsAccount[i].remove(j);

    dataAccountModel.fireTableChanged(null);
    updateTables();

  }

  /**
   *  Description of the Method
   */
  private void buttonAddRow2Report() {
    int j = tableSecondLevelView.getSelectedRow();

    for (int i = 0; i < namesSecondLevelColumns.length; i++)
      if (j < 0) {
        if (i == GRP_COL_MINUS)
          columnsSecondLevel[i].add(new Boolean(false));
        else
          columnsSecondLevel[i].add(new String(""));
      }
      else
          if (i == GRP_COL_MINUS)
        columnsSecondLevel[i].add(j, new Boolean(false));
      else
        columnsSecondLevel[i].add(j, new String(""));

    dataSecondLevelModel.fireTableChanged(null);
    updateTables();
  }

  /**
   *  Description of the Method
   */
  private void buttonDeleteRow2Report() {
    int j = tableSecondLevelView.getSelectedRow();
    if (j < 0)
      return;
    for (int i = 0; i < namesSecondLevelColumns.length; i++)
      columnsSecondLevel[i].remove(j);

    dataSecondLevelModel.fireTableChanged(null);
    updateTables();
  }

  /**
   *  Gets the secondLevelGroup attribute of the AccountSumReport object
   *
   *@param  sFindIt  Description of the Parameter
   *@return          The secondLevelGroup value
   */
  private int getSecondLevelGroup(String sFindIt) {
    for (int i = 0; i < columnsSecondLevel[0].size(); i++) {
      String sTest = (String) columnsSecondLevel[GRP_COL_GRP1].get(i);
      if (sTest == null || sTest.length() == 0)
        continue;
      if (sFindIt.compareTo(sTest) == 0)
        return i;
    }
    return -1;
  }

  /**
   *  Description of the Method
   *
   *@param  sGroupComments        Description of the Parameter
   *@param  sNowProcessingGroup   Description of the Parameter
   *@param  dFirstLevelSum        Description of the Parameter
   *@param  dFirstLevelBeforeSum  Description of the Parameter
   *@param  bPrintBefore          Description of the Parameter
   */
  private void PrintOutLevel1Sums(String sGroupComments,
      String sNowProcessingGroup, double dFirstLevelSum, double dFirstLevelBeforeSum,
      boolean bPrintBefore) {
    initializeRow(fieldSize, 9);
    if (bPrintBefore) {
      addField("------------------------", fieldSize, 6,
          -1, true);
      addField("------------------------", fieldSize, 8,
          -1, true);
    }
    addField("------------------------", fieldSize, 7,
        -1, true);
    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);

    initializeRow(fieldSize, 9);
    addField(sGroupComments, fieldSize, 3,
        -1, false);
    addField(sNowProcessingGroup, fieldSize, 4,
        -1, false);
    Boolean bIsMinus = new Boolean(false);
    int row = getSecondLevelGroup(sNowProcessingGroup);
    if (row >= 0 && ((Boolean) columnsSecondLevel[GRP_COL_MINUS].get(row)).
        booleanValue())
      addField(Translator.getTranslation("Minus"), fieldSize, 5,
          -1, false);
    if (bPrintBefore) {
      addField(new Double(dFirstLevelBeforeSum), fieldSize, 6,
          Types.DOUBLE, false);
      addField(new Double(dFirstLevelSum + dFirstLevelBeforeSum), fieldSize, 8,
          Types.DOUBLE, false);
    }
    addField(new Double(dFirstLevelSum), fieldSize, 7,
        Types.DOUBLE, false);

    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);
    jTextArea.append(newline);
  }

  /**
   *  Description of the Method
   *
   *@param  sSecondLevelComments       Description of the Parameter
   *@param  sNowProcessingSecondGroup  Description of the Parameter
   *@param  dSecondLevelSum            Description of the Parameter
   */
  private void PrintOutLevel2Sums(String sSecondLevelComments,
      String sNowProcessingSecondGroup, double dSecondLevelSum) {
    initializeRow(fieldSize, 9);
    addField("------------------------", fieldSize, 8,
        -1, true);
    addField("------------------------", fieldSize, 9,
        -1, true);
    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);

    initializeRow(fieldSize, 9);
    addField(sSecondLevelComments, fieldSize, 3,
        -1, false);
    addField(sNowProcessingSecondGroup, fieldSize, 4,
        -1, false);
    addField(new Double(dSecondLevelSum), fieldSize, 9,
        Types.DOUBLE, false);

    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);
    jTextArea.append(newline);
  }

  /**
   *  Description of the Method
   *
   *@param  db        Description of the Parameter
   *@param  arrayDyn  Description of the Parameter
   */
  public void LoadSumArray(JdbcTable db, ArrayList[] arrayDyn) {
    boolean bNewRecordExists = true;
    //Load up the sum information into the sum array
    do {
      Integer intAccountNum = (Integer) db.getObject("Amount.Account", null);
      Double dAccountSum = (Double) db.getObject("SumOfAmount1", null);
      String sAccountName = (String) db.getObject("FirstAccDesc", null);
      Integer iIsDebit = (Integer) db.getObject("Amount.IsDebit", null);

      bNewRecordExists = db.GetNextRecord();
      if (bNewRecordExists && intAccountNum.compareTo(
          (Integer) db.getObject("Amount.Account", null)) == 0) {
        //now we most likely got the credit info this time, but just
        //in case we will check here. sum= debit - credit
        if (iIsDebit.intValue() == 1)
          dAccountSum = new Double(dAccountSum.doubleValue() -
              ((Double) db.getObject("SumOfAmount1", null)).doubleValue());
        else
          dAccountSum = new Double(
              ((Double) db.getObject("SumOfAmount1", null)).doubleValue()
               - dAccountSum.doubleValue());
        //need to go to the next record which will be used next time
        //we go through this looping
        bNewRecordExists = db.GetNextRecord();
      }
      else
      //There was only a credit or a debit in the account.  We
      //need to reflect that here with a plus or a minus
          if (iIsDebit.intValue() != 1)
        dAccountSum = new Double(-dAccountSum.doubleValue());

      //go through the account list and match up the values if they exist
      for (int i = 0; i < columnsAccount[0].size(); i++) {
        if (((String) columnsAccount[ACC_COL_ACC_NUM_FROM].get(i)).length() == 0)
          continue;
        if (((String) columnsAccount[ACC_COL_ACC_NUM_TO].get(i)).length() == 0)
          continue;
        Integer iTestAccNumFrom = IntegerField.ConvertLocalizedStringToInt(
            (String) columnsAccount[ACC_COL_ACC_NUM_FROM].get(i));
        Integer iTestAccNumTo = IntegerField.ConvertLocalizedStringToInt(
            (String) columnsAccount[ACC_COL_ACC_NUM_TO].get(i));
        if (intAccountNum.compareTo(iTestAccNumFrom) >= 0 &&
            intAccountNum.compareTo(iTestAccNumTo) <= 0) {
          //we found a matching account number
          //we cant break after the find because there might be more
          arrayDyn[ACC_COL_ACC_NUM_FROM].add(intAccountNum);
          arrayDyn[ACC_COL_COMMENT].add(columnsAccount[ACC_COL_COMMENT].get(i));
          arrayDyn[ACC_COL_MINUS].add(columnsAccount[ACC_COL_MINUS].get(i));
          arrayDyn[ACC_COL_BEFORESUM].add(columnsAccount[ACC_COL_BEFORESUM].get(i));
          arrayDyn[ACC_COL_GROUP].add(columnsAccount[ACC_COL_GROUP].get(i));
          arrayDyn[ACC_COL_SUM].add(dAccountSum);
          int nowrow = getSecondLevelGroup((String) columnsAccount[ACC_COL_GROUP].get(i));
          if (nowrow >= 0)
            arrayDyn[ACC_COL_GROUP2].add((String) columnsSecondLevel[GRP_COL_GRP2].get(nowrow));
          else
            arrayDyn[ACC_COL_GROUP2].add(new String("999999"));
          //a flag meaning "no group 2"
          arrayDyn[ACC_COL_ACC_NAME].add(sAccountName);
        }
      }
    } while (bNewRecordExists)
        ;
  }

  /**
   *  Gets the accountSum attribute of the AccountSumReport object
   *
   *@param  arrayDyn        Description of the Parameter
   *@param  iAccountNumber  Description of the Parameter
   *@return                 The accountSum value
   */
  public double getAccountSum(ArrayList[] arrayDyn, Integer iAccountNumber) {
    for (int i = 0; i < arrayDyn[0].size(); i++)
      if (iAccountNumber.compareTo((Integer) arrayDyn[ACC_COL_ACC_NUM_FROM].get(i)) == 0)
        return ((Double) arrayDyn[ACC_COL_SUM].get(i)).doubleValue();
    return 0;
    //couldnt find the account.
  }

  /**
   *  Description of the Field
   */
  public final int fieldSize[] = {0, 8, 27, 15, 5, 7, 17, 17, 17, 17};

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public String buttonGetReport() {
    //
    //first create the main informations arrays
    //
    ArrayList[] arrayDyn = new ArrayList[ACC_COL_GROUP2 + 1];
    for (int i = 0; i <= ACC_COL_GROUP2; i++)
      arrayDyn[i] = new ArrayList();
    if (!IsDateFormatGood())
      return "";

    String sb = new String(
        "SELECT Amount.Account, Sum(Amount.Amount) AS SumOfAmount1, " +
        "Amount.IsDebit, Account.IsAsset AS FirstOfIsAsset, " +
        "Account.AccDesc AS FirstAccDesc " +
        "FROM Amount,Account,Activity WHERE Amount.Act_id=Activity.Act_id " +
        " AND Amount.Account = Account.Account AND " +
        " Account.CompId=Activity.CompId AND Account.CompId=Amount.CompId " +
        " AND Account.CompId=" + cc.comboBox.getSelectedItemsKey().toString() +
        " AND Activity.InvDate >= ? AND Activity.InvDate <= ? " +
        "GROUP BY Amount.Account, Amount.IsDebit " +
        "ORDER BY Amount.Account, Amount.IsDebit DESC ");
    JdbcTable db = new JdbcTable(sb,view);

    try {
      //invoice date
      db.setObject(new java.sql.Date(
          jTextField1.getDate().getTime()),
          Types.DATE);
      db.setObject(new java.sql.Date(
          jTextField2.getDate().getTime()),
          Types.DATE);
    }
    catch (Exception e) {
      return "";
    }

    if (!db.GetFirstRecord())
      return "";

    LoadSumArray(db, arrayDyn);

    //
    // create the before sum arrays
    //
    ArrayList[] arrayDyn2 = new ArrayList[ACC_COL_GROUP2 + 1];
    for (int i = 0; i <= ACC_COL_GROUP2; i++)
      arrayDyn2[i] = new ArrayList();

    String sb2 = new String(
        "SELECT Amount.Account, Sum(Amount.Amount) AS SumOfAmount1, " +
        "Amount.IsDebit, Account.IsAsset AS FirstOfIsAsset, " +
        "Account.AccDesc AS FirstAccDesc " +
        "FROM Amount,Account,Activity WHERE Amount.Act_id=Activity.Act_id " +
        " AND Amount.Account = Account.Account AND " +
        " Account.CompId=Activity.CompId AND Account.CompId=Amount.CompId " +
        " AND Account.CompId=" + cc.comboBox.getSelectedItemsKey().toString() +
        " AND Activity.InvDate < ? " +
        "GROUP BY Amount.Account, Amount.IsDebit " +
        "ORDER BY Amount.Account, Amount.IsDebit DESC ");
    JdbcTable db2 = new JdbcTable(sb2,view);

    try {
      //invoice date
      db2.setObject(new java.sql.Date(
          jTextField1.getDate().getTime()),
          Types.DATE);
    }
    catch (Exception e) {
      return "";
    }

    if (db2.GetFirstRecord())
      LoadSumArray(db2, arrayDyn2);
    //arrayDyn2 might have entries not existing in arrayDyn1.  This would
    //ruin the report.  So we add a Zero entry for any not existing in arrayDyn1.
    for (int i = 0; i < arrayDyn2[0].size(); i++) {
      int j;
      for (j = 0; j < arrayDyn[0].size(); j++)
        if (((Integer) arrayDyn2[ACC_COL_ACC_NUM_FROM].get(i)).compareTo
            ((Integer) arrayDyn[ACC_COL_ACC_NUM_FROM].get(j)) == 0)
          break;
      if (j == arrayDyn[0].size()) {
        //doesnt exist.... Add it.
        arrayDyn[ACC_COL_ACC_NUM_FROM].add(arrayDyn2[ACC_COL_ACC_NUM_FROM].get(i));
        arrayDyn[ACC_COL_COMMENT].add(arrayDyn2[ACC_COL_COMMENT].get(i));
        arrayDyn[ACC_COL_MINUS].add(arrayDyn2[ACC_COL_MINUS].get(i));
        arrayDyn[ACC_COL_BEFORESUM].add(arrayDyn2[ACC_COL_BEFORESUM].get(i));
        arrayDyn[ACC_COL_GROUP].add(arrayDyn2[ACC_COL_GROUP].get(i));
        arrayDyn[ACC_COL_SUM].add(new Double(0));
        //its nothing
        arrayDyn[ACC_COL_GROUP2].add(arrayDyn2[ACC_COL_GROUP2].get(i));
        arrayDyn[ACC_COL_ACC_NAME].add(arrayDyn2[ACC_COL_ACC_NAME].get(i));
      }
    }

    //now we have all the information, loaded up into arrays, just print
    //it out

    //Print the Report header
    AddReportHeaders();

    initializeRow(fieldSize, 9);
    addField(Translator.getTranslation("Account"), fieldSize, 1,
        -1, false);
    addField(Translator.getTranslation("Account name"), fieldSize, 2,
        -1, false);
    addField(Translator.getTranslation("Commentary"), fieldSize, 3,
        -1, false);
    addField(Translator.getTranslation("Group code"), fieldSize, 4,
        -1, false);
    addField(Translator.getTranslation("Minus"), fieldSize, 5,
        -1, false);
    addField(Translator.getTranslation("Incomming"), fieldSize, 6,
        -1, true);
    addField(Translator.getTranslation("Value"), fieldSize, 7,
        -1, true);
    addField(Translator.getTranslation("Outgoing"), fieldSize, 8,
        -1, true);
    addField(Translator.getTranslation("Group total"), fieldSize, 9,
        -1, true);

    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);
    initializeRow(fieldSize, 9);
    String sLotsOfLines = new String("---------------------------------------------------------------");
    addField(sLotsOfLines, fieldSize, 1,
        -1, false);
    addField(sLotsOfLines, fieldSize, 2,
        -1, false);
    addField(sLotsOfLines, fieldSize, 3,
        -1, false);
    addField(sLotsOfLines, fieldSize, 4,
        -1, false);
    addField(sLotsOfLines, fieldSize, 5,
        -1, false);
    addField(sLotsOfLines, fieldSize, 6,
        -1, true);
    addField(sLotsOfLines, fieldSize, 7,
        -1, true);
    addField(sLotsOfLines, fieldSize, 8,
        -1, true);
    addField(sLotsOfLines, fieldSize, 9,
        -1, true);
    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);

    //load up and sort the report definition
    double dFirstLevelBeforeSum = 0;
    double dFirstLevelSum = 0;
    double dSecondLevelSum = 0;
    String sNowProcessingGroup = new String("");
    String sNowProcessingSecondGroup = new String("");
    String sSecondLevelComments = new String("");
    String sGroupComments = new String("");
    int row;
    //first we must sort, the sort order is
    //  second level group (if non existing, then first level)
    //  first level group
    //  account number

    //Ok, here we go, the holy grail of programmers, a sorting routine!!
    //Why have I chosen the worlds worst sorting routine? Its easy and
    //fast enough....   THE BUBBLE SORT (OH NO!!!!)
    for (int x = arrayDyn[0].size() - 1; x > 0; x--)
      for (int y = 0; y < x; y++) {
        int group1y;
        int group2y;
        int accountnumy;
        int group1y1;
        int group2y1;
        int accountnumy1;
        group1y = IntegerField.ConvertLocalizedStringToInt(
            (String) arrayDyn[ACC_COL_GROUP].get(y)).intValue();
        group2y = IntegerField.ConvertLocalizedStringToInt(
            (String) arrayDyn[ACC_COL_GROUP2].get(y)).intValue();
        accountnumy = ((Integer) (arrayDyn[ACC_COL_ACC_NUM_FROM].get(y))).intValue();
        group1y1 = IntegerField.ConvertLocalizedStringToInt(
            (String) arrayDyn[ACC_COL_GROUP].get(y + 1)).intValue();
        group2y1 = IntegerField.ConvertLocalizedStringToInt(
            (String) arrayDyn[ACC_COL_GROUP2].get(y + 1)).intValue();
        accountnumy1 = ((Integer) (arrayDyn[ACC_COL_ACC_NUM_FROM].get(y + 1))).intValue();
        if (group2y == 999999)
          group2y = group1y;
        if (group2y1 == 999999)
          group2y1 = group1y1;
        if (group2y > group2y1
             || (group2y == group2y1 && group1y > group1y1)
             || (group2y == group2y1 && group1y == group1y1 && accountnumy > accountnumy1)) {
          Object tempObj;
          tempObj = arrayDyn[ACC_COL_ACC_NUM_FROM].get(y);
          arrayDyn[ACC_COL_ACC_NUM_FROM].set(y, arrayDyn[ACC_COL_ACC_NUM_FROM].get(y + 1));
          arrayDyn[ACC_COL_ACC_NUM_FROM].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_COMMENT].get(y);
          arrayDyn[ACC_COL_COMMENT].set(y, arrayDyn[ACC_COL_COMMENT].get(y + 1));
          arrayDyn[ACC_COL_COMMENT].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_MINUS].get(y);
          arrayDyn[ACC_COL_MINUS].set(y, arrayDyn[ACC_COL_MINUS].get(y + 1));
          arrayDyn[ACC_COL_MINUS].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_BEFORESUM].get(y);
          arrayDyn[ACC_COL_BEFORESUM].set(y, arrayDyn[ACC_COL_BEFORESUM].get(y + 1));
          arrayDyn[ACC_COL_BEFORESUM].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_GROUP].get(y);
          arrayDyn[ACC_COL_GROUP].set(y, arrayDyn[ACC_COL_GROUP].get(y + 1));
          arrayDyn[ACC_COL_GROUP].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_SUM].get(y);
          arrayDyn[ACC_COL_SUM].set(y, arrayDyn[ACC_COL_SUM].get(y + 1));
          arrayDyn[ACC_COL_SUM].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_GROUP2].get(y);
          arrayDyn[ACC_COL_GROUP2].set(y, arrayDyn[ACC_COL_GROUP2].get(y + 1));
          arrayDyn[ACC_COL_GROUP2].set(y + 1, tempObj);

          tempObj = arrayDyn[ACC_COL_ACC_NAME].get(y);
          arrayDyn[ACC_COL_ACC_NAME].set(y, arrayDyn[ACC_COL_ACC_NAME].get(y + 1));
          arrayDyn[ACC_COL_ACC_NAME].set(y + 1, tempObj);

        }
      }

    //begin writing the report
    int i;
    for (i = 0; i < arrayDyn[0].size(); i++) {
      //if not currently proccessing a group, then maybe we will start one
      //or we have changed groups
      if (sNowProcessingGroup.compareTo(
          (String) arrayDyn[ACC_COL_GROUP].get(i)) != 0) {
        if (sNowProcessingGroup.length() != 0)
          //must print out group sum

          PrintOutLevel1Sums(sGroupComments, sNowProcessingGroup,
              dFirstLevelSum, dFirstLevelBeforeSum, ((Boolean) arrayDyn[ACC_COL_BEFORESUM].
              get(i - 1)).booleanValue());

        else
          //print out a seperating line anyway
          jTextArea.append(newline);

        //check if the second level group has changed as well
        int nowrow = getSecondLevelGroup((String) arrayDyn[ACC_COL_GROUP].
            get(i));
        String sSecondLevTest = new String("");
        if (nowrow >= 0)
          sSecondLevTest = (String) columnsSecondLevel[GRP_COL_GRP2].get(nowrow);
        if (sNowProcessingSecondGroup.compareTo(sSecondLevTest) != 0) {
          //need to sum the Second level groups
          row = getSecondLevelGroup(sNowProcessingGroup);
          if (row >= 0) {
            if (((Boolean) columnsSecondLevel[GRP_COL_MINUS].get(row)).
                booleanValue())
              dSecondLevelSum -= dFirstLevelSum;
            else
              dSecondLevelSum += dFirstLevelSum;
          }
          if (sNowProcessingSecondGroup.length() != 0)
            //must print out group sum

            PrintOutLevel2Sums(sSecondLevelComments,
                sNowProcessingSecondGroup, dSecondLevelSum);

          dSecondLevelSum = 0;
          sNowProcessingSecondGroup = sSecondLevTest;
          sSecondLevelComments = new String("");
          if (nowrow >= 0)
            sSecondLevelComments =
                (String) columnsSecondLevel[GRP_COL_COMMENT].get(nowrow);
        }
        else {

          //need to sum the Second level groups
          row = getSecondLevelGroup(sNowProcessingGroup);
          if (row >= 0) {
            if (((Boolean) columnsSecondLevel[GRP_COL_MINUS].get(row)).
                booleanValue())
              dSecondLevelSum -= dFirstLevelSum;
            else
              dSecondLevelSum += dFirstLevelSum;
          }
        }
        dFirstLevelSum = 0;
        dFirstLevelBeforeSum = 0;
        sNowProcessingGroup = (String) arrayDyn[ACC_COL_GROUP].get(i);
        sGroupComments = (String) arrayDyn[ACC_COL_COMMENT].get(i);
      }
      //need to sum the groups
      double dd = ((Double) arrayDyn[ACC_COL_SUM].
          get(i)).doubleValue();
      if (((Boolean) arrayDyn[ACC_COL_MINUS].
          get(i)).booleanValue())
        dFirstLevelSum -= dd;
      else
        dFirstLevelSum += dd;
      double ddbefore = getAccountSum(arrayDyn2, (Integer) arrayDyn[ACC_COL_ACC_NUM_FROM].get(i));
      dFirstLevelBeforeSum += ddbefore;
      if (dd == 0 && (ddbefore == 0 || !((Boolean) arrayDyn[ACC_COL_BEFORESUM].
          get(i)).booleanValue()))
        //if there is a zero sum on this, then just skip it.
        continue;
      //start printing out the rows
      initializeRow(fieldSize, 9);
      addField(arrayDyn[ACC_COL_ACC_NUM_FROM].get(i), fieldSize, 1,
          Types.INTEGER, false);
      addField(arrayDyn[ACC_COL_ACC_NAME].get(i), fieldSize, 2,
          -1, false);
      //if the group exists and minus field is true, then print minus
      if (((Boolean) arrayDyn[ACC_COL_MINUS].get(i)).booleanValue() == true
           && ((String) arrayDyn[ACC_COL_GROUP].get(i)).length() != 0)
        addField(Translator.getTranslation("Minus"), fieldSize, 5,
            -1, false);
      //      addField(arrayDyn[ACC_COL_GROUP].get(i),fieldSize,4,
      //          -1,false);
      if (((Boolean) arrayDyn[ACC_COL_BEFORESUM].
          get(i)).booleanValue()) {
        addField(new Double(ddbefore), fieldSize, 6,
            Types.DOUBLE, false);
        addField(new Double(dd + ddbefore), fieldSize, 8,
            Types.DOUBLE, false);
      }
      addField(arrayDyn[ACC_COL_SUM].get(i), fieldSize, 7,
          Types.DOUBLE, false);

      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);

    }
    //need to print out the final sums
    if (sNowProcessingGroup.length() != 0) {
      //must print out group sum

      PrintOutLevel1Sums(sGroupComments, sNowProcessingGroup,
          dFirstLevelSum, dFirstLevelBeforeSum, ((Boolean) arrayDyn[ACC_COL_BEFORESUM].
          get(i - 1)).booleanValue());
      row = getSecondLevelGroup(sNowProcessingGroup);
      String sNextProcessingSecondGroup = new String("");
      if (row >= 0)
        sNextProcessingSecondGroup =
            (String) columnsSecondLevel[GRP_COL_GRP2].get(row);

      if (sNowProcessingSecondGroup.length() != 0
           || sNextProcessingSecondGroup.length() != 0) {
        if (sNowProcessingSecondGroup.length() == 0) {
          sNowProcessingSecondGroup =
              (String) columnsSecondLevel[GRP_COL_GRP2].get(row);
          sSecondLevelComments =
              (String) columnsSecondLevel[GRP_COL_COMMENT].get(row);
        }
        //need to sum the Second level groups
        if (row >= 0 && ((Boolean) columnsSecondLevel[GRP_COL_MINUS].get(row)).
            booleanValue())
          dSecondLevelSum -= dFirstLevelSum;
        else
          dSecondLevelSum += dFirstLevelSum;

        PrintOutLevel2Sums(sSecondLevelComments,
            sNextProcessingSecondGroup, dSecondLevelSum);
      }
    }

    return jTextField3.getText();
  }

}


