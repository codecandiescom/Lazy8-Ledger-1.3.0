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
package org.lazy8.nu.ledger.forms;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.ledger.main.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class FindRecordDialog extends JDialog {
  /**
   *  Description of the Field
   */
  public JComboBox[] jComboBoxes;
  /**
   *  Description of the Field
   */
  public JTextField[] jTextFields;
  /**
   *  Description of the Field
   */
  public JComboBox[] jFieldNames;
  /**
   *  Description of the Field
   */
  public String[] sFieldNames;
  /**
   *  Description of the Field
   */
  public String[] sUntranslatedFieldNames;
  /**
   *  Description of the Field
   */
  public int iActions[];
  /**
   *  Description of the Field
   */
  public String sValues[];
  /**
   *  Description of the Field
   */
  public boolean returnValue = false;
  boolean bShowAdvanced = false;
  /**
   *  Description of the Field
   */
  public final static int NUM_SEARCH_ROWS = 5;
  JDesktopPane desktop;
  JFrame view;

  /**
   *  Constructor for the FindRecordDialog object
   *
   *@param  frame               Description of the Parameter
   *@param  dataAccessin        Description of the Parameter
   *@param  bShowCompanyAlways  Description of the Parameter
   *@param  desktop             Description of the Parameter
   */
  public FindRecordDialog(JDesktopPane desktopin, javax.swing.JFrame frame, JdbcTable dataAccessin,
      boolean bShowCompanyAlways) {
    super(frame, Translator.getTranslation("Find"), true);
    desktop = desktopin;
    this.view=view;
    if (SetupInfo.getProperty(SetupInfo.SHOW_ADVANCED_MENUS).
        compareTo("true") == 0 || bShowCompanyAlways == true)
      bShowAdvanced = true;

    //count the number of variables in the screen
    //there will be at least one field, so do it
    ResultSetMetaData rsmd;
    try {
      rsmd = dataAccessin.getTablesMetaData();
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
      return;
    }
    jComboBoxes = new JComboBox[NUM_SEARCH_ROWS];
    jTextFields = new JTextField[NUM_SEARCH_ROWS];
    jFieldNames = new JComboBox[NUM_SEARCH_ROWS];
    int i;
    for (i = 0; i < NUM_SEARCH_ROWS; i++)
      AddOneSeekItem(i, rsmd);

    getContentPane().setLayout(new GridLayout(NUM_SEARCH_ROWS + 1, 1));

    JPanel jPanel1 = new JPanel();
    getContentPane().add(jPanel1);

    jPanel1.setLayout(new GridLayout(1, 3));

    JButton button1 = new JButton(Translator.getTranslation("OK"));

    button1.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonSaveData(evt);
        }
      }
        );

    jPanel1.add(button1);

    JButton button2 = new JButton(Translator.getTranslation("Cancel"));

    button2.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          button1ActionPerformed(evt);
        }
      }
        );
    jPanel1.add(button2);

    JButton button3 = new JButton(Translator.getTranslation("Help"));

    button3.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          button3ActionPerformed(evt);
        }
      }
        );
    jPanel1.add(button3);

    pack();
    setLocationRelativeTo(frame);
    setVisible(true);

  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  public void buttonSaveData(java.awt.event.ActionEvent evt) {
    iActions = new int[NUM_SEARCH_ROWS];
    sValues = new String[NUM_SEARCH_ROWS];
    sFieldNames = new String[NUM_SEARCH_ROWS];
    for (int i = 0; i < NUM_SEARCH_ROWS; i++) {
      iActions[i] = jComboBoxes[i].getSelectedIndex();
      sValues[i] = jTextFields[i].getText();
      if (jFieldNames[i].getSelectedIndex() > 0)
        sFieldNames[i] = sUntranslatedFieldNames[
            jFieldNames[i].getSelectedIndex() - 1];
    }
    returnValue = true;
    dispose();
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  public void button1ActionPerformed(java.awt.event.ActionEvent evt) {
    returnValue = false;
    dispose();

  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  public void button3ActionPerformed(java.awt.event.ActionEvent evt) {
    Lazy8Ledger.ShowHelp(view, "find" ,"");
  }

  /**
   *  Description of the Method
   *
   *@param  iMyNumber  Description of the Parameter
   *@param  rsmd       Description of the Parameter
   */
  public void AddOneSeekItem(int iMyNumber, ResultSetMetaData rsmd) {
    boolean isCompIdExists = false;
    JPanel jPanelmain = new JPanel();
    jPanelmain.setLayout(new GridLayout(1, 3));
    jFieldNames[iMyNumber] = new JComboBox();
    jPanelmain.add(jFieldNames[iMyNumber]);
    int colCount;
    int colType;
    int rowCounter = 0;

    String colName;
    try {
      colCount = rsmd.getColumnCount();
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
      return;
    }
    jFieldNames[iMyNumber].addItem("");
    if (iMyNumber == 0)
      sUntranslatedFieldNames = new String[colCount];
    for (int i = 1; i <= colCount; i++) {
      try {
        colType = rsmd.getColumnType(i);
        colName = rsmd.getColumnName(i);
      }
      catch (Exception e) {
        SystemLog.ProblemPrintln("Error:" + e.getMessage());
        return;
      }
      //this does not work on this mysql...        if(rsmd.isSearchable(i))
      //use this next if instead, searching cannot be done on blobs!!!
      if (colType == Types.VARCHAR
           || colType == Types.INTEGER
           || colType == Types.DOUBLE
           || colType == Types.DATE) {
        jFieldNames[iMyNumber].addItem(
            Translator.getTranslation(colName));
        if (colName.compareTo("CompId") == 0)
          isCompIdExists = true;
        if (iMyNumber == 0)
          sUntranslatedFieldNames[rowCounter++] = colName;
      }
    }
    jComboBoxes[iMyNumber] = new JComboBox();
    jComboBoxes[iMyNumber].addItem("");
    jComboBoxes[iMyNumber].addItem(
        Translator.getTranslation("Greater than or equal to"));
    jComboBoxes[iMyNumber].addItem(
        Translator.getTranslation("Less than or equal to"));
    jComboBoxes[iMyNumber].addItem(
        Translator.getTranslation("equal to"));
    jPanelmain.add(jComboBoxes[iMyNumber]);
    jTextFields[iMyNumber] = new JTextField();
    jPanelmain.add(jTextFields[iMyNumber]);
    getContentPane().add(jPanelmain);
    if (bShowAdvanced == false && isCompIdExists) {
      //select the default company
      jTextFields[0].setText(
          SetupInfo.getProperty(SetupInfo.DEFAULT_COMPANY));
      jTextFields[0].setEnabled(false);
      jFieldNames[0].setEnabled(false);
      jFieldNames[0].setSelectedItem(
          Translator.getTranslation("CompId"));
      jComboBoxes[0].setSelectedItem(
          Translator.getTranslation("equal to"));
      jComboBoxes[0].setEnabled(false);
    }
  }
}


