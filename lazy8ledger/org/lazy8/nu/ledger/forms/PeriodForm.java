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

import javax.swing.*;
import java.awt.*;
import java.text.*;
import java.sql.*;
import java.awt.event.*;
import java.util.Calendar;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class PeriodForm extends DataExchangeForm {
  public PeriodForm(JFrame view) {
    super("periodform",view,"lazy8ledger-accountingperiods");

    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      Exit();
      return ;
    }
    
    jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(4, 2));
    jPanel2 = new JPanel();
    jPanel2.setLayout(new GridLayout(2, 1));

    jPanel2.add(jPanel1);

    cc = new CompanyComponents(jPanel1,
        Translator.getTranslation("Company"), false, "periodform",view);

    //    JLabel emptySpace=new JLabel();
    //    jPanel1.add(emptySpace);

    label1 = new HelpedLabel(Translator.getTranslation("Start period"),
        "From", "periodform",view);
    textField1 = new DateField("From", "periodform",view);
    label2 = new HelpedLabel(Translator.getTranslation("End Period"),
        "To", "periodform",view);
    textField2 = new DateField("To", "periodform",view);

    jPanel1.add(label1);
    jPanel1.add(textField1);

    jPanel1.add(label2);
    jPanel1.add(textField2);

    accountAccess = new JdbcTable("AccountingPeriods", 3,view);
    dataMovementPane = new DataMovementPane(
        accountAccess, this,view);

    jPanel2.add(dataMovementPane);
      JPanel horizontalSpacePanel = new JPanel();
      horizontalSpacePanel.setLayout(new BoxLayout(horizontalSpacePanel, BoxLayout.X_AXIS));
      horizontalSpacePanel.add(Box.createHorizontalGlue());
      horizontalSpacePanel.add(jPanel2);
      horizontalSpacePanel.add(Box.createHorizontalGlue());

      setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
      add(Box.createVerticalGlue());
      add(horizontalSpacePanel);
      add(Box.createVerticalGlue());

    //pack();
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsWriteOK() {
    if (((Integer) cc.comboBox.getSelectedItemsKey()).intValue() == 0) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Update not entered"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    try {
      textField1.getDate();
      textField2.getDate();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Date must be in the following format") +
          " : " +
          DateField.getTodaysDateString(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    try {
      if (textField1.getDate().compareTo(textField2.getDate()) >= 0) {
        JOptionPane.showMessageDialog(this,
            Translator.getTranslation("The StartPeriod must be less than the EndPeroid"),
            Translator.getTranslation("Update not entered"),
            JOptionPane.PLAIN_MESSAGE);
        return false;
      }
    }
    catch (Exception e) {
    }
    try {
      DataConnection dc=DataConnection.getInstance(view);
      if(dc==null || !dc.bIsConnectionMade)return false ;
      PreparedStatement stmt = dc.con.prepareStatement(
          "SELECT * FROM AccountingPeriods where CompId=? and StartPeriod=?  and EndPeriod=?");
      stmt.setInt(1, ((Integer) cc.comboBox.getSelectedItemsKey()).intValue());
      stmt.setDate(2, new java.sql.Date(textField1.getTodaysDate().getTime()));
      stmt.setDate(3, new java.sql.Date(textField2.getTodaysDate().getTime()));
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        JOptionPane.showMessageDialog(this,
            Translator.getTranslation("This number already exists"),
            Translator.getTranslation("Update not entered"),
            JOptionPane.PLAIN_MESSAGE);
        return false;
      }
      stmt.close();
      rs.close();
    }
    catch (Exception ex) {
    }
    return true;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsChangeOK() {
    return IsWriteOK();
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsDeleteOK() {
    return super.IsDeleteOK();
  }

  /**
   *  Gets the company attribute of the PeriodForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getCompany(int i) {
    accountAccess.setObject(cc.comboBox.getSelectedItemsKey(),
        "CompId");
  }

  /**
   *  Gets the accountPeriods attribute of the PeriodForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getAccountPeriods(int i) {
    try {
      accountAccess.setObject(new java.sql.Date(textField1.getDate().getTime()),
          "StartPeriod");
      accountAccess.setObject(new java.sql.Date(textField2.getDate().getTime()),
          "EndPeriod");
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Date must be in the following format") +
          " : " + DateField.getTodaysDateString(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
    }

  }

  /**
   *  Description of the Method
   *
   *@param  bEnabled  Description of the Parameter
   */
  public void ChangeButtonEnabled(boolean bEnabled) {
    //changes are not allowed!!!!!!!!
    label1.setEnabled(!bEnabled);
    textField1.setEnabled(!bEnabled);
    label2.setEnabled(!bEnabled);
    textField2.setEnabled(!bEnabled);
    if (bEnabled)
      cc.comboBox.setEnabled(!bEnabled);
    else
      cc.comboBox.setEnabled(SetupInfo.getBoolProperty(
          SetupInfo.SHOW_ADVANCED_MENUS));
  }

  /**
   *  Gets the wholeList attribute of the PeriodForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getWholeList(int i) {
    getAccountPeriods(i++);
    getCompany(i++);
  }

  /**
   *  Gets the fields attribute of the PeriodForm object
   *
   *@param  WhyGet  Description of the Parameter
   */
  public void getFields(int WhyGet) {
    switch (WhyGet) {
      case DataMovementPane.FIRST:
        getCompany(1);
        break;
      case DataMovementPane.CHANGE:
        getWholeList(1);
        break;
      //    case DataMovementPane.SEEK:

      case DataMovementPane.ADD:
        getWholeList(1);
        break;
      case DataMovementPane.DELETE:
        getWholeList(1);
        break;
    }
  }

  /**
   *  Description of the Method
   */
  public void putFields() {
    textField1.setDate((Date)
        accountAccess.getObject("StartPeriod", null));
    textField2.setDate((Date)
        accountAccess.getObject("EndPeriod", null));

    Integer key = (Integer)
        accountAccess.getObject("CompId", null);
    //    if(key.intValue()==0)
    cc.comboBox.setSelectedItemFromKey(key);
  }

  /**
   *  Description of the Method
   */
  public void clearFields() {
    /*
     *  Calendar StartYear=Calendar.getInstance();
     *  Calendar EndYear=Calendar.getInstance();
     *  StartYear.set(StartYear.get(Calendar.YEAR),Calendar.JANUARY,1);
     *  EndYear.set(StartYear.get(Calendar.YEAR),Calendar.DECEMBER,31);
     *  textField1.setDate(StartYear.getTime());
     *  textField2.setDate(EndYear.getTime());
     */
    textField1.setText("");
    textField2.setText("");
  }

  private JPanel jPanel1;
  private JPanel jPanel2;
  private JLabel label1;
  private JLabel label2;
  private JLabel label3;
  private JLabel label4;
  private DateField textField1;
  private DateField textField2;
  private JdbcTable accountAccess;
  private CompanyComponents cc;
}


