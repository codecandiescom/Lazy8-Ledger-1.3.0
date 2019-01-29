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
import java.sql.*;
import java.awt.event.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class CompanyForm extends DataExchangeForm {
  public CompanyForm(JFrame view) {
    super("company",view,"lazy8ledger-company");
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      Exit();
      return ;
    }
    /*
    super(desktop, Translator.getTranslation("Company"),
        true,
    //resizable
        true,
    //closable
        true,
    //maximizable
        true,
    //iconifiable
        "company",
        true
        );
*/

    jPanel1 = new JPanel();

    label1 = new HelpedLabel(Translator.getTranslation("CompId"),
        "Number", "company",view);
    textField1 = new IntegerField("Number", "company",view);
    label2 = new HelpedLabel(Translator.getTranslation("Company name"),
        "Name", "company",view);
    textField2 = new HelpedTextField("Name", "company",view);
    JPanel jPanel22=new JPanel();
    jPanel22.setLayout(new BoxLayout(jPanel22, BoxLayout.Y_AXIS));

    jPanel1.setLayout(new GridLayout(2, 2));

    jPanel22.add(jPanel1);

    jPanel1.add(label1);
    jPanel1.add(textField1);

    jPanel1.add(label2);
    jPanel1.add(textField2);

    CompanyAccess = new JdbcTable("Company", 1,view);
    dataMovementPane = new DataMovementPane(
        CompanyAccess, this,view);

    jPanel22.add(dataMovementPane);

      JPanel horizontalSpacePanel = new JPanel();
      horizontalSpacePanel.setLayout(new BoxLayout(horizontalSpacePanel, BoxLayout.X_AXIS));
      horizontalSpacePanel.add(Box.createHorizontalGlue());
      horizontalSpacePanel.add(jPanel22);
      horizontalSpacePanel.add(Box.createHorizontalGlue());

      setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
      add(Box.createVerticalGlue());
      add(horizontalSpacePanel);
      add(Box.createVerticalGlue());
//    pack();
  }

  /**
   *  Description of the Method
   *
   *@param  iCompId    Description of the Parameter
   *@param  sCompName  Description of the Parameter
   */
  public static void quickAndDirtyAddCompany(int iCompId, String sCompName) {
    try {
      DataConnection dc=DataConnection.getInstance(null);
      if(dc==null || !dc.bIsConnectionMade)return ;
      PreparedStatement stmt = dc.con.prepareStatement(
          "INSERT INTO Company (CompId,Name) VALUES(?,?)");
      stmt.setInt(1, iCompId);
      stmt.setBytes(2, StringBinaryConverter.StringToBinary(sCompName));
      stmt.executeUpdate();
      stmt.close();
    }
    catch (Exception ex) {
      System.err.println("Couldnt add company " + iCompId + ":" + sCompName + ":" + ex);
    }
  }

  /**
   *  Description of the Method
   *
   *@param  iCompId    Description of the Parameter
   *@param  sCompName  Description of the Parameter
   */
  public static void quickAndDirtyChangeCompany(int iCompId, String sCompName) {
    try {
      DataConnection dc=DataConnection.getInstance(null);
      if(dc==null || !dc.bIsConnectionMade)return ;
      PreparedStatement stmt = dc.con.prepareStatement(
          "UPDATE Company set Name=? where CompId=?");
      stmt.setString(1, sCompName);
      stmt.setInt(2, iCompId);
      stmt.executeUpdate();
      stmt.close();
    }
    catch (Exception ex) {
      System.err.println("Couldnt change company " + iCompId + ":" + sCompName + ":" + ex);
    }
  }

  /**
   *  Description of the Method
   *
   *@param  intToTest  Description of the Parameter
   *@return            Description of the Return Value
   */
  public static boolean doesNumberExist(int intToTest) {
    boolean returnValue = false;
    try {
      DataConnection dc=DataConnection.getInstance(null);
      if(dc==null || !dc.bIsConnectionMade)return false;
      PreparedStatement stmt = dc.con.prepareStatement(
          "SELECT * FROM Company where CompId=?");
      stmt.setInt(1, intToTest);
      ResultSet rs = stmt.executeQuery();
      if (rs.next())
        returnValue = true;
      stmt.close();
      rs.close();
    }
    catch (Exception ex) {
    }
    return returnValue;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsWriteOK() {
    if (textField1.getInteger().intValue() == 0) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Update not entered"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    if (doesNumberExist(textField1.getInteger().intValue())) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("This number already exists"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    return true;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsDeleteOK() {
    try {
      DataConnection dc=DataConnection.getInstance(view);
      if(dc==null || !dc.bIsConnectionMade)return false ;
      PreparedStatement stmt = dc.con.prepareStatement(
          "SELECT * FROM Activity where CompId=?");
      stmt.setInt(1, textField1.getInteger().intValue());
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        CannotUpdateMessage();
        return false;
      }
      stmt.close();
      rs.close();
      stmt = dc.con.prepareStatement(
          "SELECT * FROM Account where CompId=?");
      stmt.setInt(1, textField1.getInteger().intValue());
      rs = stmt.executeQuery();
      if (rs.next()) {
        CannotUpdateMessage();
        return false;
      }
      stmt.close();
      rs.close();
      stmt = dc.con.prepareStatement(
          "SELECT * FROM Customer where CompId=?");
      stmt.setInt(1, textField1.getInteger().intValue());
      rs = stmt.executeQuery();
      if (rs.next()) {
        CannotUpdateMessage();
        return false;
      }
      stmt.close();
      rs.close();
    }
    catch (Exception ex) {
    }
    return super.IsDeleteOK();
  }

  /**
   *  Description of the Method
   *
   *@param  bEnabled  Description of the Parameter
   */
  public void ChangeButtonEnabled(boolean bEnabled) {
    label1.setEnabled(!bEnabled);
    textField1.setEnabled(!bEnabled);
  }

  /**
   *  Gets the companyId attribute of the CompanyForm object
   *
   *@param  i  Description of the Parameter
   */
  public void getCompanyId(int i) {
    CompanyAccess.setObject(textField1.getInteger(), "CompId");
  }

  /**
   *  Gets the wholeList attribute of the CompanyForm object
   *
   *@param  i  Description of the Parameter
   */
  public void getWholeList(int i) {
    getCompanyId(i++);
    CompanyAccess.setObject(textField2.getText(), "Name");
  }

  /**
   *  Gets the fields attribute of the CompanyForm object
   *
   *@param  WhyGet  Description of the Parameter
   */
  public void getFields(int WhyGet) {
    switch (WhyGet) {
      case DataMovementPane.FIRST:
        break;
      case DataMovementPane.CHANGE:
      case DataMovementPane.ADD:
        getWholeList(1);
        break;
      case DataMovementPane.DELETE:
        getCompanyId(1);
        break;
    }
  }

  /**
   *  Description of the Method
   */
  public void putFields() {
    textField1.setInteger((Integer)
        CompanyAccess.getObject("CompId", null));
    textField2.setText((String)
        CompanyAccess.getObject("Name", null));
  }

  /**
   *  Description of the Method
   */
  public void clearFields() {
    textField1.setText("");
    textField2.setText("");
  }

  private JPanel jPanel1;
  private JPanel jPanel2;
  private JLabel label1;
  private IntegerField textField1;
  private JLabel label2;
  private JTextField textField2;
  private JdbcTable CompanyAccess;
}

