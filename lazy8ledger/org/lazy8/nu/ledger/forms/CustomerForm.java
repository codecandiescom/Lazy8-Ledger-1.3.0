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

public class CustomerForm extends DataExchangeForm {
  public CustomerForm(boolean bIsAddOnly,String sAccountNum,JDialog parent,JFrame view) {
    super("customer", view,"lazy8ledger-customer");
    this.parent=parent;
    initialize(bIsAddOnly, sAccountNum);
  }
  public CustomerForm(JFrame view) {
    super("customer",view,"lazy8ledger-customer");
    initialize(false,"");
  }
  private void initialize(boolean bIsAddOnly,String sAccountNum) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      Exit();
      return ;
    }
    /*
    super(desktop, Translator.getTranslation("Customer"),
        true,
    //resizable
        true,
    //closable
        true,
    //maximizable
        true,
    //iconifiable
        "customer",
        false
        );
        */
    this.bIsAddOnly=bIsAddOnly;
    wasWriteOk=false;
    jPanel1 = new JPanel();

    label1 = new HelpedLabel(Translator.getTranslation("CustId"),
        "Number", "customer",view);
    textField1 = new IntegerField("Number", "customer",view);
    textField1.setText(sAccountNum);
    label2 = new HelpedLabel(Translator.getTranslation("Customer name"),
        "Name", "customer",view);
    textField2 = new HelpedTextField("Name", "customer",view);
    label3 = new HelpedLabel(Translator.getTranslation("Customer description"),
        "Description", "customer",view);
    textField3 = new HelpedTextField("Description", "customer",view);
    JPanel jPanel22=new JPanel();
    jPanel22.setLayout(new BoxLayout(jPanel22, BoxLayout.Y_AXIS));

    jPanel1.setLayout(new GridLayout(4, 2));

    cc = new CompanyComponents(jPanel1, Translator.getTranslation("Company"),
        false, "customer",view);

    jPanel22.add(jPanel1);

    jPanel1.add(label1);
    jPanel1.add(textField1);

    jPanel1.add(label2);
    jPanel1.add(textField2);

    jPanel1.add(label3);
    jPanel1.add(textField3);

    customerAccess = new JdbcTable("Customer", 2,view);
    dataMovementPane = new DataMovementPane(
        customerAccess, this,bIsAddOnly,view);

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
    //pack();
  }
  public void Exit(){
    wasWriteOk=false;
    if(parent!=null && bIsAddOnly)
      parent.hide();
    else
      super.Exit();
  }
  public void AfterGoodWrite(){
    wasWriteOk=true;
    if(parent!=null && bIsAddOnly)
      parent.hide();
  }
  public String customerNum(){
    return addedCustomerNum;
  }
  public boolean isOk(){
    return wasWriteOk;
  }
  public boolean IsWriteOK() {
    if (((Integer) cc.comboBox.getSelectedItemsKey()).intValue() == 0
         || textField1.getInteger().intValue() == 0) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Update not entered"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    try {
      DataConnection dc=DataConnection.getInstance(view);
      if(dc==null || !dc.bIsConnectionMade)return false;
      PreparedStatement stmt = dc.con.prepareStatement(
          "SELECT * FROM Customer where CompId=? and CustId=?");
      stmt.setInt(1, ((Integer) cc.comboBox.getSelectedItemsKey()).intValue());
      stmt.setInt(2, textField1.getInteger().intValue());
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
    addedCustomerNum=textField1.getText();
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
      if(dc==null || !dc.bIsConnectionMade)return false;
      PreparedStatement stmt = dc.con.prepareStatement(
          "SELECT * FROM Activity where CompId=? and Customer=?");
      stmt.setInt(1, ((Integer) cc.comboBox.getSelectedItemsKey()).intValue());
      stmt.setInt(2, textField1.getInteger().intValue());
      ResultSet rs = stmt.executeQuery();
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
    if (bEnabled)
      cc.comboBox.setEnabled(!bEnabled);
    else
      cc.comboBox.setEnabled(SetupInfo.getBoolProperty(
          SetupInfo.SHOW_ADVANCED_MENUS));
  }

  /**
   *  Gets the company attribute of the CustomerForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getCompany(int i) {
    customerAccess.setObject(cc.comboBox.getSelectedItemsKey(),
        "CompId");
  }

  /**
   *  Gets the customerId attribute of the CustomerForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getCustomerId(int i) {
    customerAccess.setObject(textField1.getInteger(), "CustId");
  }

  /**
   *  Gets the wholeList attribute of the CustomerForm object
   *
   *@param  i  Description of the Parameter
   */
  public void getWholeList(int i) {
    getCustomerId(i++);
    getCompany(i++);
    customerAccess.setObject(textField2.getText(), "CustName");
    customerAccess.setObject(textField3.getText(), "CustDesc");
  }

  /**
   *  Gets the fields attribute of the CustomerForm object
   *
   *@param  WhyGet  Description of the Parameter
   */
  public void getFields(int WhyGet) {
    switch (WhyGet) {
      case DataMovementPane.FIRST:
        getCompany(1);
        break;
      case DataMovementPane.CHANGE:
      case DataMovementPane.ADD:
        getWholeList(1);
        break;
      case DataMovementPane.DELETE:
        getCustomerId(1);
        getCompany(2);
        break;
    }
  }

  /**
   *  Description of the Method
   */
  public void putFields() {

    textField1.setInteger((Integer)
        customerAccess.getObject("CustId", null));
    Integer key = (Integer)
        customerAccess.getObject("CompId", null);
    //    if(key.intValue()==0)
    cc.comboBox.setSelectedItemFromKey(key);
    textField2.setText((String)
        customerAccess.getObject("CustName", null));
    textField3.setText((String)
        customerAccess.getObject("CustDesc", null));
  }

  /**
   *  Description of the Method
   */
  public void clearFields() {
    textField1.setText("");
    textField2.setText("");
    textField3.setText("");
  }

  JDialog parent;
  private boolean bIsAddOnly;
  private String addedCustomerNum;
  private boolean wasWriteOk;
  private JPanel jPanel1;
  private JPanel jPanel2;
  private JLabel label1;
  private IntegerField textField1;
  private JLabel label2;
  private JTextField textField2;
  private JLabel label3;
  private JTextField textField3;
  private JdbcTable customerAccess;
  CompanyComponents cc;
}


