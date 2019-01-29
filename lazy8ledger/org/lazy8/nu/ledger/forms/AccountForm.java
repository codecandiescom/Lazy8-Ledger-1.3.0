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
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.ledger.main.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.jedit.msg.*;
/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class AccountForm extends DataExchangeForm {
  /**
   *  Constructor for the AccountForm object
   *
   *@param  helpViewer  Description of the Parameter
   *@param  desktop     Description of the Parameter
   */
  ;
  public class CenteredInPanel extends JPanel {
    /**
     *  Constructor for the CenteredInPanel object
     *
     *@param  inCenter  Description of the Parameter
     */
    public CenteredInPanel(JComponent inCenter) {
      JPanel horizontalSpacePanel = new JPanel();
      horizontalSpacePanel.setLayout(new BoxLayout(horizontalSpacePanel, BoxLayout.X_AXIS));
      horizontalSpacePanel.add(Box.createHorizontalGlue());
      horizontalSpacePanel.add(inCenter);
      horizontalSpacePanel.add(Box.createHorizontalGlue());

      setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
      add(Box.createVerticalGlue());
      add(horizontalSpacePanel);
      add(Box.createVerticalGlue());
    }
  }

  public AccountForm(boolean bIsAddOnly,String sAccountNum,JDialog parent,JFrame view) {
    super("account",view,"lazy8ledger-account");
    this.parent=parent;
    initialize(bIsAddOnly, sAccountNum);
  }
  public AccountForm(JFrame view) {
    super("account",view,"lazy8ledger-account");
    initialize(false,"");
  }
  private void initialize(boolean bIsAddOnly,String sAccountNum) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      Exit();
      return ;
    }
   /* super(desktop, Translator.getTranslation("Account"),
        true,
    //resizable
        true,
    //closable
        true,
    //maximizable
        true,
    //iconifiable
        "account",
        false
        );
        */
    this.bIsAddOnly=bIsAddOnly;
    wasWriteOk=false;
    jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(4, 2));
    jPanel2 = new JPanel();
    jPanel2.setLayout(new BoxLayout(jPanel2, BoxLayout.Y_AXIS));

    jPanel2.add(jPanel1);
    if(!bIsAddOnly){
      JPanel horizontalSpacePanel = new JPanel();
      horizontalSpacePanel.setLayout(new BoxLayout(horizontalSpacePanel, BoxLayout.X_AXIS));
      horizontalSpacePanel.add(Box.createHorizontalGlue());
      horizontalSpacePanel.add(jPanel2);
      horizontalSpacePanel.add(Box.createHorizontalGlue());

      setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
      add(Box.createVerticalGlue());
      add(horizontalSpacePanel);
      add(Box.createVerticalGlue());
    //getContentPane().add(jPanel2);
    }else{
      add(jPanel2);
    }

    cc = new CompanyComponents(jPanel1,
        Translator.getTranslation("Company"), false, "account",view);

    //    JLabel emptySpace=new JLabel();
    //    jPanel1.add(emptySpace);

    label1 = new HelpedLabel(Translator.getTranslation("Account number"),
        "number", "account",view);
    textField1 = new IntegerField("number", "account",view);
    textField1.setText(sAccountNum);
    label2 = new HelpedLabel(Translator.getTranslation("Account name"),
        "name", "account",view);
    textField2 = new HelpedTextField("name", "account",view);

    jPanel1.add(label1);
    jPanel1.add(textField1);

    jPanel1.add(label2);
    jPanel1.add(textField2);

    jCheckBox = new HelpedCheckBox(
        Translator.getTranslation("IsAsset"), false, "asset", "account",view);
    jCheckBox.setSelected(false);
    jPanel1.add(jCheckBox);

    accountAccess = new JdbcTable("Account", 2,view);
    dataMovementPane = new DataMovementPane(
        accountAccess, this,bIsAddOnly,view);

    jPanel2.add(dataMovementPane);

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
  public String accountNum(){
    return addedAccountNum;
  }
  public boolean isOk(){
    return wasWriteOk;
  }
  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
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
          "SELECT * FROM Account where CompId=? and Account=?");
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
    addedAccountNum=textField1.getText();
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
          "SELECT * FROM Amount where CompId=? and Account=?");
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
   *  Gets the company attribute of the AccountForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getCompany(int i) {
    accountAccess.setObject(cc.comboBox.getSelectedItemsKey(),
        "CompId");
  }

  /**
   *  Gets the account attribute of the AccountForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getAccount(int i) {
    accountAccess.setObject(textField1.getInteger(),
        "Account");
  }

  /**
   *  Gets the wholeList attribute of the AccountForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getWholeList(int i) {
    getAccount(i++);
    getCompany(i++);
    accountAccess.setObject(textField2.getText(),
        "AccDesc");
    int iBoolToInt = 0;
    if (jCheckBox.isSelected())
      iBoolToInt = 1;
    accountAccess.setObject(new Integer(iBoolToInt),
        "IsAsset");
  }

  /**
   *  Gets the fields attribute of the AccountForm object
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
        getAccount(1);
        getCompany(2);
        break;
    }
  }

  /**
   *  Description of the Method
   */
  public void putFields() {
    textField1.setInteger((Integer)
        accountAccess.getObject("Account", null));

    Integer key = (Integer)
        accountAccess.getObject("CompId", null);
    //    if(key.intValue()==0)
    cc.comboBox.setSelectedItemFromKey(key);
    textField2.setText((String)
        accountAccess.getObject("AccDesc", null));
    boolean bBoolToInt = false;
    if (((Integer)
        accountAccess.getObject("IsAsset", null)).intValue() == 1)
      bBoolToInt = true;
    jCheckBox.setSelected(bBoolToInt);

  }

  /**
   *  Description of the Method
   */
  public void clearFields() {
    textField1.setText("");
    textField2.setText("");
    jCheckBox.setSelected(false);

  }
  JDialog parent;
  private boolean bIsAddOnly;
  private String addedAccountNum;
  private boolean wasWriteOk;
  private JPanel jPanel1;
  private JPanel jPanel2;
  private JLabel label1;
  private IntegerField textField1;
  private JLabel label2;
  private JLabel label3;
  private JLabel label4;
  private HelpedTextField textField2;
  private JCheckBox jCheckBox;
  private JdbcTable accountAccess;
  private CompanyComponents cc;
}


