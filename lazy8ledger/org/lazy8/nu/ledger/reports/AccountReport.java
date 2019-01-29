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

import javax.swing.*;
import java.text.*;
import java.sql.*;
import java.awt.*;
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
public class AccountReport extends TextReport {

  DataComboBox accountComboBox;
  DataComboBox customerComboBox;

  /**
   *  Constructor for the AccountReport object
   *
   *@param  helpViewerin  Description of the Parameter
   *@param  desktop       Description of the Parameter
   *@param  parent        Description of the Parameter
   */
  public AccountReport(View view ) {
    super(view, Translator.getTranslation("Account summary"),"accountresult","lazy8ledger-accountsummary");

    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      buttonExit();
      return ;
    }
    JPanel jPanel1 = new JPanel();

    jPanel1.setLayout(new GridLayout(9, 2));

    JLabel label1 = new HelpedLabel(Translator.getTranslation("Account"),
        "Account", "accountresult",view);
    jPanel1.add(label1);
    accountComboBox = new DataComboBox(new JdbcTable("Account", 2,view), true,
        "Account", "accountresult",view);
    jPanel1.add(accountComboBox);

    JLabel label2 = new HelpedLabel(Translator.getTranslation("Customer"),
        "Customer", "accountresult",view);
    jPanel1.add(label2);
    customerComboBox = new DataComboBox(new JdbcTable("Customer", 2,view), true,
        "Customer", "accountresult",view);
    jPanel1.add(customerComboBox);

    AddMiscComponents(jPanel1, SetupInfo.REPORT_TITLE_ACCOUNTS, "accountresult");

    customerComboBox.loadComboBox("CustName", "CustId", (Integer) cc.comboBox.getSelectedItemsKey());
    accountComboBox.loadComboBox("AccDesc", "Account", (Integer) cc.comboBox.getSelectedItemsKey());
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
   *  Description of the Method
   */
  public void buttonExit() {
    SetupInfo.setProperty(sReportTitle, jTextField3.getText());
    super.buttonExit();
  }

  /**
   *  Description of the Field
   */
  public final int fieldSize[] = {0, 8, 14, 30, 30, 17, 17, 17};

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public String buttonGetReport() {
    if (!IsDateFormatGood())
      return "";

    StringBuffer sb = new StringBuffer();
    sb.append(
        "SELECT Amount.Act_id, InvDate, Activity.Notes, Customer, " +
        "IsDebit*Amount AS Debit, ABS(IsDebit - 1)*Amount AS Credit, " +
        "Amount.Account, AccDesc, Amount.Notes " +
        "FROM Amount, Activity, Account " +
        "WHERE Amount.Act_id=Activity.Act_id AND " +
        "Account.Account=Amount.Account AND " +
        "Account.CompId=Activity.CompId AND Account.CompId=Amount.CompId AND " +
        "Account.CompId=" + cc.comboBox.getSelectedItemsKey().toString() +
        " AND Activity.InvDate >= ? AND Activity.InvDate <= ? ");
    if (((Integer) accountComboBox.getSelectedItemsKey()).intValue() != 0)
      sb.append("AND Account.Account= " +
          accountComboBox.getSelectedItemsKey().toString());
    if (((Integer) customerComboBox.getSelectedItemsKey()).intValue() != 0)
      sb.append(
          " AND Customer LIKE " +
          customerComboBox.getSelectedItemsKey().toString());

    sb.append(" ORDER BY Amount.Account,Amount.Act_id ");
    JdbcTable db = new JdbcTable(sb.toString(),view);
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

    //Print the Report header
    AddReportHeaders();

    Integer processingAccount = (Integer) db.getObject("Amount.Account", null);
    boolean bIsNewAccount = false;
    boolean bIsNewPost = false;
    do {
      jTextArea.append("               " + Translator.getTranslation("Account") +
          " " + IntegerField.ConvertIntToLocalizedString(
          processingAccount) + " : " +
          (String) db.getObject("AccDesc", null));
      jTextArea.append(newline);
      jTextArea.append(newline);

      initializeRow(fieldSize, 6);
      addField(Translator.getTranslation("Transaction"), fieldSize, 1,
          -1, false);
      addField(Translator.getTranslation("InvDate"), fieldSize, 2,
          -1, false);
      addField(Translator.getTranslation("Commentary"), fieldSize, 3,
          -1, false);
      addField(Translator.getTranslation("Commentary"), fieldSize, 4,
          -1, false);
      addField(Translator.getTranslation("Customer"), fieldSize, 5,
          -1, false);
      addField(Translator.getTranslation("Debit"), fieldSize, 6,
          -1, true);
      addField(Translator.getTranslation("Credit"), fieldSize, 7,
          -1, true);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
      double fDebitTotal = 0;
      double fCreditTotal = 0;

      do {
        jTextArea.append(renderIntegerField(
            db.getObject("Amount.Act_id", null), fieldSize[1]));
        jTextArea.append(renderDateField(
            db.getObject("InvDate", null), fieldSize[2]));
        jTextArea.append(renderField((String)
            db.getObject("Activity.Notes", null), fieldSize[3]));
        jTextArea.append(renderField((String)
            db.getObject("Amount.Notes", null), fieldSize[4]));
        jTextArea.append(renderIntegerField(
            db.getObject("Customer", null), fieldSize[5]));

        jTextArea.append(renderDecimalField(((Double)
            db.getObject("Debit", null)), fieldSize[6]));
        fDebitTotal += ((Double)
            db.getObject("Debit", null)).doubleValue();
        jTextArea.append(renderDecimalField(((Double)
            db.getObject("Credit", null)), fieldSize[7]));
        fCreditTotal += ((Double)
            db.getObject("Credit", null)).doubleValue();
        jTextArea.append(newline);
        bIsNewPost = db.GetNextRecord();
        if (bIsNewPost)
          bIsNewAccount = processingAccount.compareTo(
              (Integer) db.getObject("Amount.Account", null)) != 0;
      } while (bIsNewPost && !bIsNewAccount);

      initializeRow(fieldSize, 7);
      addField("-----------------------", fieldSize, 6, -1, false);
      addField("-----------------------", fieldSize, 7, -1, false);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
      initializeRow(fieldSize, 6);
      addField(new Double(fDebitTotal), fieldSize, 6, Types.DOUBLE, false);
      addField(new Double(fCreditTotal), fieldSize, 7, Types.DOUBLE, false);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
      processingAccount = (Integer) db.getObject("Amount.Account", null);

    } while (bIsNewPost);
    return jTextField3.getText();
  }

}


