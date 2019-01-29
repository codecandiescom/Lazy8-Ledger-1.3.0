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
import java.awt.*;
import java.text.*;
import java.sql.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.util.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class TransactionReport extends TextReport {
  IntegerField intField1, intField2;

  /**
   *  Constructor for the TransactionReport object
   *
   *@param  helpViewerin  Description of the Parameter
   *@param  desktop       Description of the Parameter
   *@param  parent        Description of the Parameter
   */
  public TransactionReport(View view ) {
    super(view, Translator.getTranslation("Transaction report"),"transactionresult","lazy8ledger-transactionreport");
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      buttonExit();
      return ;
    }
    JPanel jPanel1 = new JPanel();

    jPanel1.setLayout(new GridLayout(9, 2));

    AddMiscComponents(jPanel1, SetupInfo.REPORT_TITLE_ACTIVITIES, "transactionresult");
    jPanel1.add(new HelpedLabel(Translator.getTranslation("Start Transaction number"),
        "Start", "transactionresult",view));
    intField1 = new IntegerField("Start", "transactionresult",view);
    jPanel1.add(intField1);
    jPanel1.add(new HelpedLabel(Translator.getTranslation("Stop Transaction number"),
        "Stop", "transactionresult",view));
    intField2 = new IntegerField("Stop", "transactionresult",view);
    jPanel1.add(intField2);
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
  public final int fieldSize[] = {0, 8, 14, 27, 27, 11};
  /**
   *  Description of the Field
   */
  public final int fieldAccSize[] = {0, 10, 8, 3, 40, 30, 7, 17, 17};

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
        "Amount.Account, AccDesc, FileInfo,RegDate,IsDebit," +
        "Amount.Amount, Amount.Notes " +
        "FROM Amount, Activity, Account " +
        "WHERE Amount.Act_id=Activity.Act_id AND " +
        "Account.Account=Amount.Account AND " +
        "Account.CompId=Activity.CompId AND Account.CompId=Amount.CompId AND " +
        "Account.CompId=" + cc.comboBox.getSelectedItemsKey().toString() +
        " AND Activity.InvDate >= ? AND Activity.InvDate <= ? ");
    if (intField1.getText().length() != 0 && intField2.getText().length() != 0 &&
        intField2.getInteger().intValue() >= intField1.getInteger().intValue())
      sb.append(" AND Activity.Act_id >= ? AND Activity.Act_id <= ? ");

    sb.append(" ORDER BY Amount.Act_id, IsDebit");
    JdbcTable db = new JdbcTable(sb.toString(),view);
    try {
      //invoice date
      db.setObject(new java.sql.Date(
          jTextField1.getDate().getTime()),
          Types.DATE);
      db.setObject(new java.sql.Date(
          jTextField2.getDate().getTime()),
          Types.DATE);
      if (intField1.getText().length() != 0 && intField2.getText().length() != 0 &&
          intField2.getInteger().intValue() >= intField1.getInteger().intValue()) {
        db.setObject(intField1.getInteger(), Types.INTEGER);
        db.setObject(intField2.getInteger(), Types.INTEGER);
      }
    }
    catch (Exception e) {
      Log.log(Log.DEBUG,this,"Error trying to execute sql="+sb.toString());
      Log.log(Log.DEBUG,this,"Error ="+e);
      return "";
    }

    if (!db.GetFirstRecord())
      return "";
    //Print the Report header
    if (intField1.getText().length() != 0 && intField2.getText().length() != 0 &&
        intField2.getInteger().intValue() >= intField1.getInteger().intValue()) {
      jTextArea.append(Translator.getTranslation("Start Transaction number") + "    " + intField1.getText());
      jTextArea.append(newline);
      jTextArea.append(Translator.getTranslation("Stop Transaction number") + "     " + intField2.getText());
      jTextArea.append(newline);
    }

    AddReportHeaders();

    Integer processingTransaction;
    boolean bNewRecordExists;
    do {
      processingTransaction = (Integer) db.getObject("Amount.Act_id", null);
      initializeRow(fieldSize, 5);
      addField(Translator.getTranslation("Transaction"), fieldSize, 1,
          -1, false);
      addField(Translator.getTranslation("InvDate"), fieldSize, 2,
          -1, false);
      addField(Translator.getTranslation("Commentary"), fieldSize, 3,
          -1, false);
      addField(Translator.getTranslation("Fileing information"), fieldSize, 4,
          -1, false);
      addField(Translator.getTranslation("RegDate"), fieldSize, 5,
          -1, false);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);

      jTextArea.append(renderIntegerField(
          db.getObject("Amount.Act_id", null), fieldSize[1]));
      jTextArea.append(renderDateField(
          db.getObject("InvDate", null), fieldSize[2]));
      jTextArea.append(renderField((String)
          db.getObject("Activity.Notes", null), fieldSize[3]));
      jTextArea.append(renderField((String)
          db.getObject("FileInfo", null), fieldSize[4]));
      jTextArea.append(renderDateField(
          db.getObject("RegDate", null), fieldSize[5]));

      jTextArea.append(newline);
      initializeRow(fieldAccSize, 8);
      addField(Translator.getTranslation("Account"), fieldAccSize, 2,
          -1, false);
      addField(Translator.getTranslation("Account name"), fieldAccSize, 4,
          -1, false);
      addField(Translator.getTranslation("Commentary"), fieldAccSize, 5,
          -1, false);
      addField(Translator.getTranslation("Customer"), fieldAccSize, 6,
          -1, false);
      addField(Translator.getTranslation("Debit"), fieldAccSize, 7,
          -1, true);
      addField(Translator.getTranslation("Credit"), fieldAccSize, 8,
          -1, true);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
      double fDebitTotal = 0;
      double fCreditTotal = 0;
      IntHolder iType = new IntHolder();
      do {
        initializeRow(fieldAccSize, 8);

        addField(db.getObject("Amount.Account", iType),
            fieldAccSize, 2, iType.iValue, false);
        addField(db.getObject("AccDesc", iType),
            fieldAccSize, 4, iType.iValue, false);
        addField(db.getObject("Amount.Notes", iType),
            fieldAccSize, 5, iType.iValue, false);
        addField(db.getObject("Customer", iType),
            fieldAccSize, 6, iType.iValue, false);

        if (((Integer) db.getObject("IsDebit", null)).intValue() == 1) {
          addField(db.getObject("Amount.Amount", iType),
              fieldAccSize, 7, iType.iValue, false);
          fDebitTotal += ((Double)
              db.getObject("Amount.Amount", null)).doubleValue();
        }
        else {
          addField(db.getObject("Amount.Amount", iType),
              fieldAccSize, 8, iType.iValue, false);
          fCreditTotal += ((Double)
              db.getObject("Amount.Amount", null)).doubleValue();
        }
        jTextArea.append(sbRow.toString());
        jTextArea.append(newline);

        bNewRecordExists = db.GetNextRecord();
      } while (bNewRecordExists && processingTransaction.intValue()
           == ((Integer) db.getObject("Amount.Act_id", null)).intValue());

      initializeRow(fieldAccSize, 6);
      addField("---------------", fieldAccSize, 7, -1, true);
      addField("---------------", fieldAccSize, 8, -1, true);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
      initializeRow(fieldAccSize, 6);
      addField(new Double(fDebitTotal), fieldAccSize, 7, Types.DOUBLE, false);
      addField(new Double(fCreditTotal), fieldAccSize, 8, Types.DOUBLE, false);
      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);
      jTextArea.append(newline);

    } while (bNewRecordExists);

    return jTextField3.getText();
  }

}

