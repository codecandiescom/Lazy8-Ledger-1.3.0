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
public class BalanceReport extends TextReport {
  private final static int ACC_COL_SUM = 0;
  private final static int ACC_COL_IS_ASSET = 1;
  private final static int ACC_COL_ACC_NUM = 2;
  private final static int ACC_COL_ACC_NAME = 3;

  public BalanceReport(View view ,String sReportName,String frameName) {
    super(view, Translator.getTranslation("Balance report"),"sumrep",frameName);
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      buttonExit();
      return ;
    }


    JPanel reportDefinitions = new JPanel();
    //createReportDefinitionPane(reportDefinitions);
    //jReports.setSelectedItem(sReportName + "." + SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE));

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
  private void PrintOutLevel1Sums(String sGroupComments,
      String sNowProcessingGroup, double dFirstLevelSum, double dFirstLevelBeforeSum,
      boolean bPrintBefore) {
    initializeRow(fieldSize, 5);
    if (bPrintBefore) {
      addField("------------------------", fieldSize, 3,
          -1, true);
      addField("------------------------", fieldSize, 5,
          -1, true);
    }
    addField("------------------------", fieldSize, 4,
        -1, true);
    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);

    initializeRow(fieldSize, 5);
    if (bPrintBefore) {
      addField(new Double(dFirstLevelBeforeSum), fieldSize, 3,
          Types.DOUBLE, false);
      addField(new Double(dFirstLevelSum + dFirstLevelBeforeSum), fieldSize, 5,
          Types.DOUBLE, false);
    }
    addField(new Double(dFirstLevelSum), fieldSize, 4,
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
      Integer iIsAsset = (Integer) db.getObject("FirstOfIsAsset", null);

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

      //we found a matching account number
      //we cant break after the find because there might be more
      arrayDyn[ACC_COL_ACC_NUM].add(intAccountNum);
      arrayDyn[ACC_COL_ACC_NAME].add(sAccountName);
      arrayDyn[ACC_COL_SUM].add(dAccountSum);
      arrayDyn[ACC_COL_IS_ASSET].add(iIsAsset);
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
      if (iAccountNumber.compareTo((Integer) arrayDyn[ACC_COL_ACC_NUM].get(i)) == 0)
        return ((Double) arrayDyn[ACC_COL_SUM].get(i)).doubleValue();
    return 0;
    //couldnt find the account.
  }

  /**
   *  Description of the Field
   */
  public final int fieldSize[] = {0, 8, 40, 17, 17, 17};

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public String buttonGetReport() {
    //
    //first create the main informations arrays
    //
    ArrayList[] arrayDyn = new ArrayList[ACC_COL_ACC_NAME + 1];
    for (int i = 0; i <= ACC_COL_ACC_NAME; i++)
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
        "ORDER BY Account.IsAsset DESC,Amount.Account ASC, Amount.IsDebit DESC ");
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
    ArrayList[] arrayDyn2 = new ArrayList[ACC_COL_ACC_NAME + 1];
    for (int i = 0; i <= ACC_COL_ACC_NAME; i++)
      arrayDyn2[i] = new ArrayList();

    String sb2 = new String(
        "SELECT Amount.Account, Sum(Amount.Amount) AS SumOfAmount1, " +
        "Amount.IsDebit, Account.IsAsset AS FirstOfIsAsset, " +
        "Account.AccDesc AS FirstAccDesc " +
        "FROM Amount,Account,Activity WHERE Amount.Act_id=Activity.Act_id " +
        " AND Amount.Account = Account.Account AND " +
        " Account.CompId=Activity.CompId AND Account.CompId=Amount.CompId " +
        " AND Account.CompId=" + cc.comboBox.getSelectedItemsKey().toString() +
        " AND Activity.InvDate < ? AND Account.IsAsset<>0 " +
        "GROUP BY Amount.Account, Amount.IsDebit " +
        "ORDER BY Amount.Account ASC, Amount.IsDebit DESC ");
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
      for (j = 0; j < arrayDyn[0].size(); j++){
        if (((Integer) arrayDyn2[ACC_COL_ACC_NUM].get(i)).compareTo
            ((Integer) arrayDyn[ACC_COL_ACC_NUM].get(j)) == 0)
          break;
        if (((Integer) arrayDyn2[ACC_COL_ACC_NUM].get(i)).compareTo
            ((Integer) arrayDyn[ACC_COL_ACC_NUM].get(j)) < 0){
          //found one that does not exist in the first list.  instert it in.
          arrayDyn[ACC_COL_ACC_NUM].add(j,arrayDyn2[ACC_COL_ACC_NUM].get(i));
          arrayDyn[ACC_COL_ACC_NAME].add(j,arrayDyn2[ACC_COL_ACC_NAME].get(i));
          arrayDyn[ACC_COL_IS_ASSET].add(j,arrayDyn2[ACC_COL_IS_ASSET].get(i));
          arrayDyn[ACC_COL_SUM].add(j,new Double(0));
          break;
        }
      }
    }

    //now we have all the information, loaded up into arrays, just print
    //it out

    //Print the Report header
    AddReportHeaders();

    initializeRow(fieldSize, 5);
    addField(Translator.getTranslation("Account"), fieldSize, 1,
        -1, false);
    addField(Translator.getTranslation("Account name"), fieldSize, 2,
        -1, false);
    addField(Translator.getTranslation("Incomming"), fieldSize, 3,
        -1, true);
    addField(Translator.getTranslation("Value"), fieldSize, 4,
        -1, true);
    addField(Translator.getTranslation("Outgoing"), fieldSize, 5,
        -1, true);

    jTextArea.append(sbRow.toString());
    jTextArea.append(newline);
    initializeRow(fieldSize, 5);
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
    boolean foundFirstNonAsset=false;

    //begin writing the report
    int i;
    for (i = 0; i < arrayDyn[0].size(); i++) {
      //if not currently proccessing a group, then maybe we will start one
      //or we have changed groups
      if (!foundFirstNonAsset &&
          ((Integer) (arrayDyn[ACC_COL_IS_ASSET].get(i))).intValue()==0) {
        foundFirstNonAsset=true;
        PrintOutLevel1Sums(sGroupComments, sNowProcessingGroup,
            dFirstLevelSum, dFirstLevelBeforeSum, 
            ((Integer) (arrayDyn[ACC_COL_IS_ASSET].get(i-1))).intValue()!=0);
        dFirstLevelSum = 0;
        dFirstLevelBeforeSum = 0;
      }
      //need to sum the groups
      double dd = ((Double) arrayDyn[ACC_COL_SUM].
          get(i)).doubleValue();
      dFirstLevelSum += dd;
      double ddbefore = getAccountSum(arrayDyn2, (Integer) arrayDyn[ACC_COL_ACC_NUM].get(i));
      dFirstLevelBeforeSum += ddbefore;
      if (dd == 0 && (ddbefore == 0 || 
          !(((Integer) (arrayDyn[ACC_COL_IS_ASSET].get(i))).intValue()!=0)))
        //if there is a zero sum on this, then just skip it.
        continue;
      //start printing out the rows
      initializeRow(fieldSize, 5);
      addField(arrayDyn[ACC_COL_ACC_NUM].get(i), fieldSize, 1,
          Types.INTEGER, false);
      addField(arrayDyn[ACC_COL_ACC_NAME].get(i), fieldSize, 2,
          -1, false);
      if (((Integer) (arrayDyn[ACC_COL_IS_ASSET].get(i))).intValue()!=0) {
        addField(new Double(ddbefore), fieldSize, 3,
            Types.DOUBLE, false);
        addField(new Double(dd + ddbefore), fieldSize, 5,
            Types.DOUBLE, false);
      }
      addField(arrayDyn[ACC_COL_SUM].get(i), fieldSize, 4,
          Types.DOUBLE, false);

      jTextArea.append(sbRow.toString());
      jTextArea.append(newline);

    }
    //need to print out the final sums
    PrintOutLevel1Sums(sGroupComments, sNowProcessingGroup,
        dFirstLevelSum, dFirstLevelBeforeSum, 
        ((Integer) (arrayDyn[ACC_COL_IS_ASSET].get(i-1))).intValue()!=0);

    return jTextField3.getText();
  }

}


