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
package org.lazy8.nu.ledger.jdbc;

import java.sql.*;
import java.awt.event.*;
import javax.swing.*;
import org.lazy8.nu.util.gen.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class TransactionAccess extends JdbcTable {
  /**
   *  Description of the Field
   */
  public JdbcTable amountAccess;
  /**
   *  Description of the Field
   */
  public JdbcTable transactionAccess;

  /**
   *  Constructor for the TransactionAccess object
   */
  public TransactionAccess(JFrame view) {
    super("Activity", 2,view);
    amountAccess = new JdbcTable("Amount", 3,view);
    transactionAccess = new JdbcTable("Activity", 2,view);
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean AddRecord() {
    try {
      return transactionAccess.AddRecord();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(null,
          Translator.getTranslation("Something went wrong during updating")
           + " : " + e.getMessage(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);

      SystemLog.ProblemPrintln("Error:" + e.getMessage());
    }
    return false;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean DeleteRecord() {
    if (transactionAccess.DeleteRecord()) {
      amountAccess.setObject(transactionAccess.getObject("Act_id", null), "Act_id");
      amountAccess.setObject(transactionAccess.getObject("CompId", null), "CompId");
      return amountAccess.DeleteRecord();
    }
    return false;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean ChangeRecord() {
    return transactionAccess.ChangeRecord();
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean GetNextRecord() {
    if (transactionAccess.GetNextRecord()) {
      amountAccess.setObject(transactionAccess.getObject("Act_id", null), "Act_id");
      amountAccess.setObject(transactionAccess.getObject("CompId", null), "CompId");
      return amountAccess.GetFirstRecord();
    }
    return false;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean GetFirstRecord() {
    if (transactionAccess.GetFirstRecord()) {
      amountAccess.setObject(transactionAccess.getObject("Act_id", null), "Act_id");
      amountAccess.setObject(transactionAccess.getObject("CompId", null), "CompId");
      return amountAccess.GetFirstRecord();
    }
    else
      return false;
  }

  /**
   *  Description of the Method
   *
   *@param  Action            Description of the Parameter
   *@param  sTextFieldNames   Description of the Parameter
   *@param  sTextFields       Description of the Parameter
   *@param  sErrorMessage     Description of the Parameter
   *@param  bShowErrorDialog  Description of the Parameter
   *@return                   Description of the Return Value
   */
  public boolean GetFirstSeekRecord(int Action[], String sTextFieldNames[], String sTextFields[],
      StringBuffer sErrorMessage, boolean bShowErrorDialog) {
    if (transactionAccess.GetFirstSeekRecord(Action, sTextFieldNames, sTextFields,
        sErrorMessage, bShowErrorDialog)) {
      amountAccess.setObject(transactionAccess.getObject("Act_id", null), "Act_id");
      amountAccess.setObject(transactionAccess.getObject("CompId", null), "CompId");
      return amountAccess.GetFirstRecord();
    }
    else
      return false;
  }

}


