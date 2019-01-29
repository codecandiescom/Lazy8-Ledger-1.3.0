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
import org.lazy8.nu.ledger.main.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DataMovementPane extends JPanel {

  final static int ADD = 1;
  final static int DELETE = 2;
  final static int FIRST = 3;
  final static int CHANGE = 4;
  private JFrame view;

  public DataMovementPane(
      JdbcTable dataAccessin,
      DataExchangeForm formDataExchangein,boolean bAddOnly,JFrame view) {
    initialize(dataAccessin,
      formDataExchangein,bAddOnly,view);
  }
  public DataMovementPane(
      JdbcTable dataAccessin,
      DataExchangeForm formDataExchangein,JFrame view) {
    initialize(dataAccessin,
      formDataExchangein,false, view);
  }
  private void initialize(
      JdbcTable dataAccessin,
      DataExchangeForm formDataExchangein,boolean bAddOnly,JFrame view) {
    this.view=view;
    dataAccess = dataAccessin;
    formDataExchange = formDataExchangein;

    butAdd = new HelpedButton(Translator.getTranslation("Add"),
        "add", "datamove",view);
    buttonDelete = new HelpedButton(Translator.getTranslation("Delete"),
        "delete", "datamove",view);
    buttonChange = new HelpedButton(Translator.getTranslation("Change"),
        "change", "datamove",view);
    buttonGetNext = new HelpedButton(Translator.getTranslation("Next"),
        "next", "datamove",view);
    buttonGetFirst = new HelpedButton(Translator.getTranslation("First"),
        "first", "datamove",view);
    buttonClear = new HelpedButton(Translator.getTranslation("Clear"),
        "clear", "datamove",view);
    buttonSeek = new HelpedButton(Translator.getTranslation("Find"),
        "find", "datamove",view);
    buttonExit = new HelpedButton(Translator.getTranslation("Exit"),
        "exit", "datamove",view);
    buttonHelp = new HelpedButton(Translator.getTranslation("Help"),
        "help", "datamove",view);

    if(bAddOnly){
      add(butAdd);
      add(buttonExit);
      add(buttonHelp);
  
      setLayout(new GridLayout(1, 3));
    }
    else{
      add(buttonGetFirst);
      add(buttonGetNext);
      add(buttonSeek);
      add(new JLabel());
      add(buttonClear);
      add(new JLabel());
      add(butAdd);
      add(buttonChange);
      add(buttonDelete);
      add(buttonExit);
      add(new JLabel());
      add(buttonHelp);
  
      setLayout(new GridLayout(4, 3));
    }

    butAdd.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          butAddActionPerformed(evt);
        }
      }
        );
    buttonDelete.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonDeleteActionPerformed(evt);
        }
      }
        );
    buttonClear.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonClearActionPerformed(evt);
        }
      }
        );
    buttonGetNext.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonGetNextActionPerformed(evt);
        }
      }
        );
    buttonGetFirst.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonGetFirstActionPerformed(evt);
        }
      }
        );
    buttonSeek.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonSeekActionPerformed(evt);
        }
      }
        );
    buttonChange.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonChangeActionPerformed(evt);
        }
      }
        );
    buttonExit.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonExitActionPerformed(evt);
        }
      }
        );
    buttonHelp.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonHelpActionPerformed(evt);
        }
      }
        );

    butAdd.setEnabled(true);
    buttonDelete.setEnabled(false);
    buttonChange.setEnabled(false);
    formDataExchange.ChangeButtonEnabled(false);
    buttonGetNext.setEnabled(false);

  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  public void butAddActionPerformed(java.awt.event.ActionEvent evt) {
    if (formDataExchange.IsWriteOK()) {
      formDataExchange.getFields(ADD);
      if (dataAccess.AddRecord()) {
        butAdd.setEnabled(false);
        buttonDelete.setEnabled(true);
        buttonChange.setEnabled(true);
        formDataExchange.ChangeButtonEnabled(true);
        buttonGetNext.setEnabled(false);
        formDataExchange.AfterGoodWrite();
      }
    }
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void buttonDeleteActionPerformed(java.awt.event.ActionEvent evt) {
    if (formDataExchange.IsDeleteOK()) {
      formDataExchange.getFields(DELETE);
      if (dataAccess.DeleteRecord()) {
        butAdd.setEnabled(false);
        buttonDelete.setEnabled(false);
        buttonChange.setEnabled(false);
        formDataExchange.ChangeButtonEnabled(false);
        buttonGetNext.setEnabled(false);
        formDataExchange.AfterGoodDelete();
      }
    }
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  public void buttonClearActionPerformed(java.awt.event.ActionEvent evt) {
    formDataExchange.clearFields();
    butAdd.setEnabled(true);
    buttonDelete.setEnabled(false);
    buttonChange.setEnabled(false);
    formDataExchange.ChangeButtonEnabled(false);
    buttonGetNext.setEnabled(false);
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void buttonGetNextActionPerformed(java.awt.event.ActionEvent evt) {
    if (dataAccess.GetNextRecord())
      formDataExchange.putFields();
    else
      buttonClearActionPerformed(evt);
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void buttonGetFirstActionPerformed(java.awt.event.ActionEvent evt) {
    formDataExchange.getFields(FIRST);
    if (dataAccess.GetFirstRecord()) {
      formDataExchange.putFields();
      butAdd.setEnabled(false);
      buttonDelete.setEnabled(true);
      buttonChange.setEnabled(true);
      formDataExchange.ChangeButtonEnabled(true);
      buttonGetNext.setEnabled(true);
    }
    else
      buttonClearActionPerformed(evt);

  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void buttonSeekActionPerformed(java.awt.event.ActionEvent evt) {
    FindRecordDialog skDialog = new FindRecordDialog(formDataExchange.desktop, null, dataAccess,
        formDataExchange.bShowCompanyAlways);
    if (skDialog.returnValue == true) {
      if (dataAccess.GetFirstSeekRecord(skDialog.iActions, skDialog.sFieldNames, skDialog.sValues,
          new StringBuffer(""), true)) {
        formDataExchange.putFields();
        butAdd.setEnabled(false);
        buttonDelete.setEnabled(true);
        buttonChange.setEnabled(true);
        formDataExchange.ChangeButtonEnabled(true);
        buttonGetNext.setEnabled(true);
      }
      else
        buttonClearActionPerformed(evt);
    }
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void buttonChangeActionPerformed(java.awt.event.ActionEvent evt) {
    if (formDataExchange.IsChangeOK()) {
      formDataExchange.getFields(CHANGE);
      if (dataAccess.ChangeRecord()) {
        butAdd.setEnabled(false);
        buttonDelete.setEnabled(true);
        buttonChange.setEnabled(true);
        formDataExchange.ChangeButtonEnabled(true);
        buttonGetNext.setEnabled(false);
        formDataExchange.AfterGoodWrite();
      }
    }
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void buttonExitActionPerformed(java.awt.event.ActionEvent evt) {
    formDataExchange.Exit();
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  public void buttonHelpActionPerformed(java.awt.event.ActionEvent evt) {
    Lazy8Ledger.ShowHelp(view, formDataExchange.HelpFile ,"");
  }

  /**
   *  Description of the Field
   */
  public HelpedButton butAdd;
  private HelpedButton buttonDelete;
  private HelpedButton buttonChange;
  private HelpedButton buttonGetNext;
  private HelpedButton buttonGetFirst;
  private HelpedButton buttonSeek;
  private HelpedButton buttonClear;
  private HelpedButton buttonExit;
  private HelpedButton buttonHelp;
  private DataExchangeForm formDataExchange;
  private JdbcTable dataAccess;
}


