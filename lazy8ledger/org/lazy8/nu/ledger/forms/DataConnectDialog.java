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

import java.awt.*;
import java.awt.event.*;
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
public class DataConnectDialog extends JDialog {
  private String[] createTables;
  private JDesktopPane desktop;
  private JFrame view;
  public boolean isAbort=false;

  /**
   *  Description of the Method
   */
  public void SaveForm() {
    SetupInfo.setProperty(SetupInfo.CONNECT_DRIVER, textField1.getText());
    SetupInfo.setProperty(SetupInfo.CONNECT_DATABASE, textField2.getText());
    SetupInfo.setProperty(SetupInfo.CONNECT_USERNAME, textField3.getText());
    SetupInfo.setProperty(SetupInfo.CONNECT_PASSWORD,
        new String(textField4.getPassword()));
  }

  /**
   *  Constructor for the DataConnectDialog object
   *
   *@param  frame           Description of the Parameter
   *@param  createTablesin  Description of the Parameter
   *@param  desktopin       Description of the Parameter
   *@param  helpViewer      Description of the Parameter
   */
  public DataConnectDialog(javax.swing.JFrame frame, String[] createTablesin) {
    super(frame, Translator.getTranslation("Database connection"), true);
    this.view=frame;
    createTables = createTablesin;
    addWindowListener(
      new WindowAdapter() {
        public void windowClosing(WindowEvent e) {
          //if (!DataConnection.getInstance(view).IsConnectionMade())
          //  System.exit(0);
        }
      }
        );
    //if (DataConnection.getInstance(view).IsConnectionMade())
      //setDefaultCloseOperation(HIDE_ON_CLOSE);
    //else
    //setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
    jPanel1 = new JPanel();

    label1 = new HelpedLabel(Translator.getTranslation("JDBC Driver information"),
        "driver", "dataconn",view);
    textField1 = new HelpedTextField("driver", "dataconn",view);
    label2 = new HelpedLabel(Translator.getTranslation("Database connection information"),
        "dataconnect", "dataconn",view);
    textField2 = new HelpedTextField("dataconnect", "dataconn",view);

    label3 = new HelpedLabel(Translator.getTranslation("User name"),
        "user", "dataconn",view);
    textField3 = new HelpedTextField("user", "dataconn",view);

    label4 = new HelpedLabel(Translator.getTranslation("Password"),
        "password", "dataconn",view);
    textField4 = new JPasswordField();

    getContentPane().setLayout(new GridLayout(1, 1));

    getContentPane().add(jPanel1);

    jPanel1.setLayout(new GridLayout(6, 2));

    jPanel1.add(label1);

    jPanel1.add(textField1);

    jPanel1.add(label2);

    jPanel1.add(textField2);

    jPanel1.add(label3);

    jPanel1.add(textField3);

    jPanel1.add(label4);

    jPanel1.add(textField4);

    jPanel1.add(new JLabel());
    
    button1 = new HelpedButton(Translator.getTranslation("Cancel"),
        "Cancel", "dataconn",view);

    button1.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          isAbort=true;
          DataConnectDialog.this.setVisible(false);
        }
      }
        );

    jPanel1.add(button1);

    JButton button4 = new HelpedButton(Translator.getTranslation("OK"),
        "ok", "dataconn",view);

    button4.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonOK();
        }
      }
        );
    jPanel1.add(button4);

    JButton button5 = new HelpedButton(Translator.getTranslation("Help"),
        "help", "dataconn",view);

    button5.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonHelp();
        }
      }
        );
    jPanel1.add(button5);

    /*
     *  try to read the information from file.  Else default and save
     */
    // read it in again
    textField1.setText(SetupInfo.getProperty(SetupInfo.CONNECT_DRIVER));
    textField2.setText(SetupInfo.getProperty(SetupInfo.CONNECT_DATABASE));
    textField3.setText(SetupInfo.getProperty(SetupInfo.CONNECT_USERNAME));
    textField4.setText(SetupInfo.getProperty(SetupInfo.CONNECT_PASSWORD));

    pack();
    setLocationRelativeTo(frame);
    setVisible(true);
  }

  private JPanel jPanel1;
  private JLabel label1;
  private JTextField textField1;
  private JLabel label2;
  private JTextField textField2;

  private JLabel label3;
  private JTextField textField3;

  private JLabel label4;
  private JPasswordField textField4;

  private JButton button1;


  /**
   *  Description of the Method
   */
  private void buttonOK() {
    button1ActionPerformed(null);
  }

  /**
   *  Description of the Method
   */
  private void buttonHelp() {
    Lazy8Ledger.ShowHelp(view, "dataconn" ,"");
  }

  /**
   *  Description of the Method
   *
   *@param  evt  Description of the Parameter
   */
  private void button1ActionPerformed(java.awt.event.ActionEvent evt) {
    SaveForm();
  }
}

