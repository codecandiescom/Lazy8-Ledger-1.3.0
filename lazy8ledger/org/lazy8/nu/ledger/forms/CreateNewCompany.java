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
import java.io.File;
import java.awt.event.*;
import javax.swing.text.*;
import javax.swing.text.rtf.*;
import java.util.Calendar;
import java.util.Date;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import lazy8ledger.*;

public class CreateNewCompany extends SerialHelpDialogs {
  private IntegerField textField1;
  private JTextField textField2;
  private DateField dateField1;
  private DateField dateField2;
  private int iCompId = 0;
  private String sCompName = null;

  public  CreateNewCompany( JFrame frame,boolean isExitButtons) {
    super(frame,Translator.getTranslation("Getting started.  Create a new company."),
      isExitButtons);
  }

  /**
   *  Description of the Method
   */
  public void showFirstScreen() {
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("This will get you started by creating a company and accounts for your bookkeeping system.") + "\n" +
        Translator.getTranslation("Press NEXT to continue."));
    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          enterCompanyStage();
        }
      }
        );

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    getContentPane().add(jPanel1, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    setVisible(true);
  }

  /**
   *  Description of the Method
   */
  public void enterCompanyStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Enter the name of your company and any four digit number that will represent your company.") + "\n" +
        Translator.getTranslation("Press NEXT to continue."));

    textField1 = new IntegerField("Number", "company",frameParent);
    textField2 = new HelpedTextField("Name", "company",frameParent);

    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          sCompName = textField2.getText();
          if (textField2.getText().length() == 0)
            JOptionPane.showMessageDialog(CreateNewCompany.this,
                Translator.getTranslation("The company name may not be blank"),
                Translator.getTranslation("Update not entered"),
                JOptionPane.PLAIN_MESSAGE);

          else {
            iCompId = textField1.getInteger().intValue();
            if (iCompId < 1000 || iCompId >= 10000)
              JOptionPane.showMessageDialog(CreateNewCompany.this,
                  Translator.getTranslation("The company number is not 4 digits"),
                  Translator.getTranslation("Update not entered"),
                  JOptionPane.PLAIN_MESSAGE);

            else
                if (CompanyForm.doesNumberExist(iCompId))
              JOptionPane.showMessageDialog(CreateNewCompany.this,
                  Translator.getTranslation("This number already exists"),
                  Translator.getTranslation("Update not entered"),
                  JOptionPane.PLAIN_MESSAGE);

            else {
              CompanyForm.quickAndDirtyAddCompany(iCompId, textField2.getText());
              SetupInfo.setProperty(SetupInfo.DEFAULT_COMPANY, Integer.toString(iCompId));
              SetupInfo.store();
              enterPeriodStage();
            }

          }
        }
      }
        );

    JPanel jPanel3 = new JPanel();
    jPanel3.setLayout(new GridLayout(2, 2));
    jPanel3.add(new JLabel(Translator.getTranslation("CompId")));
    jPanel3.add(textField1);
    jPanel3.add(new JLabel(Translator.getTranslation("Company name")));
    jPanel3.add(textField2);

    JPanel jPanel2 = new JPanel();
    jPanel2.setLayout(new GridLayout(2, 1));
    jPanel2.add(jPanel3);

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    jPanel2.add(jPanel1);

    getContentPane().add(jPanel2, BorderLayout.SOUTH);

    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    pack();
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }

  /**
   *  Description of the Method
   */
  public void enterPeriodStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Enter the accounting period you want to work with.  This is usually from the beginning of the year to the end of the year.") + "\n" +
        Translator.getTranslation("Press NEXT to continue."));

    dateField1 = new DateField("From", "periodform",frameParent);
    dateField2 = new DateField("To", "periodform",frameParent);

    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          boolean success = false;
          try {
            dateField1.getDate();
            dateField2.getDate();
            success = true;
          }
          catch (Exception e1) {
            JOptionPane.showMessageDialog(null,
                Translator.getTranslation("Date must be in the following format") +
                " : " +
                DateField.getTodaysDateString(),
                Translator.getTranslation("Update not entered"),
                JOptionPane.PLAIN_MESSAGE);
          }
          if (success)
            try {
              if (dateField1.getDate().compareTo(dateField2.getDate()) >= 0)
                JOptionPane.showMessageDialog(null,
                    Translator.getTranslation("The StartPeriod must be less than the EndPeroid"),
                    Translator.getTranslation("Update not entered"),
                    JOptionPane.PLAIN_MESSAGE);

              else {
                try {
                  DataConnection dc=DataConnection.getInstance(null);
                  if(dc==null || !dc.bIsConnectionMade)return ;
                  PreparedStatement stmt = dc.con.prepareStatement(
                      "INSERT INTO AccountingPeriods (CompId,StartPeriod,EndPeriod) VALUES(?,?,?)");
                  stmt.setInt(1, iCompId);
                  stmt.setDate(2, new java.sql.Date(dateField1.getDate().getTime()));
                  stmt.setDate(3, new java.sql.Date(dateField2.getDate().getTime()));
                  stmt.executeUpdate();
                  stmt.close();
                }
                catch (Exception ex) {
                  System.err.println("Couldnt add Period " + iCompId + ":" + sCompName + ":" + ex);
                }
                SetupInfo.setProperty(SetupInfo.DEFAULT_PERIOD1,
                    DateField.ConvertDateToLocalizedString(dateField1.getDate()));
                SetupInfo.setProperty(SetupInfo.DEFAULT_PERIOD2,
                    DateField.ConvertDateToLocalizedString(dateField2.getDate()));
                SetupInfo.store();
                chooseDefaultAccountsStage();
              }
            }
            catch (Exception ex) {
            }

        }
      }
        );
    JPanel jPanel3 = new JPanel();
    jPanel3.setLayout(new GridLayout(2, 2));
    jPanel3.add(new JLabel(Translator.getTranslation("StartPeriod")));
    jPanel3.add(dateField1);
    jPanel3.add(new JLabel(Translator.getTranslation("EndPeriod")));
    jPanel3.add(dateField2);

    Calendar StartYear = Calendar.getInstance();
    Calendar EndYear = Calendar.getInstance();
    StartYear.set(StartYear.get(Calendar.YEAR), Calendar.JANUARY, 1);
    EndYear.set(StartYear.get(Calendar.YEAR), Calendar.DECEMBER, 31);
    dateField1.setDate(StartYear.getTime());
    dateField2.setDate(EndYear.getTime());

    JPanel jPanel2 = new JPanel();
    jPanel2.setLayout(new GridLayout(2, 1));
    jPanel2.add(jPanel3);

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    jPanel2.add(jPanel1);
    getContentPane().add(jPanel2, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }

  /**
   *  Description of the Method
   */
  public void chooseDefaultAccountsStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Do you want to use the standard default account system based on the EU BAS 99 account plan?") + "\n" +
        Translator.getTranslation("I highly recommend you answer 'yes' if you don't already have an account plan.") + "\n\n"
        );
    leftButton = new JButton(Translator.getTranslation("No"));
    rightButton = new JButton(Translator.getTranslation("Yes"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          DataConnection dc=DataConnection.getInstance(null);
          if(dc==null || !dc.bIsConnectionMade)return ;
          File ff;
          try{
            ff=Fileio.getFile("AccountPlan." + SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE) + ".bin",
              "testdata", false, false);
          }catch(Exception ee){
            SystemLog.ErrorPrint("Cant find test database : " + ee.getMessage());
            return ;
          }
	  if("1.4".compareTo((String)(System.getProperty("java.version")))>0)
		  Lazy8LedgerPlugin.createRestoreFile1_3(dc.con,true,ff.getAbsolutePath() , iCompId);
	  else 
		  Lazy8LedgerPlugin.createRestoreFile(dc.con,true,ff.getAbsolutePath() , iCompId);
          CompanyForm.quickAndDirtyChangeCompany(iCompId, sCompName);
          //name was destroyed in the last operation
          endDefaultAccountsStage();
        }
      }
        );
    leftButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          choosenNoDefaultAccountsStage();
        }
      }
        );
    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(leftButton);
    jPanel1.add(rightButton);
    getContentPane().add(jPanel1, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }

  /**
   *  Description of the Method
   */
  public void choosenNoDefaultAccountsStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("You must then begin entering your own accounts. Go to the menu DataEntry->Account after you press END below.") + "\n\n" +
        Translator.getTranslation("After your accounts are entered, go to the menu DataEntry->Transaction to begin entering what has happened to your accounts.")
        );
    rightButton = new JButton(Translator.getTranslation("End"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          setVisible(false);
        }
      }
        );

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(new JPanel());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    getContentPane().add(jPanel1, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }

  /**
   *  Description of the Method
   */
  public void endDefaultAccountsStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("You may at any time enter more accounts by going to the menu DataEntry->Account") + "\n" +
        Translator.getTranslation("All that is left is to begin entering what has happened in the accounts.") + "\n" +
        Translator.getTranslation("Go to the menu DataEntry->Transaction after you press END below")
        );
    rightButton = new JButton(Translator.getTranslation("End"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          setVisible(false);
        }
      }
        );

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(new JPanel());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    getContentPane().add(jPanel1, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }
}

