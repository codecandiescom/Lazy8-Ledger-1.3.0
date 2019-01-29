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
import java.util.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.util.Log;
import org.gjt.sp.jedit.*;
import lazy8ledger.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class StartUpDialogs extends SerialHelpDialogs {
  public  StartUpDialogs(JFrame frame,boolean isExitButtons) {
    super(frame,Translator.getTranslation("Getting started.  Basic settings."),
      isExitButtons);
  }
  JTextField userNameField;
  JPasswordField passField;
  JPasswordField passField2;
  public void showFirstScreen() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Here you can enter a username and a password.  I highly recommend that you leave all these fields blank.  If you fill in a user name and password here then each time you start this program you will be required to put in a password.  Then, if you forget the password, you will not be able to see your data.") 
    	+ "\n" +
        Translator.getTranslation("Press NEXT to continue."));

    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          if (userNameField.getText().length() == 0 ^ new String(passField.getPassword()).length() ==0){
            JOptionPane.showMessageDialog(StartUpDialogs.this,
                Translator.getTranslation("All the fields must be filled in or all must be blank."),
                Translator.getTranslation("Update not entered"),
                JOptionPane.PLAIN_MESSAGE);
            
          }
          else {
            if (userNameField.getText().length() != 0 && new String(passField.getPassword()).length() !=0){
              if( new String(passField.getPassword()).compareTo(new String(passField2.getPassword()))!=0){
                JOptionPane.showMessageDialog(StartUpDialogs.this,
                    Translator.getTranslation("You must enter the same password 2 times"),
                    Translator.getTranslation("Update not entered"),
                    JOptionPane.PLAIN_MESSAGE);
                return ;
              }
              SetupInfo.setBoolProperty(SetupInfo.REQUIRE_LOGIN,false);
              SetupInfo.setProperty(SetupInfo.CONNECT_DRIVER,DataConnection.MCKOI_DRIVER);
              SetupInfo.setProperty(SetupInfo.CONNECT_DATABASE,"");
              SetupInfo.setProperty(SetupInfo.CONNECT_PASSWORD,new String(passField2.getPassword()));
              SetupInfo.setProperty(SetupInfo.CONNECT_USERNAME,userNameField.getText());
              SetupInfo.store();
              //build the database now with these passwords...
              DataConnection.getInstance(null);
              //from now on the passwords must be entered
              SetupInfo.setBoolProperty(SetupInfo.REQUIRE_LOGIN,true);
              SetupInfo.setProperty(SetupInfo.CONNECT_PASSWORD,"");
              SetupInfo.setProperty(SetupInfo.CONNECT_USERNAME,"");
              SetupInfo.store();
            }
            else{
              SetupInfo.setBoolProperty(SetupInfo.REQUIRE_LOGIN,false);
              SetupInfo.setProperty(SetupInfo.CONNECT_DRIVER,DataConnection.MCKOI_DRIVER);
              SetupInfo.setProperty(SetupInfo.CONNECT_DATABASE,"");
              SetupInfo.setProperty(SetupInfo.CONNECT_PASSWORD,"PasswordIsPass");
              SetupInfo.setProperty(SetupInfo.CONNECT_USERNAME,"Pass");
              SetupInfo.store();
            }
            enterProfessionalRulesStage();
          }
        }
      }
        );
    JPanel wholeSouth=new JPanel();
    wholeSouth.setLayout(new GridLayout(2, 1));
    JPanel panelLanguages=new JPanel();
    panelLanguages.setLayout(new GridLayout(3, 2));
    wholeSouth.add(panelLanguages);

    userNameField=new JTextField();
    passField=new JPasswordField();
    passField2=new JPasswordField();
    panelLanguages.add(new JLabel(Translator.getTranslation("User name")));
    panelLanguages.add(userNameField);
    panelLanguages.add(new JLabel(Translator.getTranslation("Password")));
    panelLanguages.add(passField);
    panelLanguages.add(new JLabel(Translator.getTranslation("Enter the password again")));
    panelLanguages.add(passField2);

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    wholeSouth.add(jPanel1);    
    getContentPane().add(wholeSouth, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    setVisible(true);
  }
  /**
   *  Description of the Method
   */
  JRadioButton[] buttonProffesional;
  public void enterProfessionalRulesStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(
    	Translator.getTranslation("Do you want to be able to change transactions entered into the database?  The laws in most countries do not allow changes for proffesional accounting.  The lawful solution is to enter another transaction to correct the wrong transaction.  However, such strict rules are unnecessary for a non-proffesional use of this program.   If you are not sure, I suggest you choose to allow changes to the database.  This choice can not be changed later!") 
    	+ "\n" +
        Translator.getTranslation("Press NEXT to continue."));
    
    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          SetupInfo.setBoolProperty(SetupInfo.ALLOW_TRANSACTION_CHANGES,
              buttonProffesional[0].isSelected());
          SetupInfo.store();
          selectMenuType();
        }
      }
        );
    JPanel wholeSouth=new JPanel();
    wholeSouth.setLayout(new GridLayout(2, 1));
    JPanel panelLanguages=new JPanel();
    panelLanguages.setLayout(new BoxLayout(panelLanguages,BoxLayout.Y_AXIS));
    wholeSouth.add(panelLanguages);    
    
    buttonProffesional=new JRadioButton[2];
    ButtonGroup group = new ButtonGroup();
    buttonProffesional[0]=new JRadioButton(Translator.getTranslation("Allow changes to the transactions"));
    group.add(buttonProffesional[0]);
    panelLanguages.add(buttonProffesional[0]);
    buttonProffesional[1]=new JRadioButton(Translator.getTranslation("Proffesional rules.  No changes allowed."));
    group.add(buttonProffesional[1]);
    panelLanguages.add(buttonProffesional[1]);
    buttonProffesional[0].setSelected(true);
        
    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    wholeSouth.add(jPanel1);    
    getContentPane().add(wholeSouth, BorderLayout.SOUTH);
    
    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);

  }
  JRadioButton[] menuButtonLevels;
  public void selectMenuType() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(
    	Translator.getTranslation("Choose what sort of user you are?  This information is used to decide how much to change the jEdit environment.  If you choose 'Accounting only' then the jEdit environment will be changed to only allow accounting.  It is very important that you choose this if you only want to do accounting and you are not a programmer.  If you choose 'Programmer accountant' then the jEdit environment will be changed to enhance accounting, but you may still use jEdit for programming.  If you choose 'Programmer', then the JEdit environment will not be changed at all.") 
	+ "\n" +
        Translator.getTranslation("Press NEXT to continue."));
    
    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          SetupInfo.setBoolProperty(SetupInfo.ALLOW_TRANSACTION_CHANGES,
              buttonProffesional[0].isSelected());
          LedgerGeneralOptionPane.saveMenuLevel(menuButtonLevels);
          SetupInfo.store();
          setVisible(false);
          dispose();
        }
      }
        );

    JPanel wholeSouth=new JPanel();
    wholeSouth.setLayout(new BoxLayout(wholeSouth,BoxLayout.Y_AXIS));
    JPanel horizontalSpacePanel = new JPanel();
    horizontalSpacePanel.setLayout(new BoxLayout(horizontalSpacePanel, BoxLayout.X_AXIS));
    horizontalSpacePanel.add(Box.createHorizontalGlue());
    menuButtonLevels=new JRadioButton[LedgerGeneralOptionPane.NUM_MENU_LEVELS];
    JFrame view=GUIUtilities.getView(frameParent);
    horizontalSpacePanel.add(LedgerGeneralOptionPane.createMenuSelectionPanel( menuButtonLevels,view));
    horizontalSpacePanel.add(Box.createHorizontalGlue());

    wholeSouth.add(horizontalSpacePanel);    
    
    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    wholeSouth.add(jPanel1);    
    getContentPane().add(wholeSouth, BorderLayout.SOUTH);
    
    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }

}

