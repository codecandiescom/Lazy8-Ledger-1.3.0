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
import lazy8ledger.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class CreateTestCompany extends SerialHelpDialogs {
  public  CreateTestCompany(JFrame frame) {
    super(frame,Translator.getTranslation("Getting started.  Basic settings."),true);
  }
  public void showFirstScreen() {
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Do you want to create a 'test' company?  The test company already has several years accounting information.  The test company can be used to immediately test the many reports and to help you to see how it is meant for you to use this program") );
    leftButton = new JButton(Translator.getTranslation("No"));
    rightButton = new JButton(Translator.getTranslation("Yes"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          //create the test company
          DataConnection dc=DataConnection.getInstance(null);
          if(dc==null || !dc.bIsConnectionMade)return ;
          File ff;
          try{
            ff=Fileio.getFile("TestCompany." + SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE) + ".bin",
              "testdata", false, false);
          }catch(Exception ee){
            SystemLog.ErrorPrint("Cant find test database : " + ee.getMessage());
            return ;
          }
          
	  if("1.4".compareTo((String)(System.getProperty("java.version")))>0)
		  Lazy8LedgerPlugin.createRestoreFile1_3(dc.con,true,ff.getAbsolutePath() , 9989);
	  else 
		  Lazy8LedgerPlugin.createRestoreFile(dc.con,true,ff.getAbsolutePath() , 9989);
          setVisible(false);
          dispose();
        }
      }
        );
    leftButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          setVisible(false);
          dispose();
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
    setVisible(true);
  }
}

