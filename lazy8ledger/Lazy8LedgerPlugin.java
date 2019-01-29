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


package lazy8ledger;


import java.awt.Component;
import java.awt.Frame;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.lang.reflect.*;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import javax.swing.*;
import java.sql.Connection;
import org.gjt.sp.jedit.*;
import org.gjt.sp.jedit.gui.DockableWindowManager;
import org.gjt.sp.jedit.gui.OptionsDialog;
import org.gjt.sp.jedit.io.FileVFS;
import org.gjt.sp.util.Log;
import org.lazy8.nu.ledger.forms.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.gen.*;


public class Lazy8LedgerPlugin extends EditPlugin
{

	// begin EditPlugin implementation

	public void start() {
    //if first time starting this program!!!
    if (!SetupInfo.getBoolProperty(SetupInfo.ACCEPTED_COPYRIGHT)) {
      showStartupInstallSequence();
      //change the flag so that we never come here again..!!
      SetupInfo.setBoolProperty(SetupInfo.ACCEPTED_COPYRIGHT,true);
      SetupInfo.store();
    
    }
    //need to set the default number formats.
    System.setProperty("user.language",SetupInfo.getProperty(SetupInfo.NUMBER_FORMAT_LANGUAGE));
    System.setProperty("user.region",SetupInfo.getProperty(SetupInfo.PRESENT_COUNTRY));
    try {
      Log.log(Log.DEBUG,this,"user.language="+System.getProperty("user.language") +
          "  : user.region="+System.getProperty("user.region"));
      Locale lc = new Locale(System.getProperty("user.language"),
          System.getProperty("user.region"));
      Locale.setDefault(lc);
    }catch(Exception eee){}
  }
	public void stop() {
    if (SetupInfo.getBoolProperty(SetupInfo.WARN_TO_DO_BACKUP)
          && JdbcTable.isDatabaseChanged) {
      if (JOptionPane.CANCEL_OPTION != JOptionPane.showConfirmDialog(
          null,
          Translator.getTranslation("You have changed your database, do you want to make a backup copy?"),
          Translator.getTranslation("Lazy 8 ledger"),
          JOptionPane.OK_CANCEL_OPTION,
          JOptionPane.QUESTION_MESSAGE)) {
       createBackupFrame(null);
       }
    }
  }

	public void createMenuItems(Vector menuItems)
	{
		menuItems.addElement(GUIUtilities.loadMenu("lazy8ledger-menu"));
	}


	public void createOptionPanes(OptionsDialog optionsDialog)
	{
		OptionGroup og = new OptionGroup(Translator.getTranslation("Lazy8Ledger Accounting"));
    JFrame view=GUIUtilities.getView(optionsDialog);
		og.addOptionPane(new LedgerGeneralOptionPane(view));
		og.addOptionPane(new LedgerNumberOptionPane(view));
		og.addOptionPane(new LedgerWarningsOptionPane(view));
		optionsDialog.addOptionGroup(og);
	}
   
  public static boolean showStartupInstallSequence() {
      /*start the whole setup sequence.......
      1.  Get what language.  First get from OS what is the OS language
      2.  Get the country and region in order to make changes in number formats
      3.  Get a password if desired.  highly recommended not to
      4.  Proffesional rules.  Dissallow changes in the database?
      5.  Get the menu type.  Bookkeeper, programmer bookeeper, programmer just testing this module.
      6.  Create a new company and period
      7.  Create a test company if desired.
      8.  Give them a message about the tutorial
      */
      boolean isInstalled=true;
      if (!SetupInfo.getBoolProperty(SetupInfo.IS_INSTALL_ROUTINE_DONE)) {
        DoYouWantToInstall inst=new DoYouWantToInstall(null);
        if(inst.isDoInstall){
              //change the flag so that we never come here again..!!
              SetupInfo.setBoolProperty(SetupInfo.IS_INSTALL_ROUTINE_DONE,true);
              SetupInfo.store();
	      new ChangeLanguagesDialogs(null,false);
	      new StartUpDialogs(null,false);
	      new CreateNewCompany(null,false);
	      new CreateTestCompany(null);
	      new GoToTutorialMessage(null);
      
	}
	else
		isInstalled=false;
      }
      return isInstalled;
  }
  public static void createBackupCompanyFrame(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup db = new DatabaseBackup(
        dc.con);
    final String[] sTables = {"Account", "Company", "Amount", "Customer",
        "Activity", "UniqNum", "AccountingPeriods"};
    db.BackupDatabase(sTables, true);
  }

   public static  void createRestoreCompanyFrame(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup db = new DatabaseBackup(
        dc.con);
    db.RestoreDatabase(true, null, 0);
  }

  public static  void createBackupFrame(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup db = new DatabaseBackup(
        dc.con);
    final String[] sTables = {"Account", "Company", "Amount", "Customer",
        "Activity", "UniqNum", "AccountingPeriods"};
    db.BackupDatabase(sTables, false);
  }

  public static  void createRestoreFrame(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup db = new DatabaseBackup(
       dc.con);
    db.RestoreDatabase(false, null, 0);
  }
  public static  void createRestoreFile(Connection con,
  		boolean bRestoreOneCompanyOnly, String fileName, int iNewCompany) {
          DatabaseBackup db = new DatabaseBackup (con);
          db.RestoreDatabase(bRestoreOneCompanyOnly,fileName, iNewCompany);
  }

  public static void createBackupCompanyFrame1_3(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup1_3 db = new DatabaseBackup1_3(
        dc.con);
    final String[] sTables = {"Account", "Company", "Amount", "Customer",
        "Activity", "UniqNum", "AccountingPeriods"};
    db.BackupDatabase(sTables, true);
  }

   public static  void createRestoreCompanyFrame1_3(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup1_3 db = new DatabaseBackup1_3(
        dc.con);
    db.RestoreDatabase(true, null, 0);
  }

  public static  void createBackupFrame1_3(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup1_3 db = new DatabaseBackup1_3(
        dc.con);
    final String[] sTables = {"Account", "Company", "Amount", "Customer",
        "Activity", "UniqNum", "AccountingPeriods"};
    db.BackupDatabase(sTables, false);
  }

  public static  void createRestoreFrame1_3(JFrame view) {
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return;
    DatabaseBackup1_3 db = new DatabaseBackup1_3(
       dc.con);
    db.RestoreDatabase(false, null, 0);
  }
  public static  void createRestoreFile1_3(Connection con,
  		boolean bRestoreOneCompanyOnly, String fileName, int iNewCompany) {
          DatabaseBackup1_3 db = new DatabaseBackup1_3(con);
          db.RestoreDatabase(bRestoreOneCompanyOnly,fileName, iNewCompany);
  }
}

