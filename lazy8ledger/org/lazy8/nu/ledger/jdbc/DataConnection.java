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
import javax.swing.*;
import java.awt.*;
import java.io.File;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.ledger.forms.*;
import org.lazy8.nu.ledger.main.*;
import lazy8ledger.*;
/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DataConnection {
  /**
   *  Description of the Field
   */
  public Connection con;
  public boolean bIsConnectionMade;
  private Driver drv;
  private JDialog jd;
  private CompanyComponents companyComponents ;

  /**
   *  Constructor for the DataConnection object
   */
  public DataConnection(JFrame view) {
    //show always the welcome window
    Lazy8Ledger.ShowContextHelp(view, "welcome" ,"");
    bIsConnectionMade = false;
    String stringPass = SetupInfo.getProperty(SetupInfo.CONNECT_PASSWORD);
    String stringUser = SetupInfo.getProperty(SetupInfo.CONNECT_USERNAME);
    boolean isLoginRequired=SetupInfo.getBoolProperty(SetupInfo.REQUIRE_LOGIN);
    if (isLoginRequired) {
      PasswordDialog pDialog = new PasswordDialog(null,true,false);
      if (pDialog.bIsCancel)
        return;
      stringPass = new String(pDialog.jPass.getPassword());
      stringUser=new String(pDialog.jUser.getText());
      pDialog.dispose();
    }

    while (!(makeConnection(
        createTables,
        SetupInfo.getProperty(SetupInfo.CONNECT_DRIVER),
        SetupInfo.getProperty(SetupInfo.CONNECT_DATABASE),
        stringUser,
        stringPass))){
      JOptionPane.showMessageDialog(view,
          Translator.getTranslation("Unable to connect to the database.  Perhaps wrong password"),
          Translator.getTranslation("Lazy 8 ledger"),
          JOptionPane.PLAIN_MESSAGE);
      DataConnectDialog ds=ShowDataConnectDialog(view);
      if(ds.isAbort)break;
      stringPass = SetupInfo.getProperty(SetupInfo.CONNECT_PASSWORD);
      stringUser = SetupInfo.getProperty(SetupInfo.CONNECT_USERNAME);
    }
  }
  public static DataConnectDialog ShowDataConnectDialog(JFrame frame){
    return new DataConnectDialog(frame, createTables);
  }
  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsConnectionMade() {
    return bIsConnectionMade;
  }

  /**
   *  Description of the Method
   *
   *@param  createTables   Description of the Parameter
   *@param  stringDriver   Description of the Parameter
   *@param  stringConnect  Description of the Parameter
   *@param  stringName     Description of the Parameter
   *@param  stringPass     Description of the Parameter
   *@return                Description of the Return Value
   */
  public boolean makeConnection(String createTables[], String stringDriver, String stringConnect,
      String stringName, String stringPass) {
    bIsConnectionMade = false;
    Statement stmt;

    try {
      drv = (Driver) Class.forName(stringDriver).newInstance();
    }
    catch (Exception e) {
      SystemLog.ErrorPrint("ClassNotFoundException: ");
      SystemLog.ErrorPrintln(e.getMessage());
      return false;
    }
    if(stringDriver.compareTo(MCKOI_DRIVER)==0){
      File ff;
      try{
        ff=Fileio.getFile("db.conf", "data", false, false);
      }catch(Exception ee){
        SystemLog.ErrorPrint("Cant find db.conf : " + ee.getMessage());
        return false;
      }
      stringConnect="jdbc:mckoi:local://" + ff.getAbsolutePath(); 
      SystemLog.ErrorPrintln(stringConnect);
      if(stringName.length()==0 || stringPass.length()==0){
        //mckoi requires some sort of password
        stringName="PasswordIsPass";
        stringPass="Pass";
        SetupInfo.setProperty(SetupInfo.CONNECT_USERNAME, stringName);
        SetupInfo.setProperty(SetupInfo.CONNECT_PASSWORD,stringPass);
        SetupInfo.store();
      }
      
      try {
        con = java.sql.DriverManager.getConnection(
            stringConnect, stringName, stringPass);
      }
      catch (Exception ex) {
        // This URL specifies we are creating a local database.  The
        // configuration file for the database is found at './ExampleDB.conf'
        // The 'create=true' argument means we want to create the database.  If
        // the database already exists, it can not be created.
        stringConnect="jdbc:mckoi:local://" + ff.getAbsolutePath() + "?create=true";
    
        // The username/password for the database.  This will be the username/
        // password for the user that has full control over the database.
        // ( Don't use this demo username/password in your application! )
    
        // Make a connection with the database.  This will create the database
        // and log into the newly created database.
        try {
          con = DriverManager.getConnection(stringConnect, stringName, stringPass);
        }
        catch (SQLException e) {
          SystemLog.ErrorPrintln("ConnectionException: " +stringConnect + " : " + e.getMessage());
          return false;
        }
      }
    }else{
      try {
        con = DriverManager.getConnection(stringConnect, stringName, stringPass);
      }
      catch (SQLException e) {
        SystemLog.ErrorPrintln("ConnectionException: " +stringConnect + " : " + e.getMessage());
        return false;
      }
    }
    
    try {
      stmt = con.createStatement();

      for (int i = 0; i < createTables.length; i += 2)
        //check if the table exists first
        try {
          ResultSet rs = stmt.executeQuery(createTables[i]);
        }
        catch (Exception ex) {
          SystemLog.ErrorPrintln("Error executing test " + createTables[i]);
          SystemLog.ErrorPrintln(ex.getMessage());
          SystemLog.ErrorPrintln("Table probably does not exist, now creating");
          //lets try creating the table
          try {
            stmt.executeUpdate(createTables[i + 1]);
          }
          catch (Exception ex2) {
            SystemLog.ErrorPrintln("Error creating table: " +
                ex2.getMessage());
            //no way to recover from this, get out
            con.close();
            return false;
          }
        }

    }
    catch (Exception ex) {
      SystemLog.ErrorPrintln("ConnectionException: " +stringConnect + " : " + ex.getMessage());
      return false;
    }
    bIsConnectionMade = true;
    return true;
  }

  private static DataConnection myInstance;

  /**
   *  Gets the instance attribute of the DataConnection class
   *
   *@return    The instance value
   */
  public static DataConnection getInstance(JFrame view) {
    if (myInstance == null ||  ! myInstance.bIsConnectionMade){
      if(Lazy8LedgerPlugin.showStartupInstallSequence()){
	      
				myInstance = new DataConnection(view);
				if(myInstance.bIsConnectionMade){
					//this is a perfect starting point for the whole program.  Here you come the
					//first time you successfully start the program and you come here never again 
					//untill the next time the program is successfully started
					
					//Ask the user to choose a company/period...
					myInstance.jd=new JDialog(view,Translator.getTranslation("Select the company and period for this session"),true);
					myInstance.companyComponents = new CompanyComponents(myInstance.jd.getContentPane(),
							Translator.getTranslation("Default Company"), true, "setup",view);
					myInstance.companyComponents.AddPeriod(myInstance.jd.getContentPane(),
							Translator.getTranslation("Period"), true);
					if(!SetupInfo.getBoolProperty(SetupInfo.SHOW_ADVANCED_MENUS) &&
								myInstance.companyComponents.comboBox.getItemCount()>1 ||
								myInstance.companyComponents.comboBoxPeriod.getItemCount()>1){
						myInstance.jd.getContentPane().setLayout(new GridLayout(4, 2));
						
						myInstance.jd.getContentPane().add(new JLabel());
						myInstance.jd.getContentPane().add(new JLabel());
						myInstance.jd.getContentPane().add(new JLabel());
						JButton button3 = new JButton(Translator.getTranslation("OK"));
						button3.addActionListener(
							new java.awt.event.ActionListener() {
								public void actionPerformed(java.awt.event.ActionEvent evt) {
									 myInstance.companyComponents.saveDefaults();
									 SetupInfo.store();
									 myInstance.jd.setVisible(false);
									 myInstance.jd.dispose();
								}
							}
						);
						myInstance.jd.getContentPane().add(button3);
						myInstance.jd.getRootPane().setDefaultButton(button3);
	
						myInstance.jd.pack();
						if(view!=null)
							myInstance.jd.setLocationRelativeTo(view);
						myInstance.jd.setVisible(true);
					}
				}
      }
    }

    return myInstance;
  }
  public static class PasswordDialog extends JDialog {
    public JPasswordField jPass;
    public JPasswordField jPass2;
    public JTextField jUser;
    boolean bIsCancel = false;
    boolean twoPasswords;

    /**
     *  Constructor for the PasswordDialog object
     *
     *@param  frame  Description of the Parameter
     */
    public PasswordDialog(javax.swing.JFrame frame,boolean showUser,boolean twoPasswords) {
      super(frame, Translator.getTranslation("Lazy 8 ledger"), true);
      this.twoPasswords=twoPasswords;
      getContentPane().setLayout(new GridLayout(showUser||twoPasswords?4:3, 2));
      if(showUser){
        getContentPane().add(new JLabel(
            Translator.getTranslation("User name")));
        jUser = new JTextField();
        getContentPane().add(jUser);
      }
      getContentPane().add(new JLabel(
          Translator.getTranslation("Password")));
      jPass = new JPasswordField();
      getContentPane().add(jPass);
      if(twoPasswords){
        getContentPane().add(new JLabel(
            Translator.getTranslation("Enter the password again")));
        jPass2 = new JPasswordField();
        getContentPane().add(jPass2);
      }
      getContentPane().add(new JPanel());
      getContentPane().add(new JPanel());

      JButton button3 = new JButton(Translator.getTranslation("OK"));
      Dimension ddm2=button3.getPreferredSize();
      Dimension ddm=new Dimension(120,ddm2.height);
      button3.setPreferredSize(ddm);
      button3.setMinimumSize(ddm);
      button3.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            if(PasswordDialog.this.twoPasswords && new String(jPass.getPassword()).compareTo(
                  new String(jPass2.getPassword()))!=0){
              JOptionPane.showMessageDialog(PasswordDialog.this,
                  Translator.getTranslation("You must enter the same password 2 times"),
                  Translator.getTranslation("Update not entered"),
                  JOptionPane.PLAIN_MESSAGE);
              return;
            }
            buttonOK();
          }
        }
          );
      getContentPane().add(button3);
      if(showUser){
        JButton button4 = new JButton(Translator.getTranslation("Cancel"));
        button4.addActionListener(
          new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
              buttonCancel();
            }
          }
            );
        getContentPane().add(button4);
      }
      else
        getContentPane().add(new JLabel());
      getRootPane().setDefaultButton(button3);
      pack();
      setLocationRelativeTo(frame);
      setVisible(true);
    }

    /**
     *  Description of the Method
     */
    public void buttonCancel() {
      bIsCancel = true;
      hide();
    }

    /**
     *  Description of the Method
     */
    public void buttonOK() {
      bIsCancel = false;
      hide();
    }
  }
  public final static String MCKOI_DRIVER="com.mckoi.JDBCDriver";
  public final static String[] createTables = {
      "SELECT * FROM Activity WHERE CompId=99999",
      "create table Activity " +
      "(CompId INTEGER NOT NULL, " +
      "Act_id INTEGER NOT NULL, " +
      "RegDate DATE, " +
      "InvDate DATE, " +
      "Notes VARCHAR(200), " +
      "FileInfo VARCHAR(200), " +
      "PRIMARY KEY(Act_id,CompId))",
      "SELECT * FROM Amount WHERE CompId=99999",
      "create table Amount " +
      "(CompId INTEGER NOT NULL, " +
      "Act_id INTEGER NOT NULL, " +
      "Account INTEGER NOT NULL, " +
      "Amount DOUBLE, " +
      "IsDebit INTEGER, " +
      "Customer INTEGER, " +
      "Notes VARCHAR(40)) " ,
      "SELECT * FROM Account WHERE CompId=99999",
      "create table Account " +
      "(CompId int NOT NULL, " +
      "Account int NOT NULL, " +
      "AccDesc VARCHAR(255), " +
      "IsAsset int, " +
      "PRIMARY KEY(Account,CompId))",
      "SELECT * FROM Customer WHERE CompId=99999",
      "create table Customer " +
      "(CompId int NOT NULL, " +
      "CustId int NOT NULL, " +
      "CustName VARCHAR(100), " +
      "CustDesc VARCHAR(200), " +
      "PRIMARY KEY (CustId,CompId))",
      "SELECT * FROM UniqNum WHERE CompId=99999",
      "create table UniqNum  " +
      "(CompId int NOT NULL, " +
      "UniqName varchar(50) NOT NULL, " +
      "LastNumber int, " +
      "PRIMARY KEY (UniqName,CompId))",
      "SELECT * FROM AccountingPeriods WHERE CompId=99999",
      "create table AccountingPeriods  " +
      "(CompId int NOT NULL, " +
      "StartPeriod date NOT NULL, " +
      "EndPeriod date NOT NULL, " +
      "PRIMARY KEY (CompId,StartPeriod,EndPeriod))",
      "SELECT * FROM Company WHERE CompId=99999",
      "create table Company " +
      "(CompId int NOT NULL, " +
      "Name VARCHAR(200), " +
      "PRIMARY KEY (CompId))"};
      /*
  public final static String[] createTables = {
      "SELECT * FROM Activity WHERE CompId=99999",
      "create table Activity " +
      "(CompId INTEGER NOT NULL, " +
      "Act_id INTEGER NOT NULL, " +
      "RegDate DATE, " +
      "InvDate DATE, " +
      "Notes BINARY(200), " +
      "FileInfo BINARY(200), " +
      "PRIMARY KEY(Act_id,CompId))",
      "SELECT * FROM Amount WHERE CompId=99999",
      "create table Amount " +
      "(CompId INTEGER NOT NULL, " +
      "Act_id INTEGER NOT NULL, " +
      "Account INTEGER NOT NULL, " +
      "Amount DOUBLE, " +
      "IsDebit INTEGER, " +
      "Customer INTEGER, " +
      "Notes BINARY(40), " +
      "INDEX(Act_id,Account,CompId))",
      "SELECT * FROM Account WHERE CompId=99999",
      "create table Account " +
      "(CompId int NOT NULL, " +
      "Account int NOT NULL, " +
      "AccDesc varbinary(255), " +
      "IsAsset int, " +
      "PRIMARY KEY(Account,CompId))",
      "SELECT * FROM Customer WHERE CompId=99999",
      "create table Customer " +
      "(CompId int NOT NULL, " +
      "CustId int NOT NULL, " +
      "CustName varbinary(100), " +
      "CustDesc varbinary(200), " +
      "PRIMARY KEY (CustId,CompId))",
      "SELECT * FROM UniqNum WHERE CompId=99999",
      "create table UniqNum  " +
      "(CompId int NOT NULL, " +
      "UniqName varchar(50) NOT NULL, " +
      "LastNumber int, " +
      "PRIMARY KEY (UniqName,CompId))",
      "SELECT * FROM AccountingPeriods WHERE CompId=99999",
      "create table AccountingPeriods  " +
      "(CompId int NOT NULL, " +
      "StartPeriod date NOT NULL, " +
      "EndPeriod date NOT NULL, " +
      "PRIMARY KEY (CompId,StartPeriod,EndPeriod))",
      "SELECT * FROM Company WHERE CompId=99999",
      "create table Company " +
      "(CompId int NOT NULL, " +
      "Name varbinary(200), " +
      "PRIMARY KEY (CompId))"};
      */
}


