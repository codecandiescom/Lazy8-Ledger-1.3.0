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

import java.io.*;
import java.sql.*;
import java.awt.event.*;
import java.text.DateFormat;
import java.util.Calendar;
import javax.swing.*;
import java.awt.*;
import java.util.zip.*;
import java.security.*; 
import javax.crypto.*; 
import javax.crypto.spec.*; 
import org.lazy8.nu.ledger.forms.CompanyComponents;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.util.help.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DatabaseBackup {
  Connection con;
  final static String filedescriptor = "Lazy8LedgerBackupVersionNumber";
  final static String Companyfiledescriptor =
      "Lazy8LedgerCompanyBackupVersionNumber";
  final static double dVersionNumber = 1.0;

  /**
   *  Constructor for the DatabaseBackup object
   *
   *@param  conin  Description of the Parameter
   */
  public DatabaseBackup(Connection conin) {
    con = conin;
  }

  /**
   *  Description of the Class
   *
   *@author     Administrator
   *@created    den 5 mars 2002
   */
  public class CompanyDialog extends JDialog {
    boolean bIsCancel = false;
    CompanyComponents cc;

    /**
     *  Constructor for the CompanyDialog object
     *
     *@param  frame  Description of the Parameter
     *@param  sText  Description of the Parameter
     */
    public CompanyDialog(javax.swing.JFrame frame, String sText) {
      super(frame, "", true);

      getContentPane().setLayout(new GridLayout(1, 1));
      JPanel jp1 = new JPanel();
      jp1.setLayout(new GridLayout(4, 2));
      getContentPane().add(jp1);
      jp1.add(new JPanel());
      jp1.add(new JPanel());
      cc = new CompanyComponents(jp1, sText, true, "",null);
      jp1.add(new JPanel());
      jp1.add(new JPanel());

      JButton button3 = new JButton(Translator.getTranslation("OK"));
      button3.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            buttonOK();
          }
        }
          );
      jp1.add(button3);
      JButton button4 = new JButton(Translator.getTranslation("Cancel"));
      button4.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            buttonCancel();
          }
        }
          );
      jp1.add(button4);

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

  /**
   *  Description of the Method
   *
   *@param  doOut                      Description of the Parameter
   *@param  sTableName                 Description of the Parameter
   *@param  bBackupOneCompanyOnly      Description of the Parameter
   *@param  iCompanyToBackup           Description of the Parameter
   *@exception  java.io.IOException    Description of the Exception
   *@exception  java.sql.SQLException  Description of the Exception
   */
  public void SaveTable(DataOutputStream doOut, String sTableName,
      boolean bBackupOneCompanyOnly, Integer iCompanyToBackup)
    throws java.io.IOException, java.sql.SQLException {
    //save tablename
    //save number of fields
    //save Fieldname,fieldtype,fieldsize
    //save number of rows
    //save each row

    //we will catch ourselvs those errors that we can possibly recover
    // from.  other errors we let the calling funtion handle

    //save tablename
    doOut.writeUTF(sTableName);

    //try to open the table to get the field names and types
    ResultSet rs;
    ResultSetMetaData rsmd;
    try {

      String sSelect = new String("SELECT * FROM " + sTableName);
      if (bBackupOneCompanyOnly)
        sSelect = sSelect + " WHERE CompId=" + iCompanyToBackup;
      PreparedStatement pps = con.prepareStatement(sSelect);
      rs = pps.executeQuery();
      rsmd = rs.getMetaData();
    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("Could not open the table, Error:" + e.getMessage());
      //write out that there are no colums and get out
      doOut.writeInt(0);
      return;
    }

    //save number of fields
    doOut.writeInt(rsmd.getColumnCount());

    //save Fieldname,fieldtype,fieldsize
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      doOut.writeUTF(rsmd.getColumnName(i));
      doOut.writeInt(rsmd.getColumnDisplaySize(i));
      //not really needed
      doOut.writeInt(rsmd.getColumnType(i));
    }

    //save number of rows
    //we need to count them first
    int iNumOfRows = 0;
    while (rs.next())
      iNumOfRows++;
    doOut.writeInt(iNumOfRows);

    //save each row
    rs.beforeFirst();
    //position before the first record
    int j = 0;
    while (rs.next())
      for (int i = 1; i <= rsmd.getColumnCount(); i++)
        switch (rsmd.getColumnType(i)) {
          case Types.VARCHAR:
            doOut.writeUTF(rs.getString(i));
            break;
          case Types.INTEGER:
            doOut.writeInt(rs.getInt(i));
            break;
          case Types.DOUBLE:
            doOut.writeDouble(rs.getDouble(i));
            break;
          case Types.DATE:
            doOut.writeLong(rs.getDate(i).getTime());
            break;
          default:
            //this must be the blobs.....
            doOut.writeUTF(StringBinaryConverter.BinaryToString(
                rs.getBytes(i)));
        }

  }

  /**
   *  Description of the Method
   *
   *@param  din                        Description of the Parameter
   *@param  bIsTestOnly                Description of the Parameter
   *@param  bRestoreOneCompanyOnly     Description of the Parameter
   *@param  iCompanyToResoreTo         Description of the Parameter
   *@exception  java.io.IOException    Description of the Exception
   *@exception  java.sql.SQLException  Description of the Exception
   */
  public void RestoreTableForOldShit(DataInputStream din, boolean bIsTestOnly,
      boolean bRestoreOneCompanyOnly, Integer iCompanyToResoreTo)
    throws java.io.IOException, java.sql.SQLException {
    int iCompIdFieldNumber = 0;
    //get tablename
    //get number of fields
    //get Fieldname,fieldtype,fieldsize
    //if all is well, remove all records  in the existing table.
    //get number of rows
    //get each row
    String sTableName = din.readUTF();

    int iNumFields = din.readInt();
    if (iNumFields == 0)
      return;
    String[] FieldNames = new String[iNumFields];
    int[] FieldTypes = new int[iNumFields];
    int[] FieldSizes = new int[iNumFields];
    for (int i = 0; i < iNumFields; i++) {
      FieldNames[i] = din.readUTF();
      if (FieldNames[i].compareTo("CompId") == 0)
        iCompIdFieldNumber = i;
      FieldSizes[i] = din.readInt();
      //not really needed
      FieldTypes[i] = din.readInt();
    }

    if (bIsTestOnly) {
      //now check if the table exists and that the fields are the same
      //try to open the table to get the field names and types
      ResultSet rs;
      ResultSetMetaData rsmd;
      /*
       *  PreparedStatement pps =con.
       *  prepareStatement("SELECT * FROM "+sTableName);
       *  rs=pps.executeQuery();
       *  rsmd=rs.getMetaData();
       *  for (int i=1;i<=rsmd.getColumnCount();i++)
       *  {
       *  if(FieldNames[i-1].compareTo(rsmd.getColumnName(i))!=0
       *  || FieldSizes[i-1] != rsmd.getColumnDisplaySize(i)
       *  || FieldTypes[i-1] != rsmd.getColumnType(i))
       *  throw new java.io.IOException("Field "+FieldNames[i-1]+
       *  " does not match to field "+rsmd.getColumnName(i)+
       *  " in table "+sTableName);
       *  }
       */
    }
    else {
      //remove all records  in the existing table.
      String sDelete = new String("DELETE FROM " + sTableName);
      if (bRestoreOneCompanyOnly && iCompanyToResoreTo.intValue() != 0)
        sDelete = sDelete + " WHERE CompId=" + iCompanyToResoreTo;
      //also, find which field is the CompId

      if (!bRestoreOneCompanyOnly || iCompanyToResoreTo.intValue() != 0) {
        PreparedStatement pps =
            con.prepareStatement(sDelete);
        int iNumDeletedRows = pps.executeUpdate();
      }
    }

    //get number of rows
    int iNumRows = din.readInt();

    //create the insert string
    String sInsert = "INSERT INTO " + sTableName + " SET ";
    for (int i = 0; i < iNumFields; i++) {

      sInsert = sInsert + FieldNames[i] + "=?";

      if (i != (iNumFields - 1))
        sInsert = sInsert + " , ";
      else
        sInsert = sInsert + "";
    }
    SystemLog.ErrorPrintln("query: " + sInsert);
    //get each row
    for (int j = 0; j < iNumRows; j++) {
      System.err.print("row" + j);
      PreparedStatement pps =
          con.prepareStatement(sInsert);
      for (int i = 1; i <= iNumFields; i++)
        switch (FieldTypes[i - 1]) {
          case Types.VARCHAR:
            pps.setBytes(i, StringBinaryConverter.StringToBinary(
                din.readUTF()));
            //          pps.setString(i,din.readUTF());
            break;
          case Types.INTEGER:
            int iValue = din.readInt();
            if (bRestoreOneCompanyOnly && iCompanyToResoreTo.intValue() != 0
                 && (i - 1) == iCompIdFieldNumber)
              iValue = iCompanyToResoreTo.intValue();
            pps.setInt(i, iValue);
            break;
          case Types.DOUBLE:
            pps.setDouble(i, din.readDouble());
            break;
          case Types.DATE:
            pps.setDate(i, new java.sql.Date(din.readLong()));
            break;
          default:
            //this must be the blobs.....
            pps.setBytes(i, StringBinaryConverter.StringToBinary(
                din.readUTF()));
        }

      if (!bIsTestOnly)
        pps.executeUpdate();
    }
    SystemLog.ErrorPrintln("all rows completed successfully");

  }

  /**
   *  Description of the Method
   *
   *@param  din                        Description of the Parameter
   *@param  bIsTestOnly                Description of the Parameter
   *@param  bRestoreOneCompanyOnly     Description of the Parameter
   *@param  iCompanyToResoreTo         Description of the Parameter
   *@exception  java.io.IOException    Description of the Exception
   *@exception  java.sql.SQLException  Description of the Exception
   */
  public void RestoreTable(DataInputStream din, boolean bIsTestOnly,
      boolean bRestoreOneCompanyOnly, Integer iCompanyToResoreTo)
    throws java.io.IOException, java.sql.SQLException {
    int iCompIdFieldNumber = 0;
    //get tablename
    //get number of fields
    //get Fieldname,fieldtype,fieldsize
    //if all is well, remove all records  in the existing table.
    //get number of rows
    //get each row
    String sTableName = din.readUTF();
    int iNumFields = din.readInt();
    if (iNumFields == 0)
      return;
    String[] FieldNames = new String[iNumFields];
    int[] FieldTypes = new int[iNumFields];
    int[] FieldSizes = new int[iNumFields];
    for (int i = 0; i < iNumFields; i++) {
      FieldNames[i] = din.readUTF();
      if (FieldNames[i].compareTo("CompId") == 0)
        iCompIdFieldNumber = i;
      FieldSizes[i] = din.readInt();
      //not really needed
      FieldTypes[i] = din.readInt();
    }

    if (bIsTestOnly) {
      //now check if the table exists and that the fields are the same
      //try to open the table to get the field names and types
      ResultSet rs;
      ResultSetMetaData rsmd;
      PreparedStatement pps =
          con.prepareStatement("SELECT * FROM " + sTableName);
      rs = pps.executeQuery();
      rsmd = rs.getMetaData();

      for (int j = 0; j < iNumFields; j++) {
        int i;
        for (i = 1; i <= rsmd.getColumnCount(); i++)
          if (FieldNames[j].compareTo(rsmd.getColumnName(i)) == 0
          // || FieldSizes[i-1] != rsmd.getColumnDisplaySize(i)
          // || FieldTypes[i-1] != rsmd.getColumnType(i)
              )
            break;
        if (i > rsmd.getColumnCount())
          throw new java.io.IOException("Field " + FieldNames[j] +
              " is not in table " + sTableName);
      }
    }
    else {
      //remove all records  in the existing table.
      String sDelete = new String("DELETE FROM " + sTableName);
      if (bRestoreOneCompanyOnly && iCompanyToResoreTo.intValue() != 0)
        sDelete = sDelete + " WHERE CompId=" + iCompanyToResoreTo;
      //also, find which field is the CompId

      if (!bRestoreOneCompanyOnly || iCompanyToResoreTo.intValue() != 0) {
        PreparedStatement pps =
            con.prepareStatement(sDelete);
        int iNumDeletedRows = pps.executeUpdate();
      }
    }

    //get number of rows
    int iNumRows = din.readInt();

    //create the insert string
    String sABunchOfQuestionMarks = "";
    String sFields = "";
    for (int i = 0; i < iNumFields; i++) {
      sFields = sFields + FieldNames[i];
      sABunchOfQuestionMarks = sABunchOfQuestionMarks + "?";
      if (i != (iNumFields - 1)) {
        sABunchOfQuestionMarks = sABunchOfQuestionMarks + ",";
        sFields = sFields + ",";
      }
    }
    String sInsert = "INSERT INTO " + sTableName + " (" + sFields + ") VALUES (" +
        sABunchOfQuestionMarks + ")";

    //get each row
    for (int j = 0; j < iNumRows; j++) {
      PreparedStatement pps =
          con.prepareStatement(sInsert);
      for (int i = 1; i <= iNumFields; i++)
        switch (FieldTypes[i - 1]) {
          case Types.VARCHAR:
            pps.setString(i, din.readUTF());
            break;
          case Types.INTEGER:
            int iValue = din.readInt();
            if (bRestoreOneCompanyOnly && iCompanyToResoreTo.intValue() != 0
                 && (i - 1) == iCompIdFieldNumber)
              iValue = iCompanyToResoreTo.intValue();
            pps.setInt(i, iValue);
            break;
          case Types.DOUBLE:
            pps.setDouble(i, din.readDouble());
            break;
          case Types.DATE:
            pps.setDate(i, new java.sql.Date(din.readLong()));
            break;
          default:
            //this must be the blobs.....
            pps.setBytes(i, StringBinaryConverter.StringToBinary(
                din.readUTF()));
        }

      if (!bIsTestOnly)
        pps.executeUpdate();
    }

  }

  /**
   *  Description of the Method
   *
   *@param  bRestoreOneCompanyOnly  Description of the Parameter
   *@param  fileName                Description of the Parameter
   *@param  iNewCompany             Description of the Parameter
   */
  public void RestoreDatabase(boolean bRestoreOneCompanyOnly, String fileName, int iNewCompany) {
    /*
     *  get file name
     *  confirm good file
     *  First read the entire file, checking that it is ok,
     *  then read it for real.....
     *  open gziped data stream
     *  input in DataInputStream format
     *  control version information string, a kind of file identifier
     *  control version double
     *  get  the number of tables
     *  call restoretable for each table to save each tables information
     *  close streams
     */
    //make sure they really want to do this
    Integer iCompanyToResoreTo = new Integer(0);
    if (bRestoreOneCompanyOnly) {
      if (fileName == null) {
        if (JOptionPane.CANCEL_OPTION != JOptionPane.showConfirmDialog(
            null,
            Translator.getTranslation("Do you want to change the name of the company you are restoring?"),
            "",
            JOptionPane.OK_CANCEL_OPTION,
            JOptionPane.QUESTION_MESSAGE)) {
          CompanyDialog cd = new CompanyDialog(null,
              Translator.getTranslation("Choose the company to restore to"));
          if (cd.bIsCancel)
            return;
          iCompanyToResoreTo = (Integer) cd.cc.comboBox.getSelectedItemsKey();
        }
      }
      else
        iCompanyToResoreTo = new Integer(iNewCompany);
    }
    else {
      if (JOptionPane.CANCEL_OPTION == JOptionPane.showConfirmDialog(
          null,
          Translator.getTranslation("This will delete all data presently in the database. Continue?"),
          "",
          JOptionPane.OK_CANCEL_OPTION,
          JOptionPane.QUESTION_MESSAGE))
        return;

      if (JOptionPane.CANCEL_OPTION == JOptionPane.showConfirmDialog(
          null,
          Translator.getTranslation("Are you sure you want to delete it?"),
          "",
          JOptionPane.OK_CANCEL_OPTION,
          JOptionPane.QUESTION_MESSAGE))
        return;
    }
    String sFile = null;
    if (fileName == null) {
      //Get file name
      JFileChooser fileDialog = new JFileChooser();
      fileDialog.setDialogTitle(
          Translator.getTranslation("Select a file name for the backup file"));
      if (fileDialog.showOpenDialog(null) !=
          JFileChooser.APPROVE_OPTION)
        return;
      sFile = fileDialog.getSelectedFile().getPath();
    }
    else
      sFile = fileName;
    //check if the file exists

    //this loop executes twice, first is just a test that the information
    //seems valid. The second is the real read

    WorkingDialog workDialog = new WorkingDialog(null);
    workDialog.show();
    workDialog.SetProgress(0);
    Cipher myCipher=null;
    if(SetupInfo.getBoolProperty(SetupInfo.REQUIRE_BACKUP_PASSWORD))
      myCipher=createCipher(Cipher.DECRYPT_MODE);
    for (int iTestFile = 0; iTestFile < 2; iTestFile++)
      try {
        InputStream in = new FileInputStream(sFile);
        GZIPInputStream gzipin = new GZIPInputStream(in);
        InputStream in2=gzipin;
        if(SetupInfo.getBoolProperty(SetupInfo.REQUIRE_BACKUP_PASSWORD))
            in2=new CipherInputStream(gzipin,myCipher);
        DataInputStream doIn = new DataInputStream(in2);

        //make sure file and descriptions are right
        String sFileDesc = doIn.readUTF();
        double dVersion = doIn.readDouble();
        if (bRestoreOneCompanyOnly) {
          if (Companyfiledescriptor.compareTo(sFileDesc) != 0
          /*
           *  || dVersion>=dVersionNumber
           */
              ) {
            SystemLog.ErrorPrintln("Not a valid file, descriptor =" +
                sFileDesc + " should equal " + Companyfiledescriptor +
                " and version number " + dVersion + " should equal " + dVersionNumber);
            JOptionPane.showMessageDialog(null,
                Translator.getTranslation("Invalid file name."),
                Translator.getTranslation("Update not entered"),
                JOptionPane.PLAIN_MESSAGE);
            workDialog.dispose();
            return;
          }
        }
        else
            if (filedescriptor.compareTo(sFileDesc) != 0
        /*
         *  || dVersion>=dVersionNumber
         */
            ) {
          SystemLog.ErrorPrintln("Not a valid file, descriptor =" +
              sFileDesc + " should equal " + filedescriptor +
              " and version number " + dVersion + " should equal " + dVersionNumber);
          JOptionPane.showMessageDialog(null,
              Translator.getTranslation("Invalid file name."),
              Translator.getTranslation("Update not entered"),
              JOptionPane.PLAIN_MESSAGE);
          workDialog.dispose();
          return;
        }

        //get  the number of tables
        int iNumTables = doIn.readInt();
        //call restoretable for each table to save each tables information
        boolean bIsTestOnly = true;
        if (iTestFile == 1)
          bIsTestOnly = false;
        for (int i = 0; i < iNumTables; i++) {
          RestoreTable(doIn, bIsTestOnly, bRestoreOneCompanyOnly,
              iCompanyToResoreTo);
          workDialog.SetProgress((iTestFile * iNumTables + i) * 100 / (2 * iNumTables));
        }
        doIn.close();
      }
      catch (Exception ee) {
        workDialog.dispose();
        //Cannot continue, the file does not exist
        JOptionPane.showMessageDialog(null,
            Translator.getTranslation("Invalid file name.") +
            " : " + ee.getMessage(),
            Translator.getTranslation("Update not entered"),
            JOptionPane.PLAIN_MESSAGE);
        SystemLog.ErrorPrintln("FAILED trying to backup: " +
            ee.getMessage());
        return;
      }

    workDialog.dispose();
  }

  /**
   *  Description of the Method
   *
   *@param  sTables                Description of the Parameter
   *@param  bBackupOneCompanyOnly  Description of the Parameter
   */
  public void BackupDatabase(String[] sTables, boolean bBackupOneCompanyOnly) {
    /*
     *  get file name
     *  confirm good file
     *  open gziped data stream
     *  output in DataOutputStream format
     *  write version information string, a kind of file identifier
     *  write version double
     *  write the number of tables
     *  call savetable for each table to save each tables information
     *  close streams
     */
    Integer iCompanyToBackup = new Integer(0);
    if (bBackupOneCompanyOnly) {
      CompanyDialog cd = new CompanyDialog(null,
          Translator.getTranslation("Choose the company to backup"));
      if (cd.bIsCancel)
        return;
      iCompanyToBackup = (Integer) cd.cc.comboBox.getSelectedItemsKey();
    }

    //Get file name
    JFileChooser fileDialog = new JFileChooser();
    fileDialog.setDialogTitle(
        Translator.getTranslation("Select a file name for the backup file"));
    if (fileDialog.showSaveDialog(null) !=
        JFileChooser.APPROVE_OPTION)
      return;
    //check if the file exists
    try {
      InputStream in = new FileInputStream(fileDialog.getSelectedFile());
      if (JOptionPane.CANCEL_OPTION == JOptionPane.showConfirmDialog(
          null,
          Translator.getTranslation("The file already exists. Continue?"),
          "",
          JOptionPane.OK_CANCEL_OPTION,
          JOptionPane.QUESTION_MESSAGE))
        return;
    }
    catch (Exception ee) {
      //all is well, the file does not exist
    }
    //now lets open the file for output

    WorkingDialog workDialog = new WorkingDialog(null);
    //open a gzipped file stream
    try {
      DataOutputStream doOut;
      OutputStream out = new FileOutputStream(fileDialog.getSelectedFile());
      GZIPOutputStream gzipout = new GZIPOutputStream(out);
      OutputStream out2=gzipout;
      if(SetupInfo.getBoolProperty(SetupInfo.REQUIRE_BACKUP_PASSWORD))
          out2=createEncryptedOutputStream(gzipout);
      doOut = new DataOutputStream(out2);
      //save version
      if (!bBackupOneCompanyOnly)
        doOut.writeUTF(filedescriptor);
      else
        doOut.writeUTF(Companyfiledescriptor);
      doOut.writeDouble(dVersionNumber);
      //save number of tables
      doOut.writeInt(sTables.length);
      //save each table
      workDialog.show();
      workDialog.SetProgress(0);
      for (int i = 0; i < sTables.length; i++) {
        SaveTable(doOut, sTables[i], bBackupOneCompanyOnly, iCompanyToBackup);
        workDialog.SetProgress((i * 100) / (sTables.length));
      }
      SystemLog.ErrorPrintln("before clossing work dialog");
      workDialog.dispose();
      SystemLog.ErrorPrintln("after clossing work dialog");

      doOut.close();
      SystemLog.ErrorPrintln("after closeing file");

    }
    catch (Exception e2) {
      workDialog.dispose();
      SystemLog.ErrorPrintln("FAILED trying to backup: " +
          e2.getMessage());
      JOptionPane.showMessageDialog(null,
          Translator.getTranslation("Unable to create file. Error") +
          " : " + e2.getMessage(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);

      //try to delete the file
      try {
        fileDialog.getSelectedFile().delete();
        return;
      }
      catch (Exception eee) {
      }
    }
    JdbcTable.isDatabaseChanged=false;
  }
    // Salt
  private static final byte[] salt = {
        (byte)0xc7, (byte)0x73, (byte)0x21, (byte)0x8c,
        (byte)0x7e, (byte)0xc8, (byte)0xee, (byte)0x99
    };

    // Iteration count
  private static final int itCount = 20;
  
  private static Cipher createCipher(int opmode){
    PBEKeySpec pbeKeySpec;
    PBEParameterSpec pbeParamSpec;
    SecretKeyFactory keyFac;

    // Create PBE parameter set
    pbeParamSpec = new PBEParameterSpec(salt, itCount);

    // Prompt user for encryption password.
    // Collect user password as char array (using the
    // "readPasswd" method from above), and convert
    // it into a SecretKey object, using a PBE key
    // factory.

      DataConnection.PasswordDialog pDialog = new DataConnection.PasswordDialog(null,false,opmode==Cipher.ENCRYPT_MODE);
      char[] dst=pDialog.jPass.getPassword();
      pDialog.dispose();
      
    pbeKeySpec = new PBEKeySpec(dst);
    Cipher pbeCipher=null;
    try{
      keyFac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
      SecretKey pbeKey = keyFac.generateSecret(pbeKeySpec);
  
      // Create PBE Cipher
      pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
  
      // Initialize PBE Cipher with key and parameters
      pbeCipher.init(opmode, pbeKey, pbeParamSpec);
    }catch(Exception e){
      SystemLog.ErrorPrintln("Could not create cipher, Error:" + e.getMessage());
      e.printStackTrace();
      
    }
    return pbeCipher;

  }
  public static OutputStream createEncryptedOutputStream( OutputStream out){
    return new CipherOutputStream(out,createCipher(Cipher.ENCRYPT_MODE));
  }
  public static InputStream createEncryptedInputStream( InputStream in){
    return new CipherInputStream(in,createCipher(Cipher.DECRYPT_MODE));
  }
}


