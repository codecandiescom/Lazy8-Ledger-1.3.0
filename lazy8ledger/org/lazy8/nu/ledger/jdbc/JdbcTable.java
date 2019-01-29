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
import java.util.Calendar;
import java.util.ArrayList;
import java.util.Collection;
import javax.swing.*;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.util.help.*;
import org.gjt.sp.util.*;

/**
 *  This class does all the manipulation required for any table in a JDBC
 *  database
 *
 *@author     Thomas Dilts
 *@created    den 5 mars 2002
 *@version    1.0
 */
public class JdbcTable {
  /**
   *  Constructor for JdbcTable. This constructor is used when this class will
   *  represent a combination of tables given in the sSqlStringin input
   *  arguement. If this constructor is used, only functions {@link
   *  #loadVariables},{@link #GetFirstRecord}, {@link #GetNextRecord}, {@link
   *  #getObject} and {@link #setObject} are valid.
   *
   *@param  sSqlStringin  An SQL statement that this class will represent.
   */
  public JdbcTable(String sSqlStringin,JFrame view) {
    this.view=view;
    sSqlString = sSqlStringin;
    sTableName = "";
    objFields = new Object[100];
    sObjNames = new String[100];
    sFieldTypes = new int[100];
  }

  /**
   *  Constructor for JdbcTable. This constructor is used when this class will
   *  represent just one actual JDBC table.
   *
   *@param  sTableNamein     The JDBC name of table that this class will
   *      represent.
   *@param  iNumberOfKeysin  This is the number of keys in the table. These keys
   *      must be the first keys in the table definition! That means that when
   *      the table is created in the database, these keys must be the first
   *      fields defined in the create statement.
   */
  public JdbcTable(String sTableNamein, int iNumberOfKeysin,JFrame view) {
    this.view=view;
    sSqlString = "";
    //not an SQL statement but a real table
    sTableName = sTableNamein;
    iNumberOfKeys = iNumberOfKeysin;
    //try to open the table to get the field names and types
    try {
      ResultSetMetaData rsmd = getTablesMetaData();
      if (rsmd==null)return;
      int iNumColumns = rsmd.getColumnCount();
      objFields = new Object[iNumColumns + iNumberOfKeys];
      sObjNames = new String[iNumColumns + iNumberOfKeys];
      sFieldNames = new String[iNumColumns];
      sFieldTypes = new int[iNumColumns];
      for (int i = 1; i <= iNumColumns; i++) {
        sFieldNames[i - 1] = rsmd.getColumnName(i);
        sFieldTypes[i - 1] = rsmd.getColumnType(i);
      }
    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("Could not open the table, Error:" + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   *  This will get you all the information about the columns in the table by
   *  returning a ResultSetMetaData object.
   *
   *@return                            All the information about the columns in
   *      the table in a ResultSetMetaData object.
   *@exception  java.sql.SQLException  Description of the Exception
   */
  public ResultSetMetaData getTablesMetaData()
    throws java.sql.SQLException {

    ResultSet rs;
    ResultSetMetaData rsmd;
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade)return null;
    PreparedStatement pps = dc.
        con.prepareStatement("SELECT * FROM " + sTableName);
    rs = pps.executeQuery();
    rsmd = rs.getMetaData();
    return rsmd;
  }

  /**
   *  Returns the Field type for the given field name
   *
   *@param  sFieldName  The name of the field to return
   *@return             The the Field type for the given field name.
   */
  private int getTypeFromFieldName(String sFieldName) {
    int i = 0;
    for (i = 0; i < sFieldNames.length && sFieldNames[i].compareTo(sFieldName) != 0; i++)
      if (i == sFieldNames.length) {
        SystemLog.ErrorPrintln("Could not find field " + sFieldName + " in table " + sTableName);
        return -1;
      }
    return sFieldTypes[i];
  }

  /**
   *  This function can only be called after {@link #setObject } has been called
   *  for each field that is to be populated. Then after calling this function,
   *  a new record will be added to the table
   *
   *@return    Description of the Return Value
   */
  public boolean AddRecord() {
    //build up the select string
    String sABunchOfQuestionMarks = "";
    String sFields = "";
    for (int i = 0; i < iNumberOfFieldValues; i++) {
      sFields = sFields + sObjNames[i];
      sABunchOfQuestionMarks = sABunchOfQuestionMarks + "?";
      if (i != (iNumberOfFieldValues - 1)) {
        sABunchOfQuestionMarks = sABunchOfQuestionMarks + ",";
        sFields = sFields + ",";
      }
    }
    String sAddString = "INSERT INTO " + sTableName + " (" + sFields + ") VALUES (" +
        sABunchOfQuestionMarks + ")";
    try {
      loadVariables(sAddString).executeUpdate();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(view, e.getMessage(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      SystemLog.ProblemPrintln("Error:" + e.getMessage() + " while adding with sql:" + sAddString);
      e.printStackTrace();
      return false;
    }

    isDatabaseChanged=true;
    return true;
  }

  /**
   *  This function can only be called after {@link #setObject } has been called
   *  for each field that is to the selection criteria for the delete. Then
   *  after calling this function, the record will be deleted from the table
   *
   *@return    Description of the Return Value
   */
  public boolean DeleteRecord() {
    //build up the delete string
    String sDeleteString = "DELETE FROM " + sTableName + " WHERE ";
    for (int i = 0; i < iNumberOfFieldValues; i++) {
      sDeleteString = sDeleteString + sObjNames[i] + " = ? ";
      if (i != (iNumberOfFieldValues - 1))
        sDeleteString = sDeleteString + " AND ";
    }
    try {
      loadVariables(sDeleteString).executeUpdate();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(view, e.getMessage(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      SystemLog.ProblemPrintln("Error:" + e.getMessage() + " Sql delete string=" + sDeleteString);
      e.printStackTrace();
      return false;
    }
    isDatabaseChanged=true;
    return true;
  }

  /**
   *  This function can only be called after {@link #setObject } has been called
   *  for each field that is to be changed plus the keys. The keys must be added
   *  first with the {@link #setObject } function before the other fields may be
   *  added. The key fields may not be changed in this function or anywhere in
   *  this class. If you need to change the keys, then delete the record first
   *  and re-add it with the new keys. Then after calling this function, the
   *  record in the table will be changed
   *
   *@return    Description of the Return Value
   */
  public boolean ChangeRecord() {
    String sChangeString = "UPDATE " + sTableName + " SET ";
    for (int i = 0; i < iNumberOfFieldValues; i++) {
      sChangeString = sChangeString + sObjNames[i] + " = ? ";
      if (i != (iNumberOfFieldValues - 1))
        sChangeString = sChangeString + " , ";
    }
    sChangeString += " WHERE ";
    //get one more time the keys
    for (int i = 0; i < iNumberOfKeys; i++) {
      sChangeString = sChangeString + sObjNames[i] + " = ? ";
      if (i != (iNumberOfKeys - 1))
        sChangeString = sChangeString + " AND ";
      //add again the keys for the loadVariables function
      setObject(objFields[i], sObjNames[i]);
    }
    try {
      loadVariables(sChangeString).executeUpdate();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(view, e.getMessage(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      SystemLog.ProblemPrintln("Error:" + e.getMessage() + " while adding with sql:" + sChangeString);
      e.printStackTrace();
      return false;
    }
    isDatabaseChanged=true;
    return true;
  }

  /**
   *  This function can only be called after {@link #setObject } has been called
   *  for each field that is to be the selection criteria for getting the first
   *  record in the given subset of the table.
   */
  private void CreateResultSet() {
    String sSelectString;
    //if(sFieldNames==null)return;//true if unable to connect to the database
    if (sSqlString.length() != 0 && sTableName.length() == 0)
      sSelectString = sSqlString;

    else {
      //build up the selection string
      sSelectString = "SELECT * FROM " + sTableName + " ";
      if (iNumberOfFieldValues != 0)
        sSelectString = sSelectString + " WHERE ";
      for (int i = 0; i < iNumberOfFieldValues; i++) {
        sSelectString = sSelectString + sObjNames[i] + " = ? ";
        if (i != (iNumberOfFieldValues - 1))
          sSelectString = sSelectString + " AND ";
      }
      //put all the fields in the order by question
      sSelectString = sSelectString + " ORDER BY ";
      for (int i = 0; i < sFieldNames.length; i++) {
        sSelectString = sSelectString + sFieldNames[i];
        if (i != (sFieldNames.length - 1))
          sSelectString = sSelectString + " , ";
      }
    }
    try {
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
        resultSetMetaData = null;
      }
      resultSet = loadVariables(sSelectString).executeQuery();
      resultSetMetaData = resultSet.getMetaData();
      //if this is a given sql statement, then we must load this
      //information after the fact....
      if (sSqlString.length() != 0 && sTableName.length() == 0) {
        ResultSetMetaData rsmd = resultSetMetaData;
        int iNumColumns = rsmd.getColumnCount();
        objFields = new Object[iNumColumns + iNumberOfKeys];
        sObjNames = new String[iNumColumns + iNumberOfKeys];
        sFieldNames = new String[iNumColumns];
        sFieldTypes = new int[iNumColumns];
        for (int i = 1; i <= iNumColumns; i++) {
          sFieldNames[i - 1] = rsmd.getColumnName(i);
          sFieldTypes[i - 1] = rsmd.getColumnType(i);
        }

      }
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage() + " Sql select string=" + sSelectString);
      e.printStackTrace();
    }
  }

  /**
   *  This function can only be called after {@link #setObject } has been called
   *  for each field that is to be the selection criteria for getting the first
   *  record in the given subset of the table.
   *
   *@return    Description of the Return Value
   */
  public boolean GetFirstRecord() {
    CreateResultSet();
    return GetNextRecord();
  }

  /**
   *  This function can only be called after {@link #setObject } has been called
   *  for each field that is to be the selection criteria for getting the first
   *  record in the given subset of the table.
   *
   *@param  startRecord  Which record in the table to start with
   *@param  numRecords   How many records to get starting at the startRecord
   *@return              Collection of Collections of all the records in the
   *      given criteria
   */
  public Collection GetRecords(int startRecord, int numRecords) {
    CreateResultSet();
    ArrayList al = new ArrayList();
    try {
      if (resultSet != null) {
        resultSet.absolute(startRecord);
        while (resultSet.next()) {
          ArrayList alRow = new ArrayList();
          al.add(alRow);
          for (int i = 1; i <= sFieldNames.length; i++)
            alRow.add(getObject(i, new IntHolder()));
        }
        resultSet.close();
      }
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage() + " GetRecords");
      e.printStackTrace();
    }
    return al;
  }

  /**
   *  This function can only be called after {@link FindRecordDialog } has been
   *  used. This function will take the information from the {@link
   *  FindRecordDialog } and create a result set.
   *
   *@param  Action            What action is to be done on each field
   *      (SEEK_NOTHING, SEEK_GREATER_THAN,SEEK_LESS_THAN,SEEK_EQUAL)
   *@param  sTextFieldNames   The field names of each field
   *@param  sTextFields       The value to seek on for each field
   *@param  sErrorMessage     Return error message if returning false(something
   *      went wrong)
   *@param  bShowErrorDialog  Should a dialog box be shown if there is an error
   *@return                   Description of the Return Value
   */
  public boolean GetFirstSeekRecord(int Action[], String sTextFieldNames[], String sTextFields[],
      StringBuffer sErrorMessage, boolean bShowErrorDialog) {
    //build up the selection string
    StringBuffer sb = new StringBuffer("SELECT * FROM " + sTableName + " WHERE ");
    int iNumOfFields = 0;
    for (int i = 0; i < Action.length; i++)
      if (Action[i] != SEEK_NOTHING) {
        iNumOfFields++;
        try {
          switch (getTypeFromFieldName(sTextFieldNames[i])) {
            case Types.VARCHAR:
              //non UNICODE text field
              setObject(new String(sTextFields[i]), sTextFieldNames[i]);
              break;
            case Types.INTEGER:
              setObject(new Integer(sTextFields[i]), sTextFieldNames[i]);
              break;
            case Types.DOUBLE:
              setObject(new Double(sTextFields[i]), sTextFieldNames[i]);
              break;
            case Types.DATE:
              try {
                setObject(new java.sql.Date(DateField.ConvertLocalizedStringToDate(
                    sTextFields[i]).getTime()),
                    sTextFieldNames[i]);
              }
              catch (Exception e) {
                sErrorMessage.setLength(0);
                sErrorMessage.append(Translator.getTranslation(
                    "Date must be in the following format") + " : " +
                    DateField.getTodaysDateString());
                if (bShowErrorDialog)
                  JOptionPane.showMessageDialog(view, sErrorMessage,
                      Translator.getTranslation("Update not entered"),
                      JOptionPane.PLAIN_MESSAGE);
                sErrorMessage.append(":Error:" + e.getMessage());
                SystemLog.ProblemPrintln(":Error:" + e.getMessage());
                e.printStackTrace();
                return false;
              }

              break;
            default:
              //this must be the blobs...Actually a UNICODE text
              setObject(StringBinaryConverter.StringToBinary(
                  sTextFields[i]), sTextFieldNames[i]);
              break;
          }
        }
        catch (Exception e) {
          SystemLog.ProblemPrintln("Error:" + e.getMessage() + " incomplete Sql select string=" + sb);
          sErrorMessage.setLength(0);
          sErrorMessage.append(":Error:" + e.getMessage() + " incomplete Sql select string=" + sb);
          e.printStackTrace();
          return false;
        }

        if (iNumOfFields != 1)
          sb.append(" AND ");
        switch (Action[i]) {
          case SEEK_GREATER_THAN:
            sb.append(sTextFieldNames[i] + " >= ? ");
            break;
          case SEEK_LESS_THAN:
            sb.append(sTextFieldNames[i] + " <= ? ");
            break;
          case SEEK_EQUAL:
            sb.append(sTextFieldNames[i] + " = ? ");
            break;
        }

      }

    if (iNumOfFields == 0)
      return false;
    //put all the fields in the order by question
    sb.append(" ORDER BY ");
    for (int i = 0; i < sFieldNames.length; i++) {
      sb.append(sFieldNames[i]);
      if (i != (sFieldNames.length - 1))
        sb.append(" , ");
    }

    try {
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
        resultSetMetaData = null;
      }
      resultSet = loadVariables(sb.toString()).executeQuery();
      resultSetMetaData = resultSet.getMetaData();
    }
    catch (Exception e) {
      sErrorMessage.setLength(0);
      sErrorMessage.append(":Error:" + e.getMessage() + " Sql select string=" + sb);
      SystemLog.ProblemPrintln("Error:" + e.getMessage() + " Sql select string=" + sb);
      e.printStackTrace();
      return false;
    }
    return GetNextRecord();
  }

  /**
   *  This function can only be called after {@link #GetOneRecord } or {@link
   *  #GetFirstRecord } or {@link #GetFirstSeekRecord } or {@link #GetNextRecord
   *  } has been called successfully. Then after calling this function, the next
   *  record in the table will be active.
   *
   *@return    Description of the Return Value
   */
  public boolean GetNextRecord() {
    if (resultSet == null)
      return false;
    try {
      return (resultSet.next());
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
      e.printStackTrace();
      return false;
    }
  }

  /**
   *  Returns a prepared statement which can then be executed to get a result
   *  table. This function will replace all the question marks in the given SQL
   *  string with real values that were previously given by the {@link
   *  #setObject } function.
   *
   *@param  stringSql                The SQL string that usually has some
   *      question marks in it!!
   *@return                          prepared statement which can then be
   *      executed to get a result table.
   *@exception  SQLException         Description of the Exception
   *@exception  java.io.IOException  Description of the Exception
   */
  protected PreparedStatement loadVariables(String stringSql)
    throws SQLException, java.io.IOException {
    int iTmpNumberOfFieldValues = iNumberOfFieldValues;
    iNumberOfFieldValues = 0;
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
       throw(new java.io.IOException("Unable to open the database"));
    }
    PreparedStatement updateTable =
        dc.con.prepareStatement(stringSql);
    for (int i = 0; i < iTmpNumberOfFieldValues; i++) {
      int iType = 0;
      if (sSqlString.length() != 0 && sTableName.length() == 0)
        iType = sFieldTypes[i];
      else
        iType = getTypeFromFieldName(sObjNames[i]);
      switch (iType) {
        case Types.VARCHAR:
          //non UNICODE text field
          updateTable.setString(i + 1, (String) objFields[i]);
          break;
        case Types.INTEGER:
          updateTable.setInt(i + 1, ((Integer) objFields[i]).intValue());
          break;
        case Types.DOUBLE:
          updateTable.setDouble(i + 1, ((Double) objFields[i]).doubleValue());
          break;
        case Types.DATE:
          updateTable.setDate(i + 1, (java.sql.Date) objFields[i]);
          break;
        default:
          //this must be the blobs...Actually a UNICODE text
          updateTable.setBytes(i + 1,
              StringBinaryConverter.StringToBinary((String) objFields[i]));
          break;
      }
    }
    return updateTable;
  }

  /**
   *  Returns the result field value from the last search or movement in the
   *  table
   *
   *@param  iFieldNumber   The number of the field to return
   *@param  iFieldTypeOut  The field type {@link java.sql.Types} of the
   *      requested field. This is an output parameter. If this is null then
   *      nothing will be returned in this field
   *@return                The value in the table of the given field.
   */
  public Object getObject(int iFieldNumber, IntHolder iFieldTypeOut) {
    try {
      if (iFieldTypeOut != null)
        iFieldTypeOut.iValue = resultSetMetaData.getColumnType(iFieldNumber);
      switch (resultSetMetaData.getColumnType(iFieldNumber)) {
        case Types.VARCHAR:
          //non UNICODE text field
          return new String(resultSet.getString(iFieldNumber));
        case Types.INTEGER:
          return new Integer(resultSet.getInt(iFieldNumber));
        case Types.NUMERIC:
        case Types.REAL:
        case Types.FLOAT:
        case Types.DECIMAL:
        case Types.DOUBLE:
          return new Double(resultSet.getDouble(iFieldNumber));
        case Types.DATE:
          return resultSet.getDate(iFieldNumber);
        default:
//          return new String(resultSet.getString(iFieldNumber));
          //this must be the blobs...Actually a UNICODE text
          return StringBinaryConverter.BinaryToString(
              resultSet.getBytes(iFieldNumber));
      }
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  /**
   *  Returns the result field value from the last search or movement in the
   *  table
   *
   *@param  sFieldNameIn   The name of the field to return
   *@param  iFieldTypeOut  The field type {@link java.sql.Types} of the
   *      requested field. This is an output parameter. If this is null then
   *      nothing will be returned in this field
   *@return                The value in the table of the given field.
   */
  public Object getObject(String sFieldNameIn, IntHolder iFieldTypeOut) {
    try {
      return getObject(resultSet.findColumn(sFieldNameIn), iFieldTypeOut);
    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  /**
   *  This must be called for each variable in an SQL statement if this class
   *  represents only one table, see the constructor for details.
   *
   *@param  objIn       The value of the variable in the soon to be created SQL
   *      statement
   *@param  sFieldName  The field name for which the above variable value will
   *      be used.
   */
  public void setObject(Object objIn, String sFieldName) {
    sObjNames[iNumberOfFieldValues] = sFieldName;
    objFields[iNumberOfFieldValues++] = objIn;
  }

  /**
   *  This must be called for each variable in an SQL statement if this class
   *  represents an SQL statement and not just one table, see the constructor
   *  for details.
   *
   *@param  objIn      The value of the variable in the soon to be created SQL
   *      statement
   *@param  iDataType  The new object value
   */
  public void setObject(Object objIn, int iDataType) {
    sFieldTypes[iNumberOfFieldValues] = iDataType;
    objFields[iNumberOfFieldValues++] = objIn;
  }

  /**
   *  The name of the table that this class will manipulate. Given value in the
   *  constructor
   */
  protected String sTableName;
  /**
   *  The result set which is valid only after a successful search
   */
  protected ResultSet resultSet;
  /**
   *  Information about the columns in a {@link #resultSet}.
   */
  ResultSetMetaData resultSetMetaData;
  /**
   *  Values for variables to be used in an SQL statement
   */
  protected Object objFields[];
  /**
   *  Field names of the corresponding values for {@link #objFields}
   */
  protected String sObjNames[];
  /**
   *  Names of all the fields in the {@link #sTableName}
   */
  protected String sFieldNames[];
  /**
   *  Types of all the fields in the {@link #sTableName}
   */
  protected int sFieldTypes[];
  /**
   *  Number of times {@link #objFields} was called since the last search, that
   *  is, the number of variables in the next SQL statement
   */
  protected int iNumberOfFieldValues = 0;
  /**
   *  Number of unique keys in {@link #sTableName}. Given value in the
   *  constructor
   */
  protected int iNumberOfKeys = 0;
  /**
   *  Description of the Field
   */
  protected String sSqlString;

  /**
   *  Used by the {@link FindRecordDialog} class to define that there will be no
   *  comparison done on that particular field
   */
  public final static int SEEK_NOTHING = 0;
  /**
   *  Used by the {@link FindRecordDialog} class to define that the field must
   *  be greater than or equal to the given value
   */
  public final static int SEEK_GREATER_THAN = 1;
  /**
   *  Used by the {@link FindRecordDialog} class to define that the field must
   *  be less than or equal to the given value
   */
  public final static int SEEK_LESS_THAN = 2;
  /**
   *  Used by the {@link FindRecordDialog} class to define that the field must
   *  be equal to the given value
   */
  public final static int SEEK_EQUAL = 3;
  public static boolean isDatabaseChanged=false;
  private JFrame view;
}


