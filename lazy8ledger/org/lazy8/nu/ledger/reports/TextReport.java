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
package org.lazy8.nu.ledger.reports;

import java.awt.geom.*;
import java.awt.event.*;
import javax.swing.*;
import java.util.Vector;
import java.awt.*;
import java.text.*;
import java.io.*;
import java.awt.print.*;
import java.util.*;
import java.sql.*;
import java.awt.datatransfer.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.ledger.forms.*;
import org.lazy8.nu.ledger.main.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.jedit.msg.*;
import org.gjt.sp.jedit.gui.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class TextReport extends JPanel implements ItemListener {
  NumberFormat outDecimalFormat;
  DateField jTextField1, jTextField2;
  HelpedTextField jTextField3;
  String sHelpFile;
  CompanyComponents cc;
  int index = 0;
  String sReportTitle;
  String newline;
  String helpfile;
  private static int iWindowNumber = 1;
  final static int xOffset = 30, yOffset = 30;
  private static ImageIcon imageLazy8;
  private String frameName;
  ;
  JDesktopPane desktop;
  JFrame parent;
  View view;
  /**
   *  Description of the Field
   */
  protected StringBuffer jTextArea;
  JComboBox jReports;
  String sFilePattern;
  /**
   *  Description of the Field
   */
  protected boolean bIsReportOnClipboard = false;

  /**
   *  Constructor for the TextReport object
   *
   *@param  TitleBar      Description of the Parameter
   *@param  a             Description of the Parameter
   *@param  b             Description of the Parameter
   *@param  c             Description of the Parameter
   *@param  d             Description of the Parameter
   *@param  helpfilein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   *@param  desktopin     Description of the Parameter
   *@param  parentin      Description of the Parameter
   */
  public TextReport(View view,  String TitleBar,String helpfilein,String frameName) {
    //super(view,TitleBar,true);
    this.view=view;
    this.frameName=frameName;
    JDesktopPane desktopin=null;
    
    desktop = desktopin;
    

    helpfile = helpfilein;
    newline = System.getProperty("line.separator");
    //the above didnt work!!!!
    newline="\n";
    /*
     *  Rectangle rv=new Rectangle();
     *  Lazy8Ledger.desktop.getBounds(rv);
     *  setSize((int)(0.80*rv.width),(int)(0.80*rv.height));
     */
    jTextArea = new StringBuffer();
    if (SetupInfo.getProperty(SetupInfo.REPORT_NUMBER_FORMAT).compareTo("3") == 0)
      outDecimalFormat = new DecimalFormat(SetupInfo.getProperty(SetupInfo.REPORT_NUMBER_SPECIALFORMAT));
    else if (SetupInfo.getProperty(SetupInfo.REPORT_NUMBER_FORMAT).compareTo("1") == 0)
      outDecimalFormat = new DecimalFormat();
    else
      outDecimalFormat = NumberFormat.getCurrencyInstance();

  }


  /**
   *  Description of the Method
   *
   *@param  jp              Description of the Parameter
   *@param  sFilePatternin  Description of the Parameter
   */
  public void AddSaveFilePane(JPanel jp, String sFilePatternin) {
    //jp.setBorder (new javax.swing.border.TitledBorder(
    //     Translator.getTranslation("Report save and open")));
    //      jp.setLayout(new GridLayout(3,2));


    sFilePattern = sFilePatternin;
    jReports = new HelpedComboBox("Reports", helpfile, view);
    jReports.setMaximumRowCount(9);
    jReports.addItemListener(this);
    jp.add(new HelpedLabel(Translator.getTranslation("Report"), "Reports", helpfile, view));
    jp.add(jReports);
    JButton b11 = new HelpedButton(Translator.getTranslation("Save"),
        "Save", helpfile, view);
    jp.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonSaveReport();
        }
      }
        );
    b11 = new HelpedButton(Translator.getTranslation("Save as"),
        "SaveAs", helpfile, view);
    jp.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonSaveAsReport();
        }
      }
        );
    b11 = new HelpedButton(Translator.getTranslation("Delete"),
        "Delete", helpfile, view);
    jp.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonDeleteReport();
        }
      }
        );
    b11 = new HelpedButton(Translator.getTranslation("Clear"),
        "Clear", helpfile, view);
    jp.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonClearReport();
        }
      }
        );
    loadReportList();
  }

  /**
   *  Description of the Method
   */
  public void loadReportList() {
    jReports.removeAllItems();
    ArrayList aList = Fileio.getFileNames(sFilePattern);
    Object[] files = aList.toArray();
    if (files != null)
      for (int i = 0; i < files.length; i++)
        jReports.addItem(((String) files[i]).substring(0,
            ((String) files[i]).length() - 4));
  }

  /**
   *  Description of the Method
   *
   *@param  sReportName  Description of the Parameter
   */
  public void saveReportNow(String sReportName) {
    Properties pReport = new Properties();
    SaveReport(pReport);
    try {
      File file = Fileio.getFile(sReportName + ".bin", sFilePattern, true, false);
      OutputStream out = new FileOutputStream(file);
      pReport.store(out, "Version 1.0");
      loadReportList();
      jReports.setSelectedItem(sReportName);
      out.close();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Unable to create file. Error") +
          " : " + e.getMessage(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      SystemLog.ErrorPrintln("FAILED trying to save system parameters: " +
          e.getMessage());
      return;
    }
  }

  /**
   *  Description of the Method
   */
  public void buttonSaveReport() {
    saveReportNow((String) jReports.getSelectedItem());
  }

  /**
   *  Description of the Method
   */
  public void buttonSaveAsReport() {
    String inputValue = JOptionPane.showInputDialog(
        Translator.getTranslation("Input the new reports name"));
    if (inputValue.length() == 0)
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Unable to create report. Error"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);

    try {
      InputStream in = new FileInputStream(
          sFilePattern + inputValue + ".bin");
      if (JOptionPane.CANCEL_OPTION == JOptionPane.showConfirmDialog(this,
          Translator.getTranslation("The file already exists. Continue?"),
          "",
          JOptionPane.OK_CANCEL_OPTION,
          JOptionPane.QUESTION_MESSAGE))
        return;
      in.close();
    }
    catch (Exception ee) {
      //all is well, the file does not exist
    }
    saveReportNow(inputValue);
    jReports.setSelectedItem(inputValue);
  }

  /**
   *  Description of the Method
   */
  public void ClearReport() {
    //this must be overridden if used to save report information
  }

  /**
   *  Description of the Method
   *
   *@param  pList  Description of the Parameter
   */
  public void LoadReport(Properties pList) {
    //this must be overridden if used to save report information
  }

  /**
   *  Description of the Method
   */
  public void loadReport() {
    Properties pReport = new Properties();
    try {
      InputStream in  = Fileio.getInputStream(jReports.getSelectedItem() + ".bin", sFilePattern);
      pReport.load(in);
      LoadReport(pReport);
      in.close();
    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("FAILED trying to load parameters: " +
          sFilePattern + jReports.getSelectedItem() + ".bin" + " : " +
          e.getMessage());
      return;
    }
  }

  /**
   *  Description of the Method
   *
   *@param  pList  Description of the Parameter
   */
  public void SaveReport(Properties pList) {
    //this must be overridden if used to save report information
  }

  /**
   *  Description of the Method
   */
  public void buttonDeleteReport() {
    if (JOptionPane.CANCEL_OPTION == JOptionPane.showConfirmDialog(this,
        Translator.getTranslation("Are you sure you want to delete it?"),
        "",
        JOptionPane.OK_CANCEL_OPTION,
        JOptionPane.QUESTION_MESSAGE))
      return;
    try {
      File file = Fileio.getFile((String) jReports.getSelectedItem() + ".bin", sFilePattern,true,false);
      file.delete();
    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("FAILED to delete file: " +
          e.getMessage());
    }
    loadReportList();
  }

  /**
   *  Description of the Method
   */
  public void buttonClearReport() {
    ClearReport();
  }

  /**
   *  Description of the Method
   *
   *@param  jp              Description of the Parameter
   *@param  sReportTitlein  Description of the Parameter
   *@param  helpFile        Description of the Parameter
   */
  public void AddMiscComponents(JPanel jp, String sReportTitlein, String helpFile) {
    //       jp.setBorder (new javax.swing.border.TitledBorder(
    //    Translator.getTranslation("Data selection criteria")));
    //       jp.setLayout(new GridLayout(4,2));
    sHelpFile = helpFile;
    sReportTitle = sReportTitlein;
    cc = new CompanyComponents(jp, Translator.getTranslation("Company"), false, helpfile, view);
    cc.AddPeriod(jp, Translator.getTranslation("Period"), false);

    jp.add(new HelpedLabel(Translator.getTranslation("Start date"), "StartDate", helpfile, view));
    jTextField1 = new DateField("StartDate", helpfile, view);
    jTextField2 = new DateField("StopDate", helpfile, view);
    jp.add(jTextField1);
    try {
      jTextField1.setText(DateField.ConvertDateToLocalizedString(
          (java.util.Date) cc.comboBoxPeriod.getSelectedItemsKey()));
      jTextField2.setText(DateField.ConvertDateToLocalizedString(
          (java.util.Date) cc.comboBoxPeriod.getSelectedItemsSecondaryKey()));
    }
    catch (Exception e) {
    }
    jp.add(new HelpedLabel(Translator.getTranslation("Stop date"), "StopDate", helpfile, view));
    jp.add(jTextField2);

    jp.add(new HelpedLabel(Translator.getTranslation("Report title"), "ReportTitle", helpfile, view));
    jTextField3 = new HelpedTextField("ReportTitle", helpfile, view);
    jTextField3.setText(SetupInfo.getProperty(sReportTitle));
    jp.add(jTextField3);

  }

  /**
   *  Description of the Method
   *
   *@param  jAddhere  Description of the Parameter
   *@param  helpFile  Description of the Parameter
   */
  protected void AddButtonComponents(JPanel jAddhere, String helpFile) {
    sHelpFile = helpFile;
    AddButtonComponents(jAddhere);
  }

  /**
   *  Description of the Method
   *
   *@param  jAddhere  Description of the Parameter
   */
  protected void AddButtonComponents(JPanel jAddhere) {
    JButton b1 = new HelpedButton(Translator.getTranslation("Get report"), "GetReport", sHelpFile,view);
    jAddhere.add(b1);
    b1.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonGetReportPreperation(false);
        }
      }
        );

    JButton b10 = new HelpedButton(Translator.getTranslation("Get report on clipboard"), "GetReportOntoClipboard", sHelpFile,view);
    jAddhere.add(b10);
    b10.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonGetReportPreperation(true);
        }
      }
        );

    jAddhere.add(b10);

    JButton b11 = new HelpedButton(Translator.getTranslation("Exit"), "Exit", sHelpFile, view);
    jAddhere.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonExit();
        }
      }
        );
    JButton b12 = new HelpedButton(Translator.getTranslation("Help"), "Help", sHelpFile, view);
    jAddhere.add(b12);
    b12.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonHelp();
        }
      }
        );
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsDateFormatGood() {
    try {
      //invoice date
      jTextField1.getDate();
      jTextField2.getDate();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Date must be in the following format") +
          " : " +
          DateField.getTodaysDateString(),
          Translator.getTranslation("Cannot process report"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    return true;
  }


  /**
   *  Description of the Method
   *
   *@param  jAddhere  Description of the Parameter
   */
  protected void AddOtherComponents(JPanel jAddhere) {

  }

  /**
   *  Description of the Method
   *
   *@param  e  Description of the Parameter
   */
  public void itemStateChanged(ItemEvent e) {
    if (e.getStateChange() != ItemEvent.SELECTED)
      return;
    if (jReports != null && jReports.getSelectedItem() != null)
      loadReport();
  }

  StringBuffer sbRow;

  /**
   *  Description of the Method
   *
   *@param  FieldSizes  Description of the Parameter
   *@param  numFields   Description of the Parameter
   */
  protected void initializeRow(int FieldSizes[], int numFields) {
    if (sbRow == null)
      sbRow = new StringBuffer();
    sbRow.setLength(0);
    if (!bIsReportOnClipboard)
      for (int i = 1; i <= numFields; i++)
        for (int j = 1; j <= FieldSizes[i]; j++)
          sbRow.append(" ");
  }

  String empty = new String("                                                                                     ");

  /**
   *  Description of the Method
   *
   *@param  s     Description of the Parameter
   *@param  size  Description of the Parameter
   *@return       Description of the Return Value
   */
  public String renderField(String s, int size) {
    StringBuffer sb = new StringBuffer(s);
    if (sb.length() > size)
      sb.setLength(size - 1);
    if (bIsReportOnClipboard)
      return sb.toString();
    else
      return sb.toString() + empty.substring(0, size - sb.length());
  }

  /**
   *  Description of the Method
   *
   *@param  sb    Description of the Parameter
   *@param  size  Description of the Parameter
   *@return       Description of the Return Value
   */
  public String renderRightField(StringBuffer sb, int size) {
    if (sb.length() > size)
      sb.setLength(size - 1);
    if (bIsReportOnClipboard)
      return sb.toString();
    else
      return empty.substring(0, size - sb.length()) + sb.toString();
  }

  /**
   *  Description of the Method
   *
   *@param  dd  Description of the Parameter
   *@return     Description of the Return Value
   */
  public String IntegerToString(Integer dd) {
    return IntegerField.ConvertIntToLocalizedString(dd);
  }

  /**
   *  Description of the Method
   *
   *@param  db    Description of the Parameter
   *@param  size  Description of the Parameter
   *@return       Description of the Return Value
   */
  public String renderDateField(Object db, int size) {
    return renderField(DateField.ConvertDateToLocalizedString(
        (java.sql.Date) db), size);
  }

  /**
   *  Description of the Method
   *
   *@param  db    Description of the Parameter
   *@param  size  Description of the Parameter
   *@return       Description of the Return Value
   */
  public String renderDecimalField(Object db, int size) {
    StringBuffer sb2 = new StringBuffer();
    if (((Double) db).doubleValue() != 0)
      sb2.append(outDecimalFormat.format(((Double) db).doubleValue()));

    return renderRightField(sb2, size);
  }

  /**
   *  Description of the Method
   *
   *@param  db    Description of the Parameter
   *@param  size  Description of the Parameter
   *@return       Description of the Return Value
   */
  public String renderIntegerField(Object db, int size) {
    StringBuffer sb2 = new StringBuffer();
    if (((Integer) db).doubleValue() != 0)
      sb2.append(IntegerField.ConvertIntToLocalizedString((Integer) db));
    return renderField(sb2.toString(), size);
  }

  /**
   *  Adds a feature to the Field attribute of the TextReport object
   *
   *@param  s           The feature to be added to the Field attribute
   *@param  FieldSizes  The feature to be added to the Field attribute
   *@param  field       The feature to be added to the Field attribute
   *@param  type        The feature to be added to the Field attribute
   *@param  IsRight     The feature to be added to the Field attribute
   */
  protected void addField(Object s, int FieldSizes[], int field, int type, boolean IsRight) {
    int iStart = 0;
    String sFixed = new String("");
    switch (type) {
      case Types.DATE:
        sFixed = renderDateField(s, FieldSizes[field]);
        break;
      case Types.INTEGER:
        sFixed = renderIntegerField(s, FieldSizes[field]);
        break;
      case Types.DOUBLE:
        sFixed = renderDecimalField((Double) s, FieldSizes[field]);
        break;
      default:
        if (IsRight)
          sFixed = renderRightField(new StringBuffer((String) s), FieldSizes[field]);
        else
          sFixed = renderField((String) s, FieldSizes[field]);
        break;
    }
    if (bIsReportOnClipboard) {
      //no blanks between fields, just tabs, this will be a challange...
      int iTabCounter = 1;
      int i;
      for (i = 0; i < sbRow.length(); i++)
        if (sbRow.charAt(i) == '\t') {
          iTabCounter += 1;
          if (iTabCounter >= field) {
            sbRow.insert(i + 1, sFixed);
            break;
          }
        }

      if (i == sbRow.length()) {
        //need to add some tabs
        for (i = iTabCounter; i < field; i++)
          sbRow.append('\t');
        sbRow.append(sFixed);
      }
    }
    else {
      for (int i = 1; i < field; i++)
        iStart += FieldSizes[i];
      sbRow.replace(iStart, iStart + FieldSizes[field], sFixed);
    }
  }

  /**
   *  Description of the Method
   */
  public void AddReportHeaders() {
    jTextArea.append(jTextField3.getText() +
        "                   " +
        jTextField1.getText() + " <--> " + jTextField2.getText());
    jTextArea.append(newline);
    jTextArea.append(Translator.getTranslation("Todays date") + " : "
         + DateField.getTodaysDateString());
    jTextArea.append(newline);
    jTextArea.append(newline);
    jTextArea.append(newline);
  }

  /**
   *  Description of the Field
   */
  public final int fieldSize[] = {0, 5, 11, 30, 15, 10, 10};

  /**
   *  Description of the Method
   *
   *@param  IsOnClipboard  Description of the Parameter
   */
  public void buttonGetReportPreperation(boolean IsOnClipboard) {
    bIsReportOnClipboard = IsOnClipboard;
    jTextArea.setLength(0);
    String sReportsnewname = buttonGetReport();
    if (jTextArea.length() == 0) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("No records found"),
          Translator.getTranslation("No records found"),
          JOptionPane.PLAIN_MESSAGE);
      return;
    }
    if (bIsReportOnClipboard) {
      StringSelection contents = new StringSelection(jTextArea.toString());
      getToolkit().getSystemClipboard().setContents(contents, contents);
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("The report is now on the clipboard"),
          "",
          JOptionPane.PLAIN_MESSAGE);
    }
    else {
      if (SetupInfo.getProperty(SetupInfo.WORDPROCESSOR).length() != 0){
        try {
          File tmpFile = File.createTempFile("lazy8report", ".rtf", null);
          OutputStream out = new FileOutputStream(tmpFile);
          byte[] wholeReport=jTextArea.toString().getBytes();
          out.write(wholeReport);
          out.close();
          String[] args;
          args = new String[2];
          args[0] = SetupInfo.getProperty(SetupInfo.WORDPROCESSOR);
          args[1] = tmpFile.getAbsolutePath();
          Runtime.getRuntime().exec(args);
        }
        catch (Exception ex) {
          JOptionPane.showMessageDialog(parent,
              Translator.getTranslation("Cannot find the word processor defined in the options, now running the default word processor"),
              SetupInfo.getProperty(SetupInfo.WORDPROCESSOR),
              JOptionPane.PLAIN_MESSAGE);
          SystemLog.ErrorPrintln("ERROR RUNNING WORDPROCESSOR PROGRAM: " +
              SetupInfo.getProperty(SetupInfo.WORDPROCESSOR) + " : error message =" + ex);
        }
      }else{
        try{
          File tmpFile = File.createTempFile("lazy8report", ".txt", null);
          Buffer buf=jEdit.openFile(view,null, tmpFile.getPath(),true,new Hashtable());
          buf.insert(0, jTextArea.toString());
        }catch(Exception e){}
      }
    }
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public String buttonGetReport() {
    return "";
    //needs to be overridden by subclasses
  }

  /**
   *  Description of the Method
   */
  public void buttonHelp() {
    Lazy8Ledger.ShowHelp(view, helpfile ,"");
  }

  /**
   *  Description of the Method
   */
  public void buttonExit() {
    DockableWindowManager mgr = ((View)view).getDockableWindowManager();
    mgr.removeDockableWindow(frameName);
  }
}

