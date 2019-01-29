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

import java.sql.*;
import java.util.*;
import java.text.*;
import javax.swing.*;
import javax.swing.table.*;
import javax.swing.event.*;
import java.awt.*;
import java.io.*;
import org.lazy8.nu.ledger.reports.TableSorter;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.util.help.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.jedit.msg.*;
import org.lazy8.nu.ledger.main.*;
import org.gjt.sp.util.Log;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class TranslatorPane extends DataExchangeForm {
  private static String PresentLanguage = new String();
  JScrollPane table;
  TableSorter sorter;
  ShowLanguage rightLanguage, leftLanguage;
  private ArrayList keys;
  private Properties[] pLists;
  private String[] sLangCodes;
  private String[] sLanguages;
  private JFrame view;
  /**
   *  Description of the Field
   */
  public final static int iNumColumns = 2;
  public TranslatorPane(JFrame view) {
    super("translator",view,"lazy8ledger-customer");
    this.view=view;
    sLangCodes = new String[iNumColumns];
    sLanguages = new String[iNumColumns];
    for (int i = 0; i < iNumColumns; i++)
      sLanguages[i] = new String("");
    pLists = new Properties[iNumColumns];
    keys = new ArrayList();

    //Translator.Initialize();

    setLayout(new BorderLayout());

    table = createTable();
    add(table, BorderLayout.CENTER);

    JPanel jpMain = new JPanel();
    jpMain.setLayout(new GridLayout(1, 2));

    JPanel jp1 = new JPanel();
    leftLanguage = new ShowLanguage(jp1, 0, this);
    jpMain.add(jp1);

    JPanel jp2 = new JPanel();
    rightLanguage = new ShowLanguage(jp2, 1, this);
    jpMain.add(jp2);

    JComboBox[] boxes = new JComboBox[iNumColumns];
    ArrayList[] arrays = new ArrayList[iNumColumns];
    boxes[0] = leftLanguage.jLanguageBox;
    arrays[0] = leftLanguage.arrayLanguages;
    boxes[1] = rightLanguage.jLanguageBox;
    arrays[1] = rightLanguage.arrayLanguages;
    GetAllLocales(boxes, arrays, false);

    add(jpMain, BorderLayout.NORTH);

    JPanel jpButtons = new JPanel();
    jpButtons.setLayout(new GridLayout(1, 3));
    add(jpButtons, BorderLayout.SOUTH);
    //save button
    //cancel button
    //help button
    JButton b11 = new HelpedButton(Translator.getTranslation("Save"), "Save", "translator",view);
    jpButtons.add(b11);
    b11.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonSave();
        }
      }
        );
    JButton b12 = new HelpedButton(Translator.getTranslation("Cancel"), "Cancel", "translator",view);
    jpButtons.add(b12);
    b12.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonCancel();
        }
      }
        );
    JButton b13 = new HelpedButton(Translator.getTranslation("Help"), "Help", "translator",view);
    jpButtons.add(b13);
    b13.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          buttonHelp();
        }
      }
        );

    //Rectangle rv = new Rectangle();
    //desktop.getBounds(rv);
    //setSize((int) (0.80 * rv.width), (int) (0.80 * rv.height));

  }

  /**
   *  Description of the Method
   */
  public void buttonSave() {
    for (int i = 0; i < iNumColumns; i++)
      if (pLists[i] != null)
        storeLanguage(sLangCodes[i], pLists[i]);
    //dispose();
  }

  /**
   *  Description of the Method
   */
  public void buttonCancel() {
    Exit();
    //dispose();
  }

  /**
   *  Description of the Method
   */
  public void buttonHelp() {
    Lazy8Ledger.ShowHelp(view, "translator" ,"");
  }

  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  public class ShowLanguage {
    JComboBox jLanguageBox;
    ArrayList arrayLanguages;
    int iMyColumn;
    TranslatorPane tl;

    /**
     *  Constructor for the ShowLanguage object
     *
     *@param  jp           Description of the Parameter
     *@param  iMyColumnin  Description of the Parameter
     *@param  tlin         Description of the Parameter
     *@param  helpViewer   Description of the Parameter
     */
    public ShowLanguage(JPanel jp, int iMyColumnin, TranslatorPane tlin) {
      tl = tlin;
      iMyColumn = iMyColumnin;

      jp.setLayout(new GridLayout(2, 2));

      jp.add(new HelpedLabel(Translator.getTranslation("Language"), "Language", "translator",view));

      jLanguageBox = new JComboBox();
      arrayLanguages = new ArrayList();

      jp.add(jLanguageBox);

      JButton b11 = new HelpedButton(Translator.getTranslation("New"), "New", "translator",view);
      jp.add(b11);
      b11.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            buttonNew();
          }
        }
          );
      JButton b12 = new HelpedButton(Translator.getTranslation("Load"), "Load", "translator",view);
      jp.add(b12);
      b12.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            buttonLoad();
          }
        }
          );

    }

    /**
     *  Description of the Method
     */
    public void buttonNew() {
      tl.NewColumn(iMyColumn, (String)
          arrayLanguages.get(jLanguageBox.getSelectedIndex()),
          (String) jLanguageBox.getSelectedItem());
    }

    /**
     *  Description of the Method
     */
    public void buttonLoad() {
      tl.LoadColumn(iMyColumn, (String)
          arrayLanguages.get(jLanguageBox.getSelectedIndex()),
          (String) jLanguageBox.getSelectedItem());
    }
  }

  /**
   *  Description of the Method
   *
   *@param  col        Description of the Parameter
   *@param  sLangCode  Description of the Parameter
   *@param  sLanguage  Description of the Parameter
   */
  public void NewColumn(int col, String sLangCode, String sLanguage) {
    if (keys.size() == 0)
      for (Enumeration e = Translator.presentLanguage.propertyNames();
          e.hasMoreElements(); )
        keys.add(e.nextElement());
    sLangCodes[col] = new String(sLangCode);
    sLanguages[col] = new String(sLanguage);
    pLists[col] = new Properties();
    for (ListIterator i = keys.listIterator(0); i.hasNext(); ) {
      String sKey = (String) i.next();
      pLists[col].setProperty(sKey,
          Translator.presentLanguage.getProperty(sKey));
    }
    sorter.tableChanged(null);
    sorter.fireTableChanged(null);
  }

  /**
   *  Description of the Method
   *
   *@param  col        Description of the Parameter
   *@param  sLangCode  Description of the Parameter
   *@param  sLanguage  Description of the Parameter
   */
  public void LoadColumn(int col, String sLangCode, String sLanguage) {
    sLangCodes[col] = new String(sLangCode);
    sLanguages[col] = new String(sLanguage);
    pLists[col] = new Properties();
    if (!loadLanguage(sLangCode, pLists[col])) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Cannot find such a language definition"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return;
    }
    if (keys.size() == 0){
      String rowFound;
      for (Enumeration e = pLists[col].propertyNames();
           e.hasMoreElements(); ){
	rowFound=(String)e.nextElement();
        keys.add(rowFound);
	Log.log(Log.DEBUG,this,"loadcolumn="+rowFound);

      }
    }
    sorter.tableChanged(null);
    sorter.fireTableChanged(null);
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public JScrollPane createTable() {
    // Create a model of the data.
    TableModel dataModel =
      new AbstractTableModel() {
        // These methods always need to be implemented.
        public int getColumnCount() {
          return iNumColumns;
        }

        public int getRowCount() {
          return keys.size();
        }

        public Object getValueAt(int row, int col) {
          if (pLists[col] == null){
	    Log.log(Log.DEBUG,this,"getValueAt is null");
            return new String("");
	  }
	  Log.log(Log.DEBUG,this,"getValueAt="+(String) keys.get(row));
          return pLists[col].getProperty((String) keys.get(row));
        }

        // The default implementations of these methods in
        // AbstractTableModel would work, but we can refine them.
        public String getColumnName(int column) {

          return sLanguages[column];
        }

        public Class getColumnClass(int col) {
          return "".getClass();
        }

        public boolean isCellEditable(int row, int col) {
          return true;
        }

        public void setValueAt(Object aValue, int row, int column) {
          if (row < keys.size()){
            pLists[column].setProperty((String) keys.get(row),
                (String) aValue);
	  }

        }
      };

    sorter = new TableSorter(dataModel);
    // Create the table
    JTable table = new JTable(sorter);
    // Use a scrollbar, in case there are many columns.
    table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

    // Install a mouse listener in the TableHeader as the sorter UI.
    sorter.addMouseListenerToHeaderInTable(table);

    JScrollPane scrollpane = new JScrollPane(table);

    return scrollpane;
  }


  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  static class ReportFileFilter implements FilenameFilter {
    String sFilter;
    String sFilter2;

    /**
     *  Constructor for the ReportFileFilter object
     *
     *@param  sFilterin   Description of the Parameter
     *@param  sFilter2in  Description of the Parameter
     */
    public ReportFileFilter(String sFilterin, String sFilter2in) {
      sFilter = sFilterin;
      sFilter2 = sFilter2in;
    }

    /**
     *  Description of the Method
     *
     *@param  file  Description of the Parameter
     *@param  name  Description of the Parameter
     *@return       Description of the Return Value
     */
    public boolean accept(File file, String name) {
      return name.startsWith(sFilter) && name.endsWith(sFilter2);
    }
  }

  /**
   *  Description of the Method
   *
   *@param  jbox                   Description of the Parameter
   *@param  array                  Description of the Parameter
   *@param  bShowExistingLangOnly  Description of the Parameter
   */
  public static void GetAllLocales(JComboBox[] jbox, ArrayList[] array,
      boolean bShowExistingLangOnly) {

    for (int k = 0; k < jbox.length; k++) {
      array[k].clear();
      jbox[k].removeAllItems();
    }
    ArrayList dir = Fileio.getFileNames("lang");
    Object[] files = dir.toArray();

    Locale[] locales = Locale.getAvailableLocales();

    for (int j = 0; j < locales.length; j++) {
      String nextKey;
      String sIsoLanguage = locales[j].getLanguage();
      boolean bFoundDouble = false;
      for (ListIterator i = array[0].listIterator(0); i.hasNext(); ) {
        nextKey = (String) i.next();
        if (nextKey.compareTo(sIsoLanguage) == 0) {
          bFoundDouble = true;
          break;
        }
      }
      if (!bFoundDouble) {
        boolean bFoundLang = true;
        if (bShowExistingLangOnly)
          for (int k = 0; k < files.length; k++)
            if (sIsoLanguage.compareTo(((String) files[k]).substring(0, 2)) == 0) {
              bFoundLang = true;
              break;
            }

            else
              bFoundLang = false;

        if (bFoundLang)
          for (int k = 0; k < jbox.length; k++) {
            jbox[k].addItem(locales[j].getDisplayLanguage());
            array[k].add(sIsoLanguage);
          }

      }
    }
  }

  /**
   *  Description of the Method
   *
   *@param  sLangCode  Description of the Parameter
   *@param  pList      Description of the Parameter
   *@return            Description of the Return Value
   */
  private static boolean loadLanguage(String sLangCode, Properties pList) {
    try {
      InputStream in = Fileio.getInputStream(sLangCode + ".bin", "lang");
      pList.load(in);
      in.close();
    }
    catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   *  Description of the Method
   *
   *@param  sLangCode  Description of the Parameter
   *@param  pList      Description of the Parameter
   */
  public static void storeLanguage(String sLangCode, Properties pList) {
    //Initialize();
    try {
      File file = Fileio.getFile(sLangCode + ".bin", "lang", true, false);
      OutputStream out = new FileOutputStream(file);
      pList.store(out, "Version 1.0");
      out.close();

    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("FAILED trying to save language: " +
          e.getMessage());
      return;
    }
  }

}

