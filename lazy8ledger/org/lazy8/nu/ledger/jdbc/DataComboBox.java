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

import javax.swing.JComboBox;
import javax.swing.JFrame;
import java.util.*;
import java.sql.Types;
import java.sql.Date;
import java.awt.event.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.ledger.main.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DataComboBox extends JComboBox {
  ArrayList keySecondaryArray = new ArrayList();
  ArrayList keyArray = new ArrayList();
  boolean bShowFirstItemBlank;
  private static LinkedList allInstances;
  private String helpName;
  private String helpField;
  private JFrame view;
  /**
   *  Description of the Field
   */
  protected JdbcTable dataAccess;

  /**
   *  Constructor for the DataComboBox object
   *
   *@param  dataAccessin  Description of the Parameter
   *@param  bShowBlank    Description of the Parameter
   *@param  HelpField     Description of the Parameter
   *@param  HelpFile      Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   */
  public DataComboBox(JdbcTable dataAccessin, boolean bShowBlank, String HelpField, String HelpFile,JFrame view) {
    this.view=view;
    dataAccess = dataAccessin;
    bShowFirstItemBlank = bShowBlank;
    helpName = HelpFile;
    helpField = HelpField;
    
    FocusListener l =
      new FocusListener() {
        public void focusLost(FocusEvent e) {
        }

        public void focusGained(FocusEvent e) {
          showHelp();
        }
      };
    addFocusListener(l);
  }
  public void showHelp() {
    Lazy8Ledger.ShowContextHelp(view, helpName ,helpField);
  }

  /**
   *  Description of the Method
   *
   *@param  comboBoxes  Description of the Parameter
   *@param  CompId      Description of the Parameter
   */
  public static void loadComboBoxes(DataComboBox[] comboBoxes,
      Integer CompId) {
    /*
     *  This only works for Accounts
     */
    Integer intIn;
    int i;
    int iNumInArray = comboBoxes.length;

    for (i = 0; i < iNumInArray; i++) {
      comboBoxes[i].keyArray.clear();
      comboBoxes[i].removeAllItems();
    }
    comboBoxes[0].dataAccess.setObject(CompId, "CompId");
    if (comboBoxes[0].dataAccess.GetFirstRecord()) {
      if (comboBoxes[0].bShowFirstItemBlank)
        for (i = 0; i < iNumInArray; i++) {
          comboBoxes[i].addItem(new String(""));
          comboBoxes[i].keyArray.add(new Integer(0));
        }

      do {
        intIn = (Integer) comboBoxes[0].dataAccess.
            getObject("Account", null);
        String ss = new String(IntegerField.ConvertIntToLocalizedString(intIn)
             + " : " + (String) comboBoxes[0].dataAccess.
            getObject("AccDesc", null));
        for (i = 0; i < iNumInArray; i++) {
          comboBoxes[i].keyArray.add(intIn);
          comboBoxes[i].addItem(ss);
        }
      } while (comboBoxes[0].dataAccess.GetNextRecord())
          ;
    }
  }

  /**
   *  Description of the Method
   *
   *@param  sDescField  Description of the Parameter
   *@param  sIntField   Description of the Parameter
   *@param  key         Description of the Parameter
   */
  public void loadComboBox(String sDescField, String sIntField, Integer key) {
    keyArray.clear();
    removeAllItems();
    IntHolder iFieldTypeOut1 = new IntHolder();
    IntHolder iFieldTypeOut2 = new IntHolder();
    Object objDescField1;
    Object objDescField2;
    String sDescFieldToString1 = new String();
    String sDescFieldToString2 = new String();

    if (key.intValue() != 0)
      dataAccess.setObject(key, "CompId");
    if (bShowFirstItemBlank) {
      addItem(new String(""));
      keyArray.add(new Integer(0));
      keySecondaryArray.add(new String(""));
    }
    if (dataAccess.GetFirstRecord()) {
      do {
        objDescField1 = dataAccess.getObject(sIntField, iFieldTypeOut1);
        objDescField2 = dataAccess.getObject(sDescField, iFieldTypeOut2);
        keyArray.add(objDescField1);
        keySecondaryArray.add(objDescField2);
        switch (iFieldTypeOut1.iValue) {
          case Types.INTEGER:
            sDescFieldToString1 = IntegerField.ConvertIntToLocalizedString((Integer) objDescField1);
            break;
          case Types.DOUBLE:
            sDescFieldToString1 = DoubleField.ConvertDoubleToLocalizedString((Double) objDescField1);
            break;
          case Types.DATE:
            sDescFieldToString1 = DateField.ConvertDateToLocalizedString((Date) objDescField1);
            break;
          default:
            sDescFieldToString1 = (String) objDescField1;
            break;
        }
        switch (iFieldTypeOut2.iValue) {
          case Types.INTEGER:
            sDescFieldToString2 = IntegerField.ConvertIntToLocalizedString((Integer) objDescField2);
            break;
          case Types.DOUBLE:
            sDescFieldToString2 = DoubleField.ConvertDoubleToLocalizedString((Double) objDescField2);
            break;
          case Types.DATE:
            sDescFieldToString2 = DateField.ConvertDateToLocalizedString((Date) objDescField2);
            break;
          default:
            sDescFieldToString2 = (String) objDescField2;
            break;
        }
        addItem(sDescFieldToString1 + " : " + sDescFieldToString2);
      } while (dataAccess.GetNextRecord());
    }
  }

  /**
   *  Gets the selectedItemsKey attribute of the DataComboBox object
   *
   *@return    The selectedItemsKey value
   */
  public Object getSelectedItemsKey() {
    try {
      return keyArray.get(getSelectedIndex());
    }
    catch (Exception e) {
      return new Integer(0);
    }
  }

  /**
   *  Gets the selectedItemsSecondaryKey attribute of the DataComboBox object
   *
   *@return    The selectedItemsSecondaryKey value
   */
  public Object getSelectedItemsSecondaryKey() {
    try {
      return keySecondaryArray.get(getSelectedIndex());
    }
    catch (Exception e) {
      return new Integer(0);
    }
  }

  /**
   *  Sets the selectedItemFromKey attribute of the DataComboBox object
   *
   *@param  key  The new selectedItemFromKey value
   */
  public void setSelectedItemFromKey(Integer key) {
    int j = 0;
    Integer nextKey;
    for (ListIterator i = keyArray.listIterator(0); i.hasNext(); j++) {
      nextKey = (Integer) i.next();
      if (nextKey.compareTo(key) == 0) {
        setSelectedIndex(j);
        return;
      }
    }
    ListIterator i = keyArray.listIterator(0);
    if (i.hasNext())
      setSelectedIndex(0);
  }

  /**
   *  Sets the selectedItemFromKeys attribute of the DataComboBox object
   *
   *@param  key   The new selectedItemFromKeys value
   *@param  key2  The new selectedItemFromKeys value
   */
  public void setSelectedItemFromKeys(java.util.Date key, java.util.Date key2) {
    int j = 0;
    Date nextKey;
    Date nextKey2;
    ListIterator i2 = keySecondaryArray.listIterator(0);
    for (ListIterator i = keyArray.listIterator(0); i.hasNext(); j++) {
      nextKey = (Date) i.next();
      nextKey2 = (Date) i2.next();
      if (nextKey.compareTo(key) == 0 && nextKey2.compareTo(key2) == 0) {
        setSelectedIndex(j);
        return;
      }
    }
    ListIterator i = keyArray.listIterator(0);
    if (i.hasNext())
      setSelectedIndex(0);
  }
}

