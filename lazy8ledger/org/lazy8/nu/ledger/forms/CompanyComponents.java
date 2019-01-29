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
import java.text.*;
import java.util.Date;
import java.awt.event.*;
import java.awt.Container;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class CompanyComponents {
  /**
   *  Description of the Field
   */
  public DataComboBox comboBox;
  /**
   *  Description of the Field
   */
  public DataComboBox comboBoxPeriod;
  String sHelpFile;
  private JFrame view;

  /**
   *  Constructor for the CompanyComponents object
   *
   *@param  jPanel1      Description of the Parameter
   *@param  sLabelText   Description of the Parameter
   *@param  bShowAlways  Description of the Parameter
   *@param  helpFile     Description of the Parameter
   *@param  helpViewer   Description of the Parameter
   */
  public CompanyComponents(Container jPanel1, String sLabelText,
      boolean bShowAlways, String helpFile,JFrame view) {
    this.view=view;
    sHelpFile = helpFile;
    boolean bShowAdvanced = false;
    if (bShowAlways)
      bShowAdvanced = true;
    else if (SetupInfo.getProperty(SetupInfo.SHOW_ADVANCED_MENUS).compareTo("true") == 0)
      bShowAdvanced = true;

    JLabel label1 = new HelpedLabel(sLabelText, "company", helpFile,view);

    jPanel1.add(label1);

    comboBox = new DataComboBox(new JdbcTable("Company", 1,view), false, "company", helpFile,view);
    ActionListener lsel =
      new ActionListener() {

        public void actionPerformed(ActionEvent e) {
          updatePeriod();
        }
      };
    comboBox.addActionListener(lsel);
    jPanel1.add(comboBox);

    comboBox.loadComboBox("Name", "CompId", new Integer(0));
    //select the default company
    JdbcTable aa = new JdbcTable("Company", 1,view);
    try {
      aa.setObject(new Integer(
          SetupInfo.getProperty(SetupInfo.DEFAULT_COMPANY)),
          "CompId");
      if (aa.GetFirstRecord())
        comboBox.setSelectedItemFromKey((Integer)
            aa.getObject("CompId", null));

    }
    catch (Exception ex) {
      comboBox.setSelectedItem("");
      /*
       *  just clear the dialog
       */
    }
    comboBox.setEnabled(bShowAdvanced);
    label1.setEnabled(bShowAdvanced);
  }

  /**
   *  Description of the Method
   */
  public void updatePeriod() {
    if (comboBoxPeriod == null)
      return;
    comboBoxPeriod.removeAllItems();
    comboBoxPeriod.loadComboBox("EndPeriod", "StartPeriod", (Integer) comboBox.getSelectedItemsKey());
    try {
      comboBoxPeriod.setSelectedItemFromKeys((Date)
          DateField.ConvertLocalizedStringToDate(
          SetupInfo.getProperty(SetupInfo.DEFAULT_PERIOD1)), (Date)
          DateField.ConvertLocalizedStringToDate(
          SetupInfo.getProperty(SetupInfo.DEFAULT_PERIOD2)));
    }
    catch (Exception ex) {
    }
  }

  /**
   *  Description of the Method
   *
   *@param  jPanel1      Description of the Parameter
   *@param  sLabelText   Description of the Parameter
   *@param  bShowAlways  Description of the Parameter
   *@param  helpViewer   Description of the Parameter
   */
  public void AddPeriod(Container jPanel1, String sLabelText,
      boolean bShowAlways) {
    boolean bShowAdvanced = false;
    if (bShowAlways)
      bShowAdvanced = true;
    else if (SetupInfo.getProperty(SetupInfo.SHOW_ADVANCED_MENUS).compareTo("true") == 0)
      bShowAdvanced = true;

    JLabel label1 = new HelpedLabel(sLabelText, "period", sHelpFile,view);

    jPanel1.add(label1);

    comboBoxPeriod = new DataComboBox(new JdbcTable("AccountingPeriods", 1,view), false, "period", sHelpFile,view);
    jPanel1.add(comboBoxPeriod);

    comboBoxPeriod.loadComboBox("EndPeriod", "StartPeriod", (Integer) comboBox.getSelectedItemsKey());
    //select the default company
    try {
      comboBoxPeriod.setSelectedItemFromKeys((Date)
          DateField.ConvertLocalizedStringToDate(
          SetupInfo.getProperty(SetupInfo.DEFAULT_PERIOD1)), (Date)
          DateField.ConvertLocalizedStringToDate(
          SetupInfo.getProperty(SetupInfo.DEFAULT_PERIOD2)));
    }
    catch (Exception ex) {
    }
    comboBoxPeriod.setEnabled(bShowAdvanced);
    label1.setEnabled(bShowAdvanced);

  }
  public void saveDefaults(){
    Integer intKey = (Integer) comboBox.getSelectedItemsKey();
    SetupInfo.setIntProperty(SetupInfo.DEFAULT_COMPANY, intKey.intValue());
    try {
      java.util.Date dateKey = (java.util.Date) comboBoxPeriod.getSelectedItemsKey();
      java.util.Date dateKey2 = (java.util.Date) comboBoxPeriod.getSelectedItemsSecondaryKey();
      SetupInfo.setProperty(SetupInfo.DEFAULT_PERIOD1,
          DateField.ConvertDateToLocalizedString(dateKey));
      SetupInfo.setProperty(SetupInfo.DEFAULT_PERIOD2,
          DateField.ConvertDateToLocalizedString(dateKey2));
    }
    catch (Exception eeee) {
    }
    ;
  }
}


