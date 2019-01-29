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
import java.awt.Rectangle;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.gui.*;
import org.gjt.sp.jedit.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DataExchangeForm extends JPanel {
  /**
   *  Description of the Field
   */
  public String HelpFile;
  private static int iWindowNumber = 1;
  final static int xOffset = 30, yOffset = 30;
  private static ImageIcon imageLazy8;
  protected JFrame view;
  private String frameName;
  /**
   *  Description of the Field
   */
  public JDesktopPane desktop;
  boolean bShowCompanyAlways;
  /**
   *  Description of the Field
   */
  public DataMovementPane dataMovementPane;

  /**
   *  Constructor for the DataExchangeForm object
   *
   *@param  TitleBar              Description of the Parameter
   *@param  a                     Description of the Parameter
   *@param  b                     Description of the Parameter
   *@param  c                     Description of the Parameter
   *@param  d                     Description of the Parameter
   *@param  sUrlHelp              Description of the Parameter
   *@param  bShowCompanyAlwaysin  Description of the Parameter
   *@param  desktopin             Description of the Parameter
   */
  public DataExchangeForm(String HelpFile,JFrame view,String frameName) {
    this.view=view;
    this.HelpFile=HelpFile;
    this.frameName=frameName;
  }
  /**
   *  Gets the fields attribute of the DataExchangeForm object
   *
   *@param  iWhyGet  Description of the Parameter
   */
  public void getFields(int iWhyGet) { }

  /**
   *  Description of the Method
   */
  public void putFields() { }

  /**
   *  Description of the Method
   */
  public void clearFields() { }

  /**
   *  Description of the Method
   */
  public void Exit(){
    DockableWindowManager mgr = ((View)view).getDockableWindowManager();
    mgr.removeDockableWindow(frameName);
  }

  /**
   *  Description of the Method
   */
  public void CannotUpdateMessage() {
    JOptionPane.showMessageDialog(this,
        Translator.getTranslation("Other dependent records still exist"),
        Translator.getTranslation("Update not entered"),
        JOptionPane.PLAIN_MESSAGE);
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsChangeOK() {
    return true;
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsDeleteOK() {
    if (SetupInfo.getBoolProperty(SetupInfo.WARNING_BEFORE_DELETE)
         && JOptionPane.CANCEL_OPTION == JOptionPane.showConfirmDialog(
        this,
        Translator.getTranslation("Are you sure you want to delete it?"),
        "",
        JOptionPane.OK_CANCEL_OPTION,
        JOptionPane.QUESTION_MESSAGE))
      return false;
    return true;
  }

  /**
   *  Description of the Method
   *
   *@param  bEnabled  Description of the Parameter
   */
  public void ChangeButtonEnabled(boolean bEnabled) {
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsWriteOK() {
    return true;
  }

  /**
   *  Description of the Method
   */
  public void AfterGoodWrite() {
    if (SetupInfo.getBoolProperty(SetupInfo.WARNING_GOOD_ADD))
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("The record was added successfully"),
          Translator.getTranslation("Successful"),
          JOptionPane.PLAIN_MESSAGE);
    if (SetupInfo.getBoolProperty(SetupInfo.WARNING_CLEAR_AFTER_ADD))
      dataMovementPane.buttonClearActionPerformed(null);
  }

  /**
   *  Description of the Method
   */
  public void AfterGoodDelete() {
    if (SetupInfo.getBoolProperty(SetupInfo.WARNING_AFTER_DELETE))
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("The record was deleted successfully"),
          Translator.getTranslation("Successful"),
          JOptionPane.PLAIN_MESSAGE);
    if (SetupInfo.getBoolProperty(SetupInfo.WARNING_CLEAR_AFTER_DELETE))
      dataMovementPane.buttonClearActionPerformed(null);
  }
}


