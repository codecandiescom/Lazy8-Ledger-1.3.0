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
package org.lazy8.nu.util.help;

import javax.swing.text.AttributeSet;
import javax.swing.text.PlainDocument;
import javax.swing.text.Document;
import javax.swing.text.BadLocationException;
import javax.swing.JTextField;
import javax.swing.JFrame;
import java.text.DecimalFormatSymbols;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.FieldPosition;
import java.awt.event.*;
import org.lazy8.nu.ledger.main.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class IntegerField extends JTextField {

  private String helpName;
  private String helpField;
  private JFrame view;
  /**
   *  Constructor for the IntegerField object
   *
   *@param  cols          Description of the Parameter
   *@param  helpFieldin   Description of the Parameter
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   */
  public IntegerField(int cols, String helpFieldin, String helpNamein,JFrame view ) {
    super(cols);
    initialize(helpFieldin, helpNamein,view);
  }

  /**
   *  Constructor for the IntegerField object
   *
   *@param  helpFieldin   Description of the Parameter
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   */
  public IntegerField(String helpFieldin, String helpNamein,JFrame view) {
    initialize(helpFieldin, helpNamein,view);
  }

  public void showHelp() {
      Lazy8Ledger.ShowContextHelp(view, helpName ,helpField);
  }
  /**
   *  Description of the Method
   *
   *@param  helpFieldin   Description of the Parameter
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   */
  private void initialize(String helpFieldin, String helpNamein,JFrame view) {
    this.view=view;
    helpName = helpNamein;
    
    helpField = helpFieldin;
    setHorizontalAlignment(JTextField.RIGHT);
    MouseListener l =
      new MouseListener() {
        public void mousePressed(MouseEvent e) {
        }

        public void mouseReleased(MouseEvent e) {
        }

        public void mouseEntered(MouseEvent e) {
          showHelp();
        }

        public void mouseExited(MouseEvent e) {
        }

        public void mouseClicked(MouseEvent e) {
        }
      };
    addMouseListener(l);
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  protected Document createDefaultModel() {
    return new IntegerDocument();
  }

  /**
   *  Description of the Method
   *
   *@param  s  Description of the Parameter
   *@return    Description of the Return Value
   */
  public static Integer ConvertLocalizedStringToInt(String s) {
    Integer dResult;
    DecimalFormat df = new DecimalFormat();
    try {
      dResult = new Integer(df.parse(s).intValue());
    }
    catch (Exception e) {
      dResult = new Integer(0);
    }
    return dResult;
  }

  /**
   *  Description of the Method
   *
   *@param  dd  Description of the Parameter
   *@return     Description of the Return Value
   */
  public static String ConvertIntToLocalizedString(Integer dd) {
    DecimalFormat df = new DecimalFormat();
    if (dd.intValue() == 0)
      return new String("");
    else
      return (df.format(dd.intValue()));
  }

  /**
   *  Sets the integer attribute of the IntegerField object
   *
   *@param  dd  The new integer value
   */
  public void setInteger(Integer dd) {
    setText(ConvertIntToLocalizedString(dd));
  }

  /**
   *  Gets the integer attribute of the IntegerField object
   *
   *@return    The integer value
   */
  public Integer getInteger() {
    return ConvertLocalizedStringToInt(getText());
  }

  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  static class IntegerDocument extends PlainDocument {

    /**
     *  Description of the Method
     *
     *@param  offs                      Description of the Parameter
     *@param  str                       Description of the Parameter
     *@param  a                         Description of the Parameter
     *@exception  BadLocationException  Description of the Exception
     */
    public void insertString(int offs, String str, AttributeSet a)
      throws BadLocationException {

      if (str == null)
        return;

      //Just see if it is a int value or  a grouping seperator
      try {
        DecimalFormat df = new DecimalFormat();
        int db = df.parse(str).intValue();
      }
      catch (Exception e2) {
        //last chance, just see if it is the grouping seperator
        //this might bomb out if they have pasted in something
        //but so what
        DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        if (str.compareTo((new Character(
            dfs.getGroupingSeparator())).toString()) != 0)
          return;
      }
      super.insertString(offs, str, a);

    }
  }
}

