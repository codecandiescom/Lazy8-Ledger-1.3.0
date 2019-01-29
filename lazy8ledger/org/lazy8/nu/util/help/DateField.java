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
import java.text.*;
import java.util.*;
import java.awt.event.*;
import org.lazy8.nu.ledger.main.*;
/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DateField extends JTextField {
  private String helpName;
  private String helpField;
  private Date lastGotFocus;
  private JFrame view ;

  /**
   *  Constructor for the DateField object
   *
   *@param  helpFieldin   Description of the Parameter
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   */
  public DateField(String helpFieldin, String helpNamein,JFrame view ) {
    this.view=view;
    helpName = helpNamein;
    
    helpField = helpFieldin;
    FocusListener ll =
      new FocusListener() {
        public void focusLost(FocusEvent e) {
        }

        public void focusGained(FocusEvent e) {
          lastGotFocus = getTodaysDate();
        }
      };
    addFocusListener(ll);
    MouseListener l =
      new MouseListener() {
        public void mousePressed(MouseEvent e) {
        }

        public void mouseReleased(MouseEvent e) {
        }

        public void mouseEntered(MouseEvent e) {
          Lazy8Ledger.ShowContextHelp(DateField.this.view, helpName ,helpField);
        }

        public void mouseExited(MouseEvent e) {
        }

        public void mouseClicked(MouseEvent e) {
        }
      };
    addMouseListener(l);
    KeyListener keylst =
      new KeyListener() {
        public void keyTyped(KeyEvent e) {
        }

        public void keyPressed(KeyEvent e) {
        }

        public void keyReleased(KeyEvent e) {
          if (e.getKeyCode() == KeyEvent.VK_ENTER)
            if ((Calendar.getInstance().getTime().getTime() - lastGotFocus.getTime()) > 100)
              gotEnter();
        }
      };
    addKeyListener(keylst);
  }

  /**
   *  Description of the Method
   */
  public void gotEnter() {
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  protected Document createDefaultModel() {
    return new DateDocument();
  }

  /**
   *  Gets the todaysDateString attribute of the DateField class
   *
   *@return    The todaysDateString value
   */
  public static String getTodaysDateString() {
    return ConvertDateToLocalizedString(getTodaysDate());
  }

  /**
   *  Gets the todaysDate attribute of the DateField class
   *
   *@return    The todaysDate value
   */
  public static Date getTodaysDate() {
    return new java.sql.Date(Calendar.getInstance().getTime().getTime());
  }

  /**
   *  Sets the todaysDate attribute of the DateField object
   */
  public void setTodaysDate() {
    setDate(getTodaysDate());
  }

  /**
   *  Description of the Method
   *
   *@param  s                             Description of the Parameter
   *@return                               Description of the Return Value
   *@exception  java.text.ParseException  Description of the Exception
   */
  public static Date ConvertLocalizedStringToDate(String s)
    throws java.text.ParseException {
    Date dResult;
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    return new Date(df.parse(s).getTime());
  }

  /**
   *  Description of the Method
   *
   *@param  dd  Description of the Parameter
   *@return     Description of the Return Value
   */
  public static String ConvertDateToLocalizedString(Date dd) {
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    if (dd.getTime() == 0)
      return new String("");
    else
      return (df.format(dd));
  }

  /**
   *  Sets the date attribute of the DateField object
   *
   *@param  dd  The new date value
   */
  public void setDate(Date dd) {
    setText(ConvertDateToLocalizedString(dd));
  }

  /**
   *  Gets the date attribute of the DateField object
   *
   *@return                               The date value
   *@exception  java.text.ParseException  Description of the Exception
   */
  public Date getDate()
    throws java.text.ParseException {
    return ConvertLocalizedStringToDate(getText());
  }

  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  static class DateDocument extends PlainDocument {

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
      /*
       *  do nothing.. in the future maybe Ill think of something to put here
       *  if (str == null) {
       *  return;
       *  }
       *  /Just see if it is a Date value or  a decimal seperator
       *  try{
       *  DecimalFormat df=new DecimalFormat();
       *  int db=df.parse(str).intValue();
       *  }catch(Exception e2){
       *  /last chance, just see if it is the decimal seperator
       *  /this might bomb out if they have pasted in something
       *  /but so what
       *  DecimalFormatSymbols dfs=new DecimalFormatSymbols();
       *  if (str.compareTo(
       *  (new Character(dfs.getDecimalSeparator())).toString())!=0
       *  && str.compareTo((new Character(dfs.getGroupingSeparator())).
       *  toString())!=0)
       *  return;
       *  }
       */
      super.insertString(offs, str, a);
    }
  }
}


