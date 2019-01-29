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
import java.util.StringTokenizer;
import org.lazy8.nu.util.gen.*;
import org.lazy8.nu.ledger.main.*;
/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class DoubleField extends JTextField {
  /**
   *  Description of the Field
   */
  public final static String theTokens = "+-*/";
  private String helpName;
  private String helpField;
  private JFrame view;
  /**
   *  Description of the Method
   */
  public void showHelp() {
     Lazy8Ledger.ShowContextHelp(view, helpName ,helpField);
  }

  /**
   *  Constructor for the DoubleField object
   *
   *@param  cols          Description of the Parameter
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   *@param  helpFieldin   Description of the Parameter
   */
  public DoubleField(int cols, String helpFieldin, String helpNamein,JFrame view ) {
    super(cols);
    initialize(helpNamein, helpFieldin,view);
  }

  /**
   *  Constructor for the DoubleField object
   *
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   *@param  helpFieldin   Description of the Parameter
   */
  public DoubleField(String helpFieldin, String helpNamein,JFrame view) {
    initialize(helpFieldin, helpNamein,view);
  }

  /**
   *  Description of the Method
   *
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   *@param  helpFieldin   Description of the Parameter
   */
  private void initialize(String helpFieldin, String helpNamein,JFrame view) {
    helpName = helpNamein;
    this.view=view;
    helpField = helpFieldin;
    if (SetupInfo.getBoolProperty(SetupInfo.DATAENTRY_JUSTIFY_LEFT))
      setHorizontalAlignment(JTextField.LEFT);
    else
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
    FocusListener lll =
      new FocusListener() {
        public void focusLost(FocusEvent e) {
          processEndOfDataEntry();
        }

        public void focusGained(FocusEvent e) {
        }
      };
    addFocusListener(lll);
    KeyListener keylst =
      new KeyListener() {
        public void keyTyped(KeyEvent e) {
        }

        public void keyPressed(KeyEvent e) {
        }

        public void keyReleased(KeyEvent e) {
          if (e.getKeyCode() == KeyEvent.VK_ENTER)
            processEndOfDataEntry();
        }
      };
    addKeyListener(keylst);
  }

  /**
   *  Description of the Method
   */
  private void processEndOfDataEntry() {
    StringTokenizer tokens = new StringTokenizer(getText(), theTokens);
    if (tokens.countTokens() == 2) {
      double dFirst = ConvertLocalizedStringToDouble(tokens.nextToken()).doubleValue();
      double dSecond = ConvertLocalizedStringToDouble(tokens.nextToken()).doubleValue();
      //perform a mathematical calculation
      switch (stringContains(getText(), theTokens)) {
        case 0:
          // +
          setDouble(new Double(dFirst + dSecond));
          break;
        case 1:
          // -
          setDouble(new Double(dFirst - dSecond));
          break;
        case 2:
          // times
          setDouble(new Double(dFirst * dSecond));
          break;
        case 3:
          // divide
          if (dSecond != 0)
            setDouble(new Double(dFirst / dSecond));
          else
            setDouble(new Double(0));
          break;
      }
    }
    else if (tokens.countTokens() == 1)
      //just make sure the given text is really a good number
      setDouble(new Double(ConvertLocalizedStringToDouble(tokens.nextToken()).doubleValue()));

  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  protected Document createDefaultModel() {
    return new DoubleDocument();
  }

  /**
   *  Description of the Method
   *
   *@param  s  Description of the Parameter
   *@return    Description of the Return Value
   */
  public static Double ConvertLocalizedStringToDouble(String s) {
    NumberFormat df;
    if (SetupInfo.getProperty(SetupInfo.DATAENTRY_NUMBER_FORMAT).compareTo("3") == 0)
      df = new DecimalFormat(SetupInfo.getProperty(SetupInfo.DATAENTRY_NUMBER_SPECIALFORMAT));
    else if (SetupInfo.getProperty(SetupInfo.DATAENTRY_NUMBER_FORMAT).compareTo("1") == 0)
      df = new DecimalFormat();
    else
      df = NumberFormat.getCurrencyInstance();
    Double dResult;
    try {
      dResult = new Double(df.parse(s).doubleValue());
    }
    catch (Exception e) {
      dResult = new Double("0");
    }
    return dResult;
  }

  /**
   *  Description of the Method
   *
   *@param  dd  Description of the Parameter
   *@return     Description of the Return Value
   */
  public static String ConvertDoubleToLocalizedString(Double dd) {
    NumberFormat df;
    if (SetupInfo.getProperty(SetupInfo.DATAENTRY_NUMBER_FORMAT).compareTo("3") == 0)
      df = new DecimalFormat(SetupInfo.getProperty(SetupInfo.DATAENTRY_NUMBER_SPECIALFORMAT));
    else if (SetupInfo.getProperty(SetupInfo.DATAENTRY_NUMBER_FORMAT).compareTo("1") == 0)
      df = new DecimalFormat();
    else
      df = NumberFormat.getCurrencyInstance();

    if (dd.doubleValue() == 0)
      return new String("");
    else
      return (df.format(dd.doubleValue()));
  }

  /**
   *  Sets the double attribute of the DoubleField object
   *
   *@param  dd  The new double value
   */
  public void setDouble(Double dd) {
    setText(ConvertDoubleToLocalizedString(dd));
  }

  /**
   *  Gets the double attribute of the DoubleField object
   *
   *@return    The double value
   */
  public Double getDouble() {
    return ConvertLocalizedStringToDouble(getText());
  }

  /**
   *  Description of the Method
   *
   *@param  toSearch   Description of the Parameter
   *@param  searchFor  Description of the Parameter
   *@return            Description of the Return Value
   */
  private static int stringContains(String toSearch, String searchFor) {
    for (int i = 0; i < searchFor.length(); i++)
      if (toSearch.indexOf(new Character(searchFor.charAt(i)).toString()) >= 0)
        return i;
    return -1;
  }

  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  static class DoubleDocument extends PlainDocument {

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
      //Just see if it is a double value or  a decimal seperator
      try {
        DecimalFormat df = new DecimalFormat();
        double db = df.parse(str).doubleValue();
      }
      catch (Exception e2) {
        //last chance, just see if it is the decimal seperator
        //this might bomb out if they have pasted in something
        //but so what
        DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        if (str.compareTo(
            (new Character(dfs.getDecimalSeparator())).toString()) != 0
             && str.compareTo((new Character(dfs.getGroupingSeparator())).
            toString()) != 0 && stringContains(str, DoubleField.theTokens) == -1)

          return;
        else
            if (stringContains(getText(0, getLength()), theTokens) >= 0 && stringContains(str, DoubleField.theTokens) >= 0)
          return;
      }
      super.insertString(offs, str, a);
    }
  }
}


