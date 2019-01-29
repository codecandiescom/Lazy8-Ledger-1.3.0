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

import javax.swing.*;
import java.awt.event.*;
import java.util.*;
import org.lazy8.nu.ledger.main.*;
/**
 *  Description of the Class
 *
 *@author     Administrator
 *@created    den 8 mars 2002
 */
public class HelpedButton extends JButton {
  private String helpName;
  private String helpField;
  private Date lastGotFocus;
  private JFrame view;
  /**
   *  Constructor for the HelpedTextField object
   *
   *@param  helpFieldin   Description of the Parameter
   *@param  helpNamein    Description of the Parameter
   *@param  helpViewerin  Description of the Parameter
   *@param  Caption       Description of the Parameter
   */
  public HelpedButton(String Caption, String helpFieldin, String helpNamein,JFrame view) {
    super(Caption);
    helpName = helpNamein;
    this.view=view;
    helpField = helpFieldin;
    FocusListener ll =
      new FocusListener() {
        public void focusLost(FocusEvent e) {
        }

        public void focusGained(FocusEvent e) {
          lastGotFocus = new Date(Calendar.getInstance().getTime().getTime());
        }
      };
    addFocusListener(ll);
    KeyListener keylst =
      new KeyListener() {
        public void keyTyped(KeyEvent e) {
          e.consume();
        }

        public void keyPressed(KeyEvent e) {
          if (e.getKeyCode() == KeyEvent.VK_ENTER)
          if ((Calendar.getInstance().getTime().getTime() - lastGotFocus.getTime()) > 200){
              doClick();
              e.consume();
          }
        }

        public void keyReleased(KeyEvent e) {
          e.consume();
        }
        
      };
    addKeyListener(keylst);
    MouseListener l =
      new MouseListener() {
        public void mousePressed(MouseEvent e) {
        }

        public void mouseReleased(MouseEvent e) {
        }

        public void mouseEntered(MouseEvent e) {
          Lazy8Ledger.ShowContextHelp(HelpedButton.this.view, helpName ,helpField);
        }

        public void mouseExited(MouseEvent e) {
        }

        public void mouseClicked(MouseEvent e) {
        }
      };
    addMouseListener(l);
  }

}

