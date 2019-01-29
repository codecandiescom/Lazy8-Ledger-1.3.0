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
import java.awt.*;
import java.awt.event.*;
import javax.swing.text.*;
import javax.swing.text.rtf.*;
import java.util.*;
import org.lazy8.nu.util.gen.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public abstract class SerialHelpDialogs extends JDialog {
  protected JTextPane m_monitor;
  protected JButton leftButton;
  protected JButton rightButton;
  protected JScrollPane ps;
  protected boolean isExitButton = false;
  protected JFrame frameParent;

  final static int DIALOG_WIDTH = 600;
  final static int DIALOG_HEIGHT = 300;

  /**
   *  Constructor for the CreateNewCompany object
   *
   *@param  isKillOnExitin  Description of the Parameter
   *@param  frame           Description of the Parameter
   */
  public  SerialHelpDialogs(JFrame frame,String title,boolean isExitButton ) {
    super(frame, title,true);
    frameParent = frame;
    this.isExitButton=isExitButton;

    getContentPane().setLayout(new BorderLayout());
    StyleContext m_context;
    DefaultStyledDocument m_doc;
    RTFEditorKit m_kit;
    m_monitor = new JTextPane();
/*    m_kit = new RTFEditorKit();
    m_monitor.setEditorKit(m_kit);
    m_context = new StyleContext();
    m_doc = new DefaultStyledDocument(m_context);
    m_monitor.setDocument(m_doc);
*/
    m_monitor.setEnabled(false);
    ps = new JScrollPane(m_monitor);
    Dimension ddm=new Dimension(DIALOG_WIDTH,DIALOG_HEIGHT*2/3);
    ps.setPreferredSize(ddm);
    ps.setMinimumSize(ddm);

    m_monitor.setCaretPosition(0);
    //attempt to move the cursor up

    showFirstScreen();

  }
  protected abstract void showFirstScreen();
  public JComponent addExitButton(){
    if(isExitButton){
      JButton exitButton = new JButton(Translator.getTranslation("Exit"));
      exitButton.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            setVisible(false);
            dispose();
          }
        }
          );
      return exitButton;        
      
    }else{
      return new JPanel();
    }
  }
}

