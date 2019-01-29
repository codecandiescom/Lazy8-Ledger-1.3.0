/*
 * MenuOptionPane.java - General options panel
 * Copyright (C) 1998, 1999, 2000, 2001 Slava Pestov
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package lazy8ledger;

import javax.swing.*;
import java.awt.event.*;
import java.awt.*;
import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.util.Log;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.ledger.forms.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;

public class LedgerGeneralOptionPane extends AbstractOptionPane
{
  private JFrame view;
	public LedgerGeneralOptionPane(JFrame view)
	{
		super(Translator.getTranslation("General"));
    this.view=view;
	}
	// protected members
	protected void _init()
	{
    addComponent(new JLabel(
    	Translator.getTranslation("After making any changes here you must restart jEdit")));
    addComponent(new JLabel(" "));
    addComponent(new JLabel(" "));
    menuButtonLevels=new JRadioButton[NUM_MENU_LEVELS];
    addComponent(createMenuSelectionPanel( menuButtonLevels,view));
    textField5 = new HelpedTextField("WordProcesser", "setup",view);

 
    JButton butLang=new HelpedButton(Translator.getTranslation("Default language"),
        "Languages", "setup",view);
    addComponent(butLang);
    butLang.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          //Get file name
          new ChangeLanguagesDialogs(view,true);
        }
      }
        );

    JPanel jpSearch = new JPanel();
    jpSearch.setLayout(new BorderLayout());

    jpSearch.add(new HelpedLabel(Translator.getTranslation("External word processor"),
        "WordProcesser", "setup",view),BorderLayout.WEST);

    JButton butsearch = new HelpedButton(Translator.getTranslation("Browse"),
        "Browse", "setup",view);

    butsearch.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          //Get file name
          JFileChooser fileDialog = new JFileChooser();
          fileDialog.setDialogTitle(
              Translator.getTranslation("Select a word processor"));
          if (fileDialog.showOpenDialog(null) ==
              JFileChooser.APPROVE_OPTION)
            textField5.setText(fileDialog.getSelectedFile().getPath());
        }
      }
        );
    jpSearch.add(butsearch,BorderLayout.CENTER);
    Dimension dm=textField5.getPreferredSize();
    dm.setSize(125,dm.height);
    textField5.setPreferredSize(dm);
    textField5.setMaximumSize(dm);
    textField5.setMinimumSize(dm);
    textField5.setText(SetupInfo.getProperty(SetupInfo.WORDPROCESSOR));
    jpSearch.add(textField5,BorderLayout.EAST);
    addComponent(jpSearch);
  }
  public static void saveMenuLevel(JRadioButton[] menuButton){
    //save the menu value
    int menuLevelBefore=jEdit.getIntegerProperty("lazy8ledger.menu.level", -1);
    int menuLevel=0;
    for(int i=0;i<NUM_MENU_LEVELS;i++){
      if(menuButton[i].isSelected()){
        jEdit.setIntegerProperty("lazy8ledger.menu.level", i);
        menuLevel=i;
        break;
      }
    }
    //if no change, get out
    if(menuLevel==menuLevelBefore)return;
    try{
      //erase all the menu properties
      Properties jLazy8GenMenuProps =  new Properties();
      jLazy8GenMenuProps.load(Fileio.getInputStream("programmeraccountant.bin", "props"));
      for (Enumeration e = jLazy8GenMenuProps.propertyNames() ; e.hasMoreElements() ;) {
        jEdit.resetProperty((String)(e.nextElement()));
      }
      Properties jLazy8AccMenuProps =  new Properties();
      jLazy8AccMenuProps.load(Fileio.getInputStream("accountant.bin", "props"));
      for (Enumeration e = jLazy8AccMenuProps.propertyNames() ; e.hasMoreElements() ;) {
        jEdit.resetProperty((String)(e.nextElement()));
      }
      //must add the translations for the jEdit menus and dialogs.
      String nameElement;
      for (Enumeration e = Translator.presentLanguage.propertyNames() ; e.hasMoreElements() ;) {
        nameElement=(String)e.nextElement();
        if( ! nameElement.startsWith("lazy8ledgerTRANS"))
            jEdit.setProperty(nameElement,Translator.presentLanguage.getProperty(nameElement));
      }
      //add all the new properties depending on the menu selection
      switch (menuLevel){
        case 0:
          for (Enumeration e = jLazy8AccMenuProps.propertyNames() ; e.hasMoreElements() ;) {
            nameElement=(String)e.nextElement();
            jEdit.setProperty(nameElement,jLazy8AccMenuProps.getProperty(nameElement));
          }
	  //bleed on over to the next case.  We need these properties also
        case 1:
          for (Enumeration e = jLazy8GenMenuProps.propertyNames() ; e.hasMoreElements() ;) {
            nameElement=(String)e.nextElement();
            jEdit.setProperty(nameElement,jLazy8GenMenuProps.getProperty(nameElement));
          }
          break;
	case 2:
	  //must remove all the translations for the jEdit menus and dialogs.
	  for (Enumeration e = Translator.presentLanguage.propertyNames() ; e.hasMoreElements() ;) {
	    nameElement=(String)e.nextElement();
	    if( ! nameElement.startsWith("lazy8ledger"))
	      jEdit.resetProperty(nameElement);
	  }
	  break;
      }
    }catch(IOException ee){
    }
  }
  public static JPanel createMenuSelectionPanel(JRadioButton[] menuButton,JFrame view){
    ButtonGroup group = new ButtonGroup();
    JPanel menuPanel=new JPanel();
    menuPanel.setBorder(new javax.swing.border.TitledBorder(
          Translator.getTranslation("Menu difficulty level")));
    menuPanel.setLayout(new GridLayout(NUM_MENU_LEVELS,1));
    for(int i=0;i<NUM_MENU_LEVELS;i++){
      menuButton[i]=new HelpedRadioButton(Translator.getTranslation("menu.level."+i),
      "menulevel", "setup",view);
      group.add(menuButton[i]);
      menuPanel.add(menuButton[i]);
    }
    
    if (jEdit.getIntegerProperty("lazy8ledger.menu.level", 0)<NUM_MENU_LEVELS)
      menuButton[jEdit.getIntegerProperty("lazy8ledger.menu.level", 0)].setSelected(true);
    else
      menuButton[0].setSelected(true);
    return menuPanel;
  }

	protected void _save()
	{
    saveMenuLevel(menuButtonLevels);
    //companyComponents.saveDefaults();
    SetupInfo.setProperty(SetupInfo.WORDPROCESSOR,
        textField5.getText().trim());
    SetupInfo.store();

	}

	// private members
  JRadioButton[] menuButtonLevels;
  public static final int NUM_MENU_LEVELS=3;
  private JTextField textField5;
  JCheckBox checkShowPassword;

  private JButton button1;

  private JCheckBox checkShowAdvancedMenus;
  private CompanyComponents companyComponents;
}
