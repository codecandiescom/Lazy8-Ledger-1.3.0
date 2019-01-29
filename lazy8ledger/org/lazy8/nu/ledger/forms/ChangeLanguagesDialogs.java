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
import java.sql.*;
import java.io.*;
import java.awt.event.*;
import javax.swing.text.*;
import javax.swing.text.rtf.*;
import java.util.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.util.Log;
import lazy8ledger.*;
import org.gjt.sp.jedit.jEdit;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class ChangeLanguagesDialogs extends SerialHelpDialogs {
  public  ChangeLanguagesDialogs(JFrame frame,boolean isExitButtons) {
    super(frame,Translator.getTranslation("Getting started.  Basic settings."),
      isExitButtons);
  }
  JComboBox jLangCombo;
  ArrayList langKey ;

  public void showFirstScreen() {
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Select one of the following languages for this program") 
    	+ "\n" +
        Translator.getTranslation("Press NEXT to continue."));
    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          SetupInfo.setProperty(SetupInfo.PRESENT_LANGUAGE,(String)langKey.get(jLangCombo.getSelectedIndex()));
	  
	  //copy all language properties to the main jEdit property archive
	  Properties jLazy8LangProps =  new Properties();
	  try{
	    jLazy8LangProps.load(Fileio.getInputStream(
	      (String)langKey.get(jLangCombo.getSelectedIndex()) + ".bin", "lang"));
	  }catch(IOException ee)
	  {
	     Log.log(Log.ERROR,this,"Cannot open file="+
	       (String)langKey.get(jLangCombo.getSelectedIndex()) + ".bin  : error="+ee);
          }
	  String nameElement;
          for (Enumeration e = jLazy8LangProps.propertyNames() ; e.hasMoreElements() ;) {
            nameElement=(String)e.nextElement();
	    if(! nameElement.startsWith("lazy8ledgerTRANS-"))
              jEdit.setProperty(nameElement,jLazy8LangProps.getProperty(nameElement));
          }
          Translator.reInitialize();
          selectLanguageForNumberStage();
        }
      }
        );
    JPanel wholeSouth=new JPanel();
    wholeSouth.setLayout(new GridLayout(2, 1));
    JPanel panelLanguages=new JPanel();
    panelLanguages.setLayout(new GridLayout(2, 2));
    wholeSouth.add(panelLanguages);    
    
    panelLanguages.add(new HelpedLabel(Translator.getTranslation("Default language"),
        "Languages", "setup",null));
    jLangCombo = new JComboBox();
    panelLanguages.add(jLangCombo);
    
    langKey = new ArrayList();
    JComboBox[] boxes = new JComboBox[1];
    ArrayList[] arrays = new ArrayList[1];
    boxes[0] = jLangCombo;
    arrays[0] = langKey;

    TranslatorPane.GetAllLocales(boxes, arrays, true);
    String sSelectedLang = SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE);
    if (sSelectedLang.compareTo("") == 0)
      sSelectedLang=System.getProperty("user.language");
      if (sSelectedLang==null || sSelectedLang.compareTo("") == 0)
        sSelectedLang = new String("en");
    int j = 0;
    String nextLang;
    for (ListIterator i = langKey.listIterator(0); i.hasNext(); j++){
      nextLang=(String) i.next();
      Log.log(Log.DEBUG,this,"languages="+nextLang);
      if (sSelectedLang.compareTo(nextLang) == 0) {
        jLangCombo.setSelectedIndex(j);
        break;
      }
    }

    panelLanguages.add(new JLabel());
    panelLanguages.add(new JLabel());
        
    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    wholeSouth.add(jPanel1);    
    getContentPane().add(wholeSouth, BorderLayout.SOUTH);
    
    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    setVisible(true);
  }
  public void selectLanguageForNumberStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    //might be in another language this time....
    setTitle(Translator.getTranslation("Getting started.  Basic settings."));
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Select again a language.  This time, most languages in the world are included.  This will be used to define what date and number formats to use for this program.") + "\n" +
        Translator.getTranslation("Press NEXT to continue."));
    
    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          SetupInfo.setProperty(SetupInfo.NUMBER_FORMAT_LANGUAGE,(String)langKey.get(jLangCombo.getSelectedIndex()));
          System.setProperty("user.language",(String)langKey.get(jLangCombo.getSelectedIndex()));
          Translator.reInitialize();
          selectCountryStage();
        }
      }
        );
    JPanel wholeSouth=new JPanel();
    wholeSouth.setLayout(new GridLayout(2, 1));
    JPanel panelLanguages=new JPanel();
    panelLanguages.setLayout(new GridLayout(2, 2));
    wholeSouth.add(panelLanguages);    

    panelLanguages.add(new HelpedLabel(Translator.getTranslation("Default language"),
        "Languages", "setup",null));
    jLangCombo = new JComboBox();
    panelLanguages.add(jLangCombo);
    langKey = new ArrayList();
    JComboBox[] boxes = new JComboBox[1];
    ArrayList[] arrays = new ArrayList[1];
    boxes[0] = jLangCombo;
    arrays[0] = langKey;

    TranslatorPane.GetAllLocales(boxes, arrays, false);
    String sSelectedLang = System.getProperty("user.language");
    if (sSelectedLang==null || sSelectedLang.compareTo("") == 0)
      sSelectedLang=SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE);
      if (sSelectedLang.compareTo("") == 0)
        sSelectedLang = new String("en");
    int j = 0;
    for (ListIterator i = langKey.listIterator(0); i.hasNext(); j++)
      if (sSelectedLang.compareTo((String) i.next()) == 0) {
        jLangCombo.setSelectedIndex(j);
        break;
      }

    panelLanguages.add(new JLabel());
    panelLanguages.add(new JLabel());
        
    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    wholeSouth.add(jPanel1);    
    getContentPane().add(wholeSouth, BorderLayout.SOUTH);
    
    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }
  public void selectCountryStage() {
    //setVisible(false);
    getContentPane().removeAll();
    getContentPane().setLayout(new BorderLayout());
    getContentPane().add(ps, BorderLayout.CENTER);
    m_monitor.setText("");
    m_monitor.replaceSelection(Translator.getTranslation("Select a country.  This will be used to further define what date and number formats to use for this program.") + "\n" +
        Translator.getTranslation("Press NEXT to continue."));

    rightButton = new JButton(Translator.getTranslation("Next"));

    rightButton.addActionListener(
      new java.awt.event.ActionListener() {
        public void actionPerformed(java.awt.event.ActionEvent evt) {
          SetupInfo.setProperty(SetupInfo.PRESENT_COUNTRY,(String)langKey.get(jLangCombo.getSelectedIndex()));
          System.setProperty("user.region",(String)langKey.get(jLangCombo.getSelectedIndex()));
          try {
            Locale lc = new Locale(System.getProperty("user.language"),
                System.getProperty("user.region"));
            Locale.setDefault(lc);
          }
          catch (Exception cccc) {
          }
           setVisible(false);
        }
      }
        );
    JPanel wholeSouth=new JPanel();
    wholeSouth.setLayout(new GridLayout(2, 1));
    JPanel panelLanguages=new JPanel();
    panelLanguages.setLayout(new GridLayout(2, 2));
    wholeSouth.add(panelLanguages);

    Locale[] locales = Locale.getAvailableLocales();
    langKey = new ArrayList();
    jLangCombo = new JComboBox();
    String currentLanguage = System.getProperty("user.language");
    String currentCountry = SetupInfo.getProperty(SetupInfo.PRESENT_COUNTRY);
    if (currentCountry.compareTo("") == 0)
      currentCountry=System.getProperty("user.region");
    for (int j = 0; j < locales.length; j++) {
      if(locales[j].getLanguage().compareTo(currentLanguage)==0 && locales[j].getCountry().length()!=0){
          Log.log(Log.DEBUG,this,"Locale="+locales[j].getCountry()+";"+locales[j].getDisplayCountry());
        int i;
        for (i = 0; i < langKey.size(); i++) 
          if(((String)langKey.get(i)).compareTo((String)(locales[j].getCountry()))==0)
            break;
        if(i==langKey.size()){
          langKey.add(locales[j].getCountry());
          jLangCombo.addItem(locales[j].getDisplayCountry());
          if(currentCountry!=null && currentCountry.compareTo(locales[j].getCountry())==0)
            jLangCombo.setSelectedIndex(i);
        }
      }
    }
    if (langKey.size()<=1){
      //no need to force them to do this dialog if they only have one choice.  Just confinue
      if(langKey.size()==1){
        SetupInfo.setProperty(SetupInfo.PRESENT_COUNTRY,(String)langKey.get(0));
        SetupInfo.store();
        System.setProperty("user.region",(String)langKey.get(0));
      }
      try {
        Locale lc = new Locale(System.getProperty("user.language"),
            System.getProperty("user.region"));
        Locale.setDefault(lc);
      }
      catch (Exception cccc) {
      }
      setVisible(false);
      return;
    }
    panelLanguages.add(new JLabel(Translator.getTranslation("Country")));
    panelLanguages.add(jLangCombo);
    panelLanguages.add(new JLabel());
    panelLanguages.add(new JLabel());

    JPanel jPanel1 = new JPanel();
    jPanel1.setLayout(new GridLayout(1, 3));
    jPanel1.add(addExitButton());
    jPanel1.add(new JPanel());
    jPanel1.add(rightButton);
    wholeSouth.add(jPanel1);    
    getContentPane().add(wholeSouth, BorderLayout.SOUTH);

    pack();
    //setSize(DIALOG_WIDTH, DIALOG_HEIGHT);
    setLocationRelativeTo(frameParent);
    //setVisible(true);
  }
}

