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
package org.lazy8.nu.util.gen;

import java.util.*;
import java.text.*;
import java.awt.*;
import java.io.*;
import javax.swing.JOptionPane;
/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class Translator {
  private static String PresentLanguage = new String();
  /**
   *  Description of the Field
   */
  public static Properties presentLanguage;

  /**
   *  Gets the translation attribute of the Translator class
   *
   *@param  toTranslate  Description of the Parameter
   *@return              The translation value
   */
  public static String getTranslation(String toTranslate) {
    Initialize();
    String returnValue = presentLanguage.getProperty("lazy8ledgerTRANS-"+toTranslate);
    if (returnValue == null) {
      presentLanguage.setProperty("lazy8ledgerTRANS-"+toTranslate, toTranslate);
      returnValue = toTranslate;
    }
    return returnValue;
  }

  /**
   *  Description of the Method
   *
   *@param  sLangCode  Description of the Parameter
   *@param  pList      Description of the Parameter
   *@return            Description of the Return Value
   */
  private static boolean loadLanguage(String sLangCode, Properties pList) {
    try {
      InputStream in = Fileio.getInputStream(sLangCode + ".bin", "lang");
      pList.load(in);
      in.close();
    }
    catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   *  Description of the Method
   */
  public static void reInitialize() {
    presentLanguage = null;
    Initialize();
  }
  private static void Initialize() {
    if (presentLanguage == null) {
      boolean bSetupLanguageExits = true;
      String sPresentLanguage = SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE);
      if (sPresentLanguage.length() == 0) {
        //lets try the system default even though it may not exist
        sPresentLanguage = System.getProperty("user.language");
        bSetupLanguageExits = false;
      }
      presentLanguage = new Properties();
      if (!loadLanguage(sPresentLanguage, presentLanguage)) {
        SystemLog.ErrorPrintln("Could not find language '" + sPresentLanguage
             + "' , now trying english");
        //make a last desperate attemp with default of english
        sPresentLanguage = "en";
        bSetupLanguageExits = false;
        if (!loadLanguage(sPresentLanguage, presentLanguage)) {
          JOptionPane.showMessageDialog(null,
              "Cannot find file Lang.en.bin please install this file and restart",
              "MAJOR ERROR",
              JOptionPane.PLAIN_MESSAGE);
          SystemLog.ErrorPrintln(
              "Cannot find file Lang.en.bin please install this file and restart");
        }
        else
            if (!bSetupLanguageExits)
          SetupInfo.setProperty(SetupInfo.PRESENT_LANGUAGE, sPresentLanguage);
      }
      else
          if (!bSetupLanguageExits)
        SetupInfo.setProperty(SetupInfo.PRESENT_LANGUAGE, sPresentLanguage);
    }
    else
      return;
  }

}

