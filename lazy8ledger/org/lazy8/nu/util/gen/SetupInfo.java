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
import java.io.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class SetupInfo {
  /**
   *  Description of the Field
   */
  public final static String ACCEPTED_COPYRIGHT = new String("INSTALLERREMOVE accepted copyright");
  /**
   *  Description of the Field
   */
  public final static String TRIAL_PERIOD_START_TIME = new String("INSTALLERREMOVE trial period start");
  /**
   *  Description of the Field
   */
  public final static String TRIAL_PERIOD_OVER_WARNING_SHOWN = new String("INSTALLERREMOVE trial period over");
  /**
   *  Description of the Field
   */
  public final static String WARNING_GOOD_ADD = new String("INSTALLDEFAULT_TRUE-warning good add");
  /**
   *  Description of the Field
   */
  public final static String WARNING_BEFORE_DELETE = new String("INSTALLDEFAULT_TRUE-warning before delete");
  /**
   *  Description of the Field
   */
  public final static String WARNING_AFTER_DELETE = new String("INSTALLDEFAULT_TRUE-warning after delete");
  /**
   *  Description of the Field
   */
  public final static String WARNING_CLEAR_AFTER_ADD = new String("INSTALLDEFAULT_TRUE-warning clear after add");
  /**
   *  Description of the Field
   */
  public final static String WARNING_CLEAR_AFTER_DELETE = new String("INSTALLDEFAULT_TRUE-warning clear after delete");
  /**
   *  Description of the Field
   */
  public final static String REQUIRE_LOGIN = new String("INSTALLDEFAULT_FALSE-require login");
  /**
   *  Description of the Field
   */
  public final static String WINDOW_STYLE = new String("INSTALLDEFAULT_metal-window style");
  /**
   *  Description of the Field
   */
  public final static String CONNECT_DRIVER = new String("INSTALLDEFAULT_com.mckoi.JDBCDriver-connect driver");
  /**
   *  Description of the Field
   */
  public final static String CONNECT_DATABASE = new String("INSTALLDEFAULT_blank database");
  /**
   *  Description of the Field
   */
  public final static String CONNECT_USERNAME = new String("INSTALLERREMOVE user name");
  /**
   *  Description of the Field
   */
  public final static String CONNECT_PASSWORD = new String("INSTALLERREMOVE password");
  /**
   *  Description of the Field
   */
  public final static String REPORT_TITLE_TAX = new String("INSTALLERREMOVE report title sales tax");
  /**
   *  Description of the Field
   */
  public final static String REPORT_TITLE_ACTIVITIES = new String("INSTALLERREMOVE report title activities");
  /**
   *  Description of the Field
   */
  public final static String REPORT_TITLE_RESULT = new String("INSTALLERREMOVE report title result");
  /**
   *  Description of the Field
   */
  public final static String REPORT_TITLE_ACCOUNTS = new String("INSTALLERREMOVE report title accounts");
  /**
   *  Description of the Field
   */
  public final static String DEFAULT_COMPANY = new String("INSTALLERREMOVE default company");
  /**
   *  Description of the Field
   */
  public final static String DEFAULT_PERIOD1 = new String("INSTALLERREMOVE default period1");
  /**
   *  Description of the Field
   */
  public final static String DEFAULT_PERIOD2 = new String("INSTALLERREMOVE default period2");
  /**
   *  Description of the Field
   */
  public final static String SHOW_ADVANCED_MENUS = new String("INSTALLDEFAULT_false-show advanced menus");
  /**
   *  Description of the Field
   */
  public final static String PRESENT_LANGUAGE = new String("INSTALLERREMOVE present language");
  /**
   *  Description of the Field
   */
  public final static String FAST_ACCOUNT_DIGITS = new String("INSTALLDEFAULT_4-fast entry number of digits");
  /**
   *  Description of the Field
   */
  public final static String WORDPROCESSOR = new String("INSTALLERREMOVE wordprocessor");
  /**
   *  It is either left justified or right. If this is true, then it is left
   */
  public final static String REPORT_NUMBER_JUSTIFY_LEFT = new String("INSTALLDEFAULT_false-report number justify left ");
  /**
   *  Description of the Field
   */
  public final static String REPORT_NUMBER_FORMAT = new String("INSTALLDEFAULT_1-report number format");
  /**
   *  Description of the Field
   */
  public final static String REPORT_NUMBER_SPECIALFORMAT = new String("INSTALLERREMOVE report number special format");
  /**
   *  It is either left justified or right. If this is true, then it is left
   */
  public final static String DATAENTRY_JUSTIFY_LEFT = new String("INSTALLDEFAULT_false-dataentry number justify left ");
  /**
   *  Description of the Field
   */
  public final static String DATAENTRY_NUMBER_FORMAT = new String("INSTALLDEFAULT_1-dataentry number format");
  /**
   *  Description of the Field
   */
  public final static String DATAENTRY_NUMBER_SPECIALFORMAT = new String("INSTALLERREMOVE dataentry number special format");
  /**
   *  Description of the Field
   */
  public final static String SHOW_ALWAYS_HELP = new String("INSTALLDEFAULT_true-dataentry number special format");
  /**
   *  Description of the Field
   */
  public final static String MAIN_FRAME_START_WIDTH = new String("INSTALLERREMOVE mainframe start width");
  /**
   *  Description of the Field
   */
  public final static String MAIN_FRAME_START_HEIGHT = new String("INSTALLERREMOVE mainframe start height");
  /**
   *  Description of the Field
   */
  public final static String MAIN_FRAME_START_X = new String("INSTALLERREMOVE mainframe start X");
  /**
   *  Description of the Field
   */
  public final static String MAIN_FRAME_START_Y = new String("INSTALLERREMOVE mainframe start Y");
  /**
   *  Description of the Field
   */
  public final static String INPUT_TABSTOP_CUSTOMER = new String("INSTALLDEFAULT_false input tabstop on customer");
  /**
   *  Description of the Field
   */
  public final static String ALLOW_TRANSACTION_CHANGES = new String("INSTALLDEFAULT_false allow transaction changes");
  public final static String SHOW_CONTEXTHELP = new String("INSTALLDEFAULT_true show context help");
  public final static String WARN_TO_DO_BACKUP = new String("INSTALLDEFAULT_true warn to do backup");
  public final static String PRESENT_COUNTRY = new String("INSTALLERREMOVE default country");
  public final static String NUMBER_FORMAT_LANGUAGE = new String("INSTALLERREMOVE number format language");
  public final static String REQUIRE_BACKUP_PASSWORD= new String("INSTALLDEFAULT_false require backup password");
  public final static String IS_INSTALL_ROUTINE_DONE= new String("INSTALLDEFAULT_false is install routine run");

  private static Properties setupProperties;
  private final static String fileName = new String("setup.bin");

  /**
   *  Constructor for the SetupInfo object
   */
  public SetupInfo() {
    Initialize();
  }

  /**
   *  Description of the Method
   */
  private static void Initialize() {
    if (setupProperties == null) {

      setupProperties = new Properties();
      try {

        InputStream in = Fileio.getInputStream(fileName, "props");
        setupProperties.load(in);
        in.close();
      }
      catch (Exception e) {
        SystemLog.ErrorPrintln("FAILED trying to find system parameters: " +
            e.getMessage());
        return;
      }
    }
    else
      return;
  }

  /**
   *  Sets the intProperty attribute of the SetupInfo class
   *
   *@param  Name    The new intProperty value
   *@param  iValue  The new intProperty value
   */
  public static void setIntProperty(String Name, int iValue) {
    setProperty(Name, (new Integer(iValue)).toString());
  }

  /**
   *  Gets the intProperty attribute of the SetupInfo class
   *
   *@param  Name  Description of the Parameter
   *@return       The intProperty value
   */
  public static int getIntProperty(String Name) {
    if (getProperty(Name).compareTo("") == 0)
      return 0;
    else
      try {
        return Integer.parseInt(getProperty(Name));
      }
      catch (Exception e) {
        return 0;
      }

  }

  /**
   *  Gets the boolProperty attribute of the SetupInfo class
   *
   *@param  Name  Description of the Parameter
   *@return       The boolProperty value
   */
  public static boolean getBoolProperty(String Name) {
    boolean bReturnValue = false;

    if (getProperty(Name).compareTo("true") == 0)
      bReturnValue = true;

    return bReturnValue;
  }

  /**
   *  Sets the boolProperty attribute of the SetupInfo class
   *
   *@param  Name    The new boolProperty value
   *@param  bValue  The new boolProperty value
   */
  public static void setBoolProperty(String Name, boolean bValue) {
    if (bValue)
      setProperty(Name, "true");
    else
      setProperty(Name, "false");
  }

  /**
   *  Gets the property attribute of the SetupInfo class
   *
   *@param  Name  Description of the Parameter
   *@return       The property value
   */
  public static String getProperty(String Name) {
    Initialize();
    String rs = setupProperties.getProperty(Name);
    if (rs == null)
      rs = new String("");
    return rs;
  }

  /**
   *  Sets the property attribute of the SetupInfo class
   *
   *@param  Name      The new property value
   *@param  property  The new property value
   */
  public static void setProperty(String Name, String property) {
    Initialize();
    setupProperties.setProperty(Name, property);
  }

  /**
   *  Description of the Method
   */
  public static void store() {
    Initialize();
    try {
      File file = Fileio.getFile(fileName, "props", true, false);
      OutputStream out = new FileOutputStream(file);
      setupProperties.store(out, "Version 1.0");
      out.close();
    }
    catch (Exception e) {
      SystemLog.ErrorPrintln("FAILED trying to save system parameters: " +
          e.getMessage());
      return;
    }
  }
}


