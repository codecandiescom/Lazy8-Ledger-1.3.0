/**
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
 *
 *@author     Administrator
 *@created    den 7 mars 2002
 */
package org.lazy8.nu.util.gen;


/**
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */

public class StringBinaryConverter {

  /**
   *  Description of the Method
   *
   *@param  ss  Description of the Parameter
   *@return     Description of the Return Value
   */
  public static byte[] StringToBinary(String ss) {
    byte bb[] = new byte[255];

    try {
      bb = ss.getBytes("UTF-8");
    }
    catch (Exception e1) {
      try {
        bb = ss.getBytes("UTF-16BE");
      }
      catch (Exception e2) {
        try {
          bb = ss.getBytes("UTF-16LE");
        }
        catch (Exception e3) {
          try {
            bb = ss.getBytes("UTF-16");
          }
          catch (Exception e4) {
            SystemLog.ProblemPrintln("Major Error:" + e4.getMessage());
          }
        }
      }
    }
    return bb;
  }

  /**
   *  Description of the Method
   *
   *@param  bb  Description of the Parameter
   *@return     Description of the Return Value
   */
  public static String BinaryToString(byte[] bb) {

    String ss = new String();

    try {
      ss = new String(bb, "UTF-8");
    }
    catch (Exception e1) {
      try {
        ss = new String(bb, "UTF-16BE");
      }
      catch (Exception e2) {
        try {
          ss = new String(bb, "UTF-16LE");
        }
        catch (Exception e3) {
          try {
            ss = new String(bb, "UTF-16");
          }
          catch (Exception e4) {
            SystemLog.ProblemPrintln("Major Error:" + e4.getMessage());
          }
        }
      }
    }
    return ss;
  }
}



