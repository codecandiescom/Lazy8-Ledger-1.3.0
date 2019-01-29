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
package org.lazy8.nu.ledger.jdbc;

import java.sql.*;
import org.lazy8.nu.util.gen.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class UniqNumGenerator {

  /**
   *  Constructor for the UniqNumGenerator object
   */
  public UniqNumGenerator() { }

  /**
   *  Description of the Method
   *
   *@param  NumbersName  Description of the Parameter
   *@param  minimum      Description of the Parameter
   *@param  maximum      Description of the Parameter
   *@param  compId       Description of the Parameter
   *@return              Description of the Return Value
   */
  public int GetUniqueNumber(String NumbersName, int minimum, int maximum,
      Integer compId) {
    int lNewNumber = minimum;
    try {
      ResultSet resultSet;
      DataConnection dc=DataConnection.getInstance(null);
      if(dc==null || !dc.bIsConnectionMade)return 0;
      PreparedStatement readTable =
          dc.con.prepareStatement(
          "SELECT UniqName,CompId,LastNumber FROM UniqNum WHERE UniqName LIKE ? AND CompId=?");
      readTable.setString(1, NumbersName);
      readTable.setInt(2, compId.intValue());
      resultSet = readTable.executeQuery();
      if (resultSet.next()) {
        int lNowNumber = resultSet.getInt(3);
        if ((lNowNumber + 1) <= maximum)
          lNowNumber++;
        else
          lNowNumber = minimum;
        /*
         *  change the database to reflect the new number
         */
        PreparedStatement updateTable =
            dc.con.prepareStatement(
            "UPDATE UniqNum SET LastNumber = ? WHERE UniqName LIKE ? AND CompId=?");
        updateTable.setInt(1, lNowNumber);
        updateTable.setString(2, NumbersName);
        updateTable.setInt(3, compId.intValue());
        updateTable.executeUpdate();
        lNewNumber = lNowNumber;
      }
      else {
        /*
         *  It does not exist, create it
         */
        PreparedStatement updateTable =
            dc.con.prepareStatement(
            "INSERT INTO UniqNum (UniqName,CompId,LastNumber) VALUES (?, ?, ?)");
        updateTable.setString(1, NumbersName);
        updateTable.setInt(2, compId.intValue());
        updateTable.setInt(3, minimum);
        updateTable.executeUpdate();
      }

    }
    catch (Exception e) {
      SystemLog.ProblemPrintln("Error:" + e.getMessage());
    }
    return lNewNumber;
  }
}

