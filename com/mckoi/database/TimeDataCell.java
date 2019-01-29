/**
 * com.mckoi.database.TimeDataCell  26 Mar 1998
 *
 * Mckoi SQL Database ( http://www.mckoi.com/database )
 * Copyright (C) 2000, 2001  Diehl and Associates, Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * Version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License Version 2 for more details.
 *
 * You should have received a copy of the GNU General Public License
 * Version 2 along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * Change Log:
 * 
 * 
 */

package com.mckoi.database;

import java.util.Date;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * Represents a cell of information that represents a given time.  The cell
 * contains a long value that represents the number of milliseconds since
 * the 1st Jan 1970.
 * <p>
 * NOTE: A long is 8 bytes long, however this cell takes up 16 bytes.  The
 * extra space is there just incase we need to save extra information about
 * the time in a legacy database.  Need to think about this...  We could just
 * write a new class to handle times that store the timezone it was stored in.
 * <p>
 * @author Tobias Downer
 */

public class TimeDataCell extends DataCell {

  /**
   * The long value that stores the number of seconds this time represents.
   */
  Date date;

  /**
   * The Constructor.
   */
  public TimeDataCell(long date) {
    this.date = new Date(date);
  }

  public TimeDataCell(Date date) {
    this.date = date;
  }

  public TimeDataCell() {
    this.date = null;
  }

  public Object getCell() {
    return date;
  }

  public int getExtractionType() {
    return com.mckoi.database.global.Types.DB_TIME;
  }

  public int compareTo(DataCell cell) {
    Date targ = ((TimeDataCell) cell).date;

    if (targ != null) {
      if (date != null) {
        // neither date nor targ are 'null'
        if (date.before(targ)) {
          return -1;
        }
        else if (date.equals(targ)) {
          return 0;
        }
        else {
          return 1;
        }
      }
      else {
        // If only date 'null' return less
        return -1;
      }
    }
    else {
      if (date != null) {
        // If only targ 'null' return greater
        return 1;
      }
      else {
        // If both targ and date 'null' then return equal
        return 0;
      }
    }

  }

  public int sizeof() {
    return 16;   // NOTE: we actually require 8 + 1 bytes to store state
  }

  public int currentSizeOf() {
    return sizeof();
  }

  public void writeTo(DataOutput out) throws IOException {
    if (date != null) {
      out.writeByte(1);
      out.writeLong(date.getTime());
    }
    else {
      out.writeByte(0);
    }
  }

  public void readFrom(DataInput in) throws IOException {
    byte stat = in.readByte();
    if (stat == 0) {
      date = null;
    }
    else {
      date = new Date(in.readLong());
    }
  }

//  public static Date readDateFrom(DataInput in) throws IOException {
//    return new Date(in.readLong());
//  }

}
