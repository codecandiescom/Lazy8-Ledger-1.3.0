/**
 * com.mckoi.database.StringDataCell  08 Mar 1998
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

import java.text.Collator;
import java.text.CollationKey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A DataCell that represents a String type.  A String data cell must always
 * have a 'maximum length'.  That is the number of characters that can be
 * stored in the cell.  Even a 'null' StringDataCell must have some information
 * regarding the size of the cell.
 * <p>
 * Note that a StringDataCell is sorted lexicographically.  For strings that
 * are ordered by text Collation use the StringCollationDataCell.
 *
 * @author Tobias Downer
 */

public final class StringDataCell extends DataCell {

  String val;
  int maximum_size;


  public StringDataCell(int max_length, String val) {
    if (max_length >= 0) {
      this.val = val;
      maximum_size = max_length;
      if (val != null && val.length() > maximum_size) {
        throw new Error("String in StringDataCell too large for given max length.");
      }
    }
    else {
      throw new Error("StringDataCell fixed length must be greater than 0");
    }
  }

  public StringDataCell(int max_length) {
    this.val = null;
    if (max_length >= 0) {
      maximum_size = max_length;
    }
    else {
      throw new Error("StringDataCell fixed length must be greater than 0");
    }
  }

  public Object getCell() {
    return val;
  }

  public int getExtractionType() {
    return com.mckoi.database.global.Types.DB_STRING;
  }

  public int compareTo(DataCell cell) {

//    String d = ((StringDataCell) cell).val;
    Object d = cell.getCell();
    if (d != null) {
      if (val != null) {
        return val.compareTo((String) d);
      }
      else {
        return -1;
      }
    }
    else {
      if (val != null) {
        return 1;
      }
      else {
        return 0;
      }
    }
  }

  public int sizeof() {
    return (maximum_size * 2) + 4;
  }

  public int currentSizeOf() {
    if (val != null) {
      return (val.length() * 2) + 4;
    }
    else {
      return 4 + 2 + 4;
    }
  }

  public void writeTo(DataOutput out) throws IOException {
    if (val != null) {
      // Write chars to the stream.
      out.writeInt(val.length());
      out.writeChars(val);
    }
    else {
      out.writeInt(-1);
    }
  }

  public void readFrom(DataInput in) throws IOException {
    // Check: Can't read a data cell twice.
    if (val != null) {
      // ISSUE: This won't work if we've previously read a 'null' value.
      //   THIS COULD CAUSE ERRORS/CORRUPTION IF THIS METHOD CALLED MORE THAN
      //   ONCE (FOR EXAMPLE, RECYCLING A DATACELL).
      throw new Error("Tried to overwrite value.");
    }

    this.val = readStringFrom(in);

  }

  public static String readStringFrom(DataInput in) throws IOException {
    int len = in.readInt();
    if (len != -1) {
      StringBuffer sbuf = new StringBuffer(len + 1);
      for (int i = len; i > 0; --i) {
        sbuf.append(in.readChar());
      }
      // NOTE: We intern the string here so as to save some memory.
      String rtn = new String(sbuf);
      return rtn.intern();
    }
    else {
      return null;
    }
  }

}
