/**
 * com.mckoi.database.BooleanDataCell  27 Sep 1998
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A DataCell that represents a Boolean type.  A Boolean is a 1 bit value which
 * represents one of two states, on or off, true of false, etc.
 * <p>
 * @author Tobias Downer
 */

public final class BooleanDataCell extends DataCell {

  Boolean val;

  public BooleanDataCell(Boolean value) {
    this.val = value;
  }

  public BooleanDataCell() {
    this.val = null;
  }

  public Object getCell() {
    return val;
  }

  public int getExtractionType() {
    return com.mckoi.database.global.Types.DB_BOOLEAN;
  }

  public int compareTo(DataCell cell) {
    Boolean dest_val = ((BooleanDataCell) cell).val;

    // To quantify booleans, there are three possible states in ascending
    // order:
    //   null, false, true.
    // Therefore:
    //   false.compareTo(null) < 0
    //   null.compareTo(true) > 0
    //   false.compareTo(false) == 0

    // First handle case where one of the terms is null,

    if (val == null) {
      if (dest_val == null) {
        return 0;
      }
      else {
        return 1;
      }
    }

    if (dest_val == null) {
      return -1;
    }

    // Both are non-null,

    if (val.equals(dest_val)) {
      return 0;
    }
    else if (val.equals(Boolean.TRUE)) {
      // Implies dest_val == false
      return 1;
    }
    else {
      return -1;
    }
  }

  public int sizeof() {
    return 1;
  }

  public int currentSizeOf() {
    return sizeof();
  }

  private static final byte NULL_FLAG  = 0;
  private static final byte TRUE_FLAG  = 1;
  private static final byte FALSE_FLAG = 2;

  public void writeTo(DataOutput out) throws IOException {
    byte flag = NULL_FLAG;
    if (val != null) {
      if (val.equals(Boolean.TRUE)) {
        flag = TRUE_FLAG;
      }
      else {
        flag = FALSE_FLAG;
      }
    }
    out.writeByte(flag);
  }

  public void readFrom(DataInput in) throws IOException {
    // Check: Can't read a data cell twice.
    if (val != null) {
      // ISSUE: This won't work if we've previously read a 'null' value.
      //   This is not likely to be an error however.
      throw new Error("Tried to overwrite value.");
    }

    byte flag = in.readByte();
    if (flag == NULL_FLAG) {
      val = null;
    }
    else if (flag == TRUE_FLAG) {
      val = Boolean.TRUE;
    }
    else {
      val = Boolean.FALSE;
    }
  }

}
