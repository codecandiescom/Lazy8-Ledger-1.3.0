/**
 * com.mckoi.database.DecimalDataCell  04 Apr 1998
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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This represents a NUMERIC value to be stored in the database.
 * This is the prefered way to store numbers because unlike floats and doubles
 * it doesn't lose any precision.
 * <p>
 * NOTE: The number overflows at 2^336.  This means in base 10 there's about
 * 100 digits to play with.
 * <p>
 * @author Tobias Downer
 */

public class DecimalDataCell extends DataCell {

  /**
   * The maximum number of bytes a decimal can take up when stored in a file.
   */
  private final static int MAX_STORAGE_BYTES = 48;

  boolean    is_null = false;

  BigDecimal val = null;

  BigInteger bigint;
  int        scale;

  public DecimalDataCell(byte[] bytes, int scale) {
    is_null = false;
    this.scale = scale;
    bigint = new BigInteger(bytes);
    val = new BigDecimal(bigint, scale);
  }

  public DecimalDataCell(BigDecimal val) {
    if (val == null) {
      is_null = true;
    }
    else {
      is_null = false;
      this.val = val;
      this.scale = val.scale();
      this.bigint = val.movePointRight(scale).toBigInteger();
    }

  }

  public DecimalDataCell() {
    is_null = true;
  }

  public Object getCell() {
    return val;
  }

  public int getExtractionType() {
    return com.mckoi.database.global.Types.DB_NUMERIC;
  }

  public int compareTo(DataCell cell) {
    DecimalDataCell c = (DecimalDataCell) cell;
    if (!c.is_null) {
      if (!is_null) {
        return val.compareTo(c.val);
      }
      else {
        return -1;
      }
    }
    else {
      if (!is_null) {
        return 1;
      }
      else {
        return 0;
      }
    }
  }

  public int sizeof() {
    return 2 + 2 + MAX_STORAGE_BYTES;
  }

  public int currentSizeOf() {
    return sizeof();
  }

  public void writeTo(DataOutput out) throws IOException {
    if (val != null) {
      byte[] buf = bigint.toByteArray();
      out.writeShort((short) scale);
      out.writeShort((short) buf.length);
      out.write(buf);
    }
    else {
      out.writeShort(30000);
    }
  }

  public void readFrom(DataInput in) throws IOException {
    // Check: Do we already have a value set?
    if (is_null == false) {
      // ISSUE: This could cause corruption if we recycle this object and
      //   it's still being referenced.  There's a bug that could break
      //   immutability of this object.
      throw new Error("Tried to overwrite value.");
    }

    scale = in.readShort();
    if (scale == 30000) {
      is_null = true;
      val = null;
    }
    else {
      is_null = false;
      int length = in.readShort();
      byte[] buf = new byte[length];
      in.readFully(buf, 0, length);

      bigint = new BigInteger(buf);
      val = new BigDecimal(bigint, scale);
    }
  }

}
