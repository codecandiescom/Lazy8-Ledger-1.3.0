/**
 * com.mckoi.database.global.ObjectTransfer  20 Jul 2000
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

package com.mckoi.database.global;

import java.io.*;
import java.util.Date;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Provides static methods for transfering different types of objects over
 * a Data input/output stream.
 *
 * @author Tobias Downer
 */

public class ObjectTransfer {

  /**
   * Makes an estimate of the size of the object.  This is useful for making
   * a guess for how much this will take up.
   */
  public static int size(Object ob) throws IOException {
    if (ob == null || ob instanceof NullObject) {
      return 9;
    }
    else if (ob instanceof String) {
      return (ob.toString().length() * 2) + 9;
    }
    else if (ob instanceof BigDecimal) {
      return 15 + 9;
    }
    else if (ob instanceof Date) {
      return 8 + 9;
    }
    else if (ob instanceof Boolean) {
      return 2 + 9;
    }
    else if (ob instanceof ByteLongObject) {
      return ((ByteLongObject) ob).length() + 9;
    }
    else {
      throw new IOException("Unrecognised type.");
    }
  }

  /**
   * Writes an object to the data output stream.
   */
  public static void writeTo(DataOutputStream out, Object ob)
                                                           throws IOException {
    if (ob == null || ob instanceof NullObject) {
      out.writeByte(1);
    }
    else if (ob instanceof String) {
      String str = (String) ob;

      // All strings send as char array,
      out.writeByte(18);
      out.writeInt(str.length());
      out.writeChars(str);

//      // If the string is too large to be transfered via UTF ( >64k ) then
//      // send as a char array
//      if (str.length() >= 65535) {
//        out.writeByte(18);
//        out.writeInt(str.length());
//        out.writeChars(str);
//      }
//      else {
//        out.writeByte(3);
//        out.writeUTF(str);
//      }

    }
    else if (ob instanceof BigDecimal) {
      BigDecimal n = (BigDecimal) ob;
      out.writeByte(6);
      out.writeInt(n.scale());

      // NOTE: This method is only available in 1.2.  This needs to be
      //   compatible with 1.1 so we use a slower method,
//      BigInteger unscaled_val = n.unscaledValue();
      // NOTE: This can be swapped out eventually when we can guarentee
      //   everything is 1.2 minimum.
      BigInteger unscaled_val = n.movePointRight(n.scale()).toBigInteger();

      byte[] buf = unscaled_val.toByteArray();
      out.writeInt(buf.length);
      out.write(buf);
    }
    else if (ob instanceof Date) {
      Date d = (Date) ob;
      out.writeByte(9);
      out.writeLong(d.getTime());
    }
    else if (ob instanceof Boolean) {
      Boolean b = (Boolean) ob;
      out.writeByte(12);
      out.writeBoolean(b.booleanValue());
    }
    else if (ob instanceof ByteLongObject) {
      ByteLongObject barr = (ByteLongObject) ob;
      out.writeByte(15);
      byte[] arr = barr.getByteArray();
      out.writeLong(arr.length);
      out.write(arr);
    }
    else {
      throw new IOException("Unrecognised type.");
    }
  }

  /**
   * Writes an object from the data input stream.
   */
  public static Object readFrom(DataInputStream in) throws IOException {
    byte type = in.readByte();

    if (type == 1) {
      return NullObject.NULL_OBJ;
    }
    else if (type == 3) {
      return in.readUTF();
    }
    else if (type == 6) {
      int scale = in.readInt();
      int blen = in.readInt();
      byte[] buf = new byte[blen];
      in.readFully(buf);

      BigInteger bi = new BigInteger(buf);
      return new BigDecimal(bi, scale);
    }
    else if (type == 9) {
      long time = in.readLong();
      return new Date(time);
    }
    else if (type == 12) {
      return new Boolean(in.readBoolean());
    }
    else if (type == 15) {
      long size = in.readLong();
      byte[] arr = new byte[(int) size];
      in.read(arr, 0, (int) size);
      return new ByteLongObject(arr);
    }
    else if (type == 18) {
      // Handles strings > 64k
      int len = in.readInt();
      StringBuffer buf = new StringBuffer();
      while (len > 0) {
        buf.append(in.readChar());
        --len;
      }
      return new String(buf);
    }
    else {
      throw new IOException("Unrecognised type.");
    }
  }




}
