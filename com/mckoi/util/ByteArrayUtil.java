/**
 * com.mckoi.util.ByteArrayUtil  26 Jun 2000
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

package com.mckoi.util;

/**
 * Static utilities for byte arrays.
 *
 * @author Tobias Downer
 */

public class ByteArrayUtil {

  /**
   * Returns the short at the given offset of the byte array.
   */
  public static final short getShort(byte[] arr, int offset) {
    int c1 = (((int) arr[offset + 0]) & 0x0FF);
    int c2 = (((int) arr[offset + 1]) & 0x0FF);
    return (short) ((c1 << 8) + (c2));
  }

  /**
   * Sets the short at the given offset of the byte array.
   */
  public static final void setShort(short value, byte[] arr, int offset) {
    arr[offset + 0] = (byte) ((value >>> 8) & 0x0FF);
    arr[offset + 1] = (byte) ((value >>> 0) & 0x0FF);
  }

  /**
   * Returns the int at the given offset of the byte array.
   */
  public static final int getInt(byte[] arr, int offset) {
    int c1 = (((int) arr[offset + 0]) & 0x0FF);
    int c2 = (((int) arr[offset + 1]) & 0x0FF);
    int c3 = (((int) arr[offset + 2]) & 0x0FF);
    int c4 = (((int) arr[offset + 3]) & 0x0FF);
    return (c1 << 24) + (c2 << 16) + (c3 << 8) + (c4);
  }

  /**
   * Sets the int at the given offset of the byte array.
   */
  public static final void setInt(int value, byte[] arr, int offset) {
    arr[offset + 0] = (byte) ((value >>> 24) & 0xFF);
    arr[offset + 1] = (byte) ((value >>> 16) & 0xFF);
    arr[offset + 2] = (byte) ((value >>>  8) & 0xFF);
    arr[offset + 3] = (byte) ((value >>>  0) & 0xFF);
  }

  /**
   * Returns the long at the given offset of the byte array.
   */
  public static final long getLong(byte[] arr, int offset) {
    int c1 = (((int) arr[offset + 0]) & 0x0FF);
    int c2 = (((int) arr[offset + 1]) & 0x0FF);
    int c3 = (((int) arr[offset + 2]) & 0x0FF);
    int c4 = (((int) arr[offset + 3]) & 0x0FF);
    int c5 = (((int) arr[offset + 4]) & 0x0FF);
    int c6 = (((int) arr[offset + 5]) & 0x0FF);
    int c7 = (((int) arr[offset + 6]) & 0x0FF);
    int c8 = (((int) arr[offset + 7]) & 0x0FF);
    return (c1 << 56) + (c2 << 48) + (c3 << 40) + (c4 << 32) +
           (c5 << 24) + (c6 << 16) + (c7 <<  8) + (c8);
  }

  /**
   * Sets the long at the given offset of the byte array.
   */
  public static final void setLong(long value, byte[] arr, int offset) {
    arr[offset + 0] = (byte) ((value >>> 56) & 0xFF);
    arr[offset + 1] = (byte) ((value >>> 48) & 0xFF);
    arr[offset + 2] = (byte) ((value >>> 40) & 0xFF);
    arr[offset + 3] = (byte) ((value >>> 32) & 0xFF);
    arr[offset + 4] = (byte) ((value >>> 24) & 0xFF);
    arr[offset + 5] = (byte) ((value >>> 16) & 0xFF);
    arr[offset + 6] = (byte) ((value >>>  8) & 0xFF);
    arr[offset + 7] = (byte) ((value >>>  0) & 0xFF);
  }


}
