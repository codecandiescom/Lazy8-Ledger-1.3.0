/**
 * com.mckoi.database.DataCell  08 Mar 1998
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

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/**
 * Represents a single cell of information in a table.  A cell exhibits certain
 * properties, such as whether it is greater than, equal or less than another
 * cell.  And properties about storing the cell, such as the cell size.
 * <p>
 * We may also construct a 'null' type which only describes the properties
 * the cell, as opposed to describing the properties _and_ a piece of data.
 *
 * @author Tobias Downer
 */

public abstract class DataCell implements java.io.Serializable {

  /**
   * Returns true if the DataCell has been set to 'null'.  This is used to
   * check in the database 'add' and 'alter' commands whether the column can
   * be set to null.
   */
  public boolean isNull() {
    return (getCell() == null);
  }

  /**
   * Returns an Object that represents the cell contents.  In other words,
   * this is the actual object that describes the direct contents of the
   * cell this object represents.  Below is a list of Object types that
   * this function should return for the different cell types:
   *   String  =  String
   *   Boolean =  Boolean
   *   Decimal =  BigDecimal
   *   Time    =  Date
   *   Binary  =  ByteLongObject
   * It returns 'null' if the cell contents have explicitly been set to 'null'
   */
  public abstract Object getCell();

  /**
   * Returns a type indentifier that describes the object returned by the
   * call to 'getCell()'.  This method is used when extracting the records
   * from the database DataCell objects and writing them to a stream.
   */
  public abstract int getExtractionType();

  /**
   * Compares this DataCell with another of the same type.  Returns a negative
   * number, 0 or a positive number to indicate whether this object is less
   * than, equal or greater than the parameter cell.
   * A 'null' cell is always lower than any other non-null cell.
   */
  public abstract int compareTo(DataCell cell);

  /**
   * A method for describing the size property of the cell.  This is used to
   * determine how much space is allocated in the database file to store the
   * cell.  Therefore this method returns the number of octets needed to
   * to store all the information about the data in the cell.
   */
  public abstract int sizeof();

  /**
   * Returns an approximate value that indicates the size that this data cell
   * takes up in memory.  This is used when objects are cached so the system
   * can tell how much memory is being consumed by the cache.
   */
  public abstract int currentSizeOf();

  /**
   * Writes the contents of the cell to a DataOutput 'stream'.  It is not
   * necessary to write out all the bytes count as returned by 'sizeof()'
   * however it should never write out more bytes than returned by 'sizeof()'
   */
  public abstract void writeTo(DataOutput out) throws IOException;

  /**
   * Reads the contents of the cell in from a DataInput 'stream'.  This
   * assumes the cell has been previously allocated as a 'null' cell.
   * It is not necessary to read in all the bytes count as returned by
   * 'sizeof()'
   */
  public abstract void readFrom(DataInput in) throws IOException;

  /**
   * Converts this DataCell as a human readable string.
   */
  public String toString() {
    Object cell = getCell();
    if (cell == null) {
      return "null";
    }
    return cell.toString();
  }

}
