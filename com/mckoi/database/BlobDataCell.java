/**
 * com.mckoi.database.BlobDataCell  05 Apr 1998
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

import com.mckoi.database.global.ByteLongObject;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.DataOutput;
import java.io.DataInput;

/**
 * This class represents a cell for storing blobs.  A blob is binary object
 * of any type and size such as a document, image, sound, etc.  It is not
 * possible to perform any type of query on a blob, therefore the 'compareTo'
 * method throws an Exception.
 *
 * @author Tobias Downer
 */

public class BlobDataCell extends DataCell {

  /**
   * The ByteLongObject that contains the binary data for this cell.
   */
  protected ByteLongObject blob;

  /**
   * The maximum size of BLOB's that can be stored in this cell.
   */
  protected int maximum_size;


  public BlobDataCell(int maximum_size, ByteLongObject blob) {
    if (blob != null && blob.length() > maximum_size) {
      throw new Error("BLOB in BlobDataCell too large for given max length.");
    }
    else if (maximum_size < 0) {
      throw new Error("Maximum BLOB size must be >= 0.");
    }
    this.maximum_size = maximum_size;
    this.blob = blob;
  }

  public BlobDataCell(int maximum_size) {
    if (maximum_size < 0) {
      throw new Error("Maximum BLOB size must be >= 0.");
    }
    this.maximum_size = maximum_size;
    this.blob = null;
  }

  public Object getCell() {
    return blob;
  }

  public int getExtractionType() {
    return com.mckoi.database.global.Types.DB_BLOB;
  }

  public int compareTo(DataCell cell) {
    throw new Error(
                    "Unable to perform relational operations on a BLOB.");
  }

  public int sizeof() {
    return maximum_size + 4;
  }

  public int currentSizeOf() {
    if (blob != null) {
      return blob.length() + 4;
    }
    else {
      return 4 + 8;
    }
  }

  public void writeTo(DataOutput out) throws IOException {
    if (blob != null) {
      out.writeInt(blob.length());
      out.write(blob.getByteArray());
    }
    else {
      out.writeInt(-1);
    }
  }

  public void readFrom(DataInput in) throws IOException {
    int len = in.readInt();
    if (len != -1) {
      byte[] buf = new byte[len];
      in.readFully(buf, 0, len);
      blob = new ByteLongObject(buf);
    }
    else {
      blob = null;
    }
  }

}
