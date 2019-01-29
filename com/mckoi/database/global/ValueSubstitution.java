/**
 * com.mckoi.database.global.ValueSubstitution  11 May 1998
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

//import com.mckoi.util.StreamCopier;
import java.util.Date;
import java.math.BigDecimal;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

/**
 * This object represents a 'bridge' class between the data on the server and
 * the data the client understands.  Both the client and server understand
 * the ValueSubstitution object.  The client can use it to form constants
 * within a query, the server can use it to covert data received from the
 * client into DataCell objects to store away in the database.  The server
 * also sends back ValueSubstitution objects as the result of queries.
 * <p>
 * Therefore, this is a very important class.  It is the container for all
 * data types that the database understands and 'transmits'.
 * <p>
 * A nice thing about this class, is that it can serialize any core database
 * object type, even objects that implement the BlobInput interface.
 * <p>
 * NOTE: When serializing values of type == Types.BINARY, we handle the
 *   streamization of it differently.  Since the reference will be an
 *   InputStream, when we write the object we need to write the contents
 *   of the input stream, and when we read the object we need to write the
 *   binary data into a temporary store such as a ByteArrayInputStream.
 * <p>
 * @author Tobias Downer
 */

public final class ValueSubstitution implements java.io.Serializable {

  static final long serialVersionUID = 3420277522781024107L;

  /**
   * The type of the value substitution.
   * All the possible types are specified in the 'Types' class.
   */
  public int type;

  /**
   * The object itself.  This may be either a Date, a String, a BigDecimal,
   * or a BlobInput object.
   */
  public Object ob;

  /**
   * The Constructor.
   */
  public ValueSubstitution(int type, Object ob) {
    this.type = type;
    this.ob = ob;
  }

  /**
   * Functions for querying the contents of the container.
   */
  public int getType() {
    return type;
  }

  public Object getObject() {
    return ob;
  }

  /**
   * Creates a ValueSubstitution object given an Object.
   */
  public static final ValueSubstitution fromObject(Object ob) {
    if (ob instanceof String) {
      return new ValueSubstitution(Types.DB_STRING, ob);
    }
    else if (ob instanceof BigDecimal) {
      return new ValueSubstitution(Types.DB_NUMERIC, ob);
    }
    else if (ob instanceof Date) {
      return new ValueSubstitution(Types.DB_TIME, ob);
    }
    else if (ob instanceof Boolean) {
      return new ValueSubstitution(Types.DB_BOOLEAN, ob);
    }
    else if (ob instanceof ValueSubstitution) {
      return (ValueSubstitution) ob;
    }
    else if (ob instanceof com.mckoi.util.TimeFrame) {
      return new ValueSubstitution(
              Types.DB_NUMERIC, ((com.mckoi.util.TimeFrame) ob).getPeriod());
    }
    else {
      throw new Error("Don't understand type: " + ob.getClass());
    }
  }

  /**
   * This is the modified functions for serialization of this object.  This is
   * required so we can write and read values of type 'BINARY'.  Otherwise
   * the InputStream would not serialize.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {

    // Write out the member variables of this object

    out.writeInt(type);

//    // Special case if the object is a blob (InputStream)
//    if (type == Types.DB_BINARY) {
//
//      BlobInput blob = (BlobInput) ob;
//
//      // NOTE: Cast blob length from 'long' to 'int'
//      int size = (int) blob.length();
//      out.writeInt(size);
//
//      // Copy the contents of the blob stream to the object writer.
//
//      InputStream in = blob.getInputStream();
//      int written = StreamCopier.copy(in, out);
//      in.close();
//      if (written != size) {
//        throw new IOException("Incorrect size comparison when writing out a binary stream");
//      }
//
//    }
//
//    // Anything other than a binary is written out as normal.
//    else {

      out.writeObject(ob);

//    }

  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

    // Read in the member variables of this object.

    type = in.readInt();

//    // Special case if the type is a blob (Binary stream)
//    if (type == Types.DB_BINARY) {
//
//      // Copy contents into a byte array, and set 'ob' to a new
//      // ByteArrayInputStream object to reflect the data.
//
//      // ISSUE: Copying into a byte[] array probably won't be efficient for
//      //   very large binary objects.  It may be better to copy into a
//      //   temporary file.
//
//      int image_length = in.readInt();
//      byte[] buf = new byte[image_length];
//      in.readFully(buf, 0, image_length);
//      ob = new ValueBlobInput(buf);
//
//    }
//    else {

      ob = in.readObject();

//    }

  }

  /**
   * The equals method.
   */
  public boolean equals(Object to) {
    ValueSubstitution targ = (ValueSubstitution) to;
    return (type == targ.type && ob.equals(targ.ob));
  }

  /**
   * Finds the hash.
   */
  public int hashCode() {
    return ob.hashCode();
  }

  /**
   * For debugging.
   */
  public String toString() {
    if (ob == null) {
      return "null";
    }
    return ob.toString();
  }


//  /**
//   * This static inner-class is for the generation of a BlobInput object that
//   * is constructed from after a blob type is read over the stream.
//   */
//  final static class ValueBlobInput implements BlobInput {
//
//    /**
//     * This is the byte array that represents the actual blob data.
//     * NOTE: This could intelligently be stored in a file in the blob is too
//     *   large to fit into memory.  For example, it would be a bit silly
//     *   storing a 2 gigabyte blob in memory.
//     */
//    private byte[] blob_buffer;
//
//    /**
//     * The Constructor.
//     * NOTE: That the byte[] array given is stored and not copied, so there
//     *   should be no tampering with it after it enters here.
//     */
//    ValueBlobInput(byte[] dat) {
//      this.blob_buffer = dat;
//    }
//
//    // ---------- Implemented from BlobInput interface ----------
//
//    public InputStream getInputStream() {
//      return new ByteArrayInputStream(blob_buffer);
//    }
//
//    public long length() {
//      return blob_buffer.length;
//    }
//
//  }

}
