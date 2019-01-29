/**
 * com.mckoi.database.DataCellSerialization  07 Dec 2000
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

import com.mckoi.database.global.*;
import java.util.zip.*;
import java.util.Date;
import java.math.*;
import java.io.*;

/**
 * The serialized form of a DataCell.  This is an intermediary object between
 * the form a DataCell and the serialized form inside a database file.
 *
 * @author Tobias Downer
 */

final class DataCellSerialization extends ByteArrayOutputStream
                                                        implements CellInput {

  /**
   * A Deflater and Inflater used to compress and uncompress the size of data
   * fields put into the store.
   */
  private Deflater deflater;
  private Inflater inflater;
  private byte[] compress_buf;
  private int compress_length;

  /**
   * The DataCell to be serialized, or that has been read in.
   */
  private DataCell data_cell;

  /**
   * If true, when writing out use the compressed form.
   */
  private boolean use_compressed;

  /**
   * The type of object.
   */
  private short type;

  /**
   * Set to true if null.
   */
  private boolean is_null;

  /**
   * Constructor.
   */
  DataCellSerialization() {
    super(1024);
  }


  /**
   * Returns the number of bytes to skip on the stream to go past the
   * next serialization.
   */
  int skipSerialization(CellInput din) throws IOException {
    int len = din.readInt();
    return len - 4;
  }

  /**
   * Reads input from the given CellInput object.
   */
  DataCell readSerialization(CellInput din) throws IOException {

    count = 0;

    // Read the length first,
    int len = din.readInt();
    short s = din.readShort();
    type = (short) (s & 0x0FFF);
    is_null = (s & 0x02000) != 0;
    use_compressed = (s & 0x04000) != 0;

    // If we are compressed...
    if (use_compressed) {
      // Uncompress it,
      int uncompressed_len = din.readInt();
      if (buf.length < uncompressed_len) {
        buf = new byte[uncompressed_len];
      }

      // Write data to the compressed buffer
      compress_length = len - 4 - 2 - 4;
      if (compress_buf == null || compress_buf.length < compress_length) {
        compress_buf = new byte[compress_length];
      }
      din.readFully(compress_buf, 0, compress_length);

      if (inflater == null) {
        inflater = new Inflater();
      }
      inflater.reset();
      inflater.setInput(compress_buf, 0, compress_length);
      int inflate_count;
      try {
        inflate_count = inflater.inflate(buf, 0, uncompressed_len);
      }
      catch (DataFormatException e) {
        throw new RuntimeException(e.getMessage());
      }

      din = this;

    }

    return readFromCellInput(din);
  }

  private DataCell readFromCellInput(CellInput din) throws IOException {

    // If null byte is 1 then return null data cell.
    if (is_null) {
      if (type == Types.DB_NUMERIC) {
        return DataCellFactory.NULL_DECIMAL_CELL;
      }
      else if (type == Types.DB_STRING) {
        return DataCellFactory.NULL_STRING_CELL;
      }
      else if (type == Types.DB_BOOLEAN) {
        return DataCellFactory.NULL_BOOLEAN_CELL;
      }
      else if (type == Types.DB_TIME) {
        return DataCellFactory.NULL_TIME_CELL;
      }
      else if (type == Types.DB_BLOB) {
        return DataCellFactory.NULL_BLOB_CELL;
      }
      else if (type == Types.DB_OBJECT) {
        return DataCellFactory.NULL_OBJECT_CELL;
      }
      else {
        throw new Error("(Null) Don't understand type: " + type);
      }
    }
    else {
      if (type == Types.DB_NUMERIC) {
        int scale = din.readShort();
        int num_len = din.readInt();
        byte[] buf = new byte[num_len];
        din.readFully(buf, 0, num_len);

        // Interns to save memory
        if (scale == 0 && num_len == 1 && buf[0] == 0) {
          return DataCellFactory.ZERO_DECIMAL_CELL;
        }
        else if (scale == 0 && num_len == 1 && buf[0] == 1) {
          return DataCellFactory.ONE_DECIMAL_CELL;
        }

        return new DecimalDataCell(buf, scale);
      }
      else if (type == Types.DB_STRING) {
        int str_length = din.readInt();
        // Intern to save memory
        if (str_length == 0) {
          return DataCellFactory.EMPTY_STRING_CELL;
        }

//        String dastr = helperReadChars(din, str_length);
        String dastr = din.readChars(str_length);
        // NOTE: We intern the string to save memory.
        return new StringDataCell(Integer.MAX_VALUE, dastr.intern());
      }
      else if (type == Types.DB_BOOLEAN) {
        return din.readByte() == 0 ? DataCellFactory.FALSE_BOOLEAN_CELL :
                                     DataCellFactory.TRUE_BOOLEAN_CELL;
      }
      else if (type == Types.DB_TIME) {
        return new TimeDataCell(din.readLong());
      }
      else if (type == Types.DB_BLOB) {
        int blob_length = din.readInt();
        // Intern to save memory
        if (blob_length == 0) {
          return DataCellFactory.EMPTY_BLOB_CELL;
        }

        byte[] buf = new byte[blob_length];
        din.readFully(buf, 0, blob_length);
        return new BlobDataCell(Integer.MAX_VALUE, new ByteLongObject(buf));
      }
      else if (type == Types.DB_OBJECT) {
        int blob_length = din.readInt();

        byte[] buf = new byte[blob_length];
        din.readFully(buf, 0, blob_length);
        return new SerializedObjectDataCell(new ByteLongObject(buf));
      }
      else {
        throw new Error("Don't understand type: " + type);
      }

    }

  }










  /**
   * Write the serialized data to the output stream.
   */
  void writeSerialization(DataOutputStream out) throws IOException {
    int len = use_compressed ? (compress_length + 4) : count;
    // size + (type | null | compressed)
    len += 4 + 2;
    out.writeInt(len);
    short s = type;
    if (is_null) {
      s |= 0x02000;
    }
    if (use_compressed) {
      s |= 0x04000;
    }
    out.writeShort(s);

    // Write out the data.
    if (use_compressed) {
      // If compressed, must write out uncompressed size first.
      out.writeInt(count);
      out.write(compress_buf, 0, compress_length);
    }
    else {
      out.write(buf, 0, count);
    }

    // And that's it!
  }

  /**
   * Sets this up with a DataCell to serialize.
   */
  void setToSerialize(DataCell cell) throws IOException {

    count = 0;
    is_null = false;
    use_compressed = false;
    type = (short) cell.getExtractionType();
    Object ob = cell.getCell();

    if (ob == null) {
      is_null = true;
      return;
    }

    // Write the serialized form to the buffer,
    writeToBuffer(cell);

    // Should we compress?

    // If it's a string, blob or serialized object, we may want to compress it,
    if (type == Types.DB_STRING ||
        type == Types.DB_BLOB ||
        type == Types.DB_OBJECT) {
      int length = count;
      // Any strings > 150 are compressed
      if (length > 150) {

        if (deflater == null) {
          deflater = new Deflater();
        }

        deflater.setInput(buf, 0, length);
        deflater.finish();

        if (compress_buf == null || compress_buf.length < length) {
          compress_buf = new byte[length];
        }
        compress_length = deflater.deflate(compress_buf);
        deflater.reset();

        if (compress_length < length) {
          use_compressed = true;
        }
      }
    }

  }

  /**
   * Writes the DataCell to the data buffer in this object.
   */
  private void writeToBuffer(DataCell cell) throws IOException {

    // Write out the type of cell this is,
    int type = cell.getExtractionType();
    Object ob = cell.getCell();

    if (type == Types.DB_NUMERIC) {
      DecimalDataCell ddc = (DecimalDataCell) cell;
      byte[] buf = ddc.bigint.toByteArray();
      writeShort((short) ddc.val.scale());
      writeInt(buf.length);
      write(buf);
    }
    else if (type == Types.DB_STRING) {
      String str = (String) ob;
      writeInt(str.length());
      writeChars(str);
    }
    else if (type == Types.DB_BOOLEAN) {
      Boolean bool = (Boolean) ob;
      writeByte((byte) (bool.booleanValue() ? 1 : 0));
    }
    else if (type == Types.DB_TIME) {
      Date date = (Date) ob;
      writeLong(date.getTime());
    }
    else if (type == Types.DB_BLOB) {
      ByteLongObject blob = (ByteLongObject) ob;
      writeInt(blob.length());
      write(blob.getByteArray());
    }
    else if (type == Types.DB_OBJECT) {
      ByteLongObject blob = (ByteLongObject) ob;
      writeInt(blob.length());
      write(blob.getByteArray());
    }
    else {
      throw new Error("Don't know how to write type " + type + " out.");
    }

  }

  public final void writeBoolean(boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  public final void writeByte(int v) throws IOException {
    write(v);
  }

  public final void writeShort(int v) throws IOException {
    write((v >>> 8) & 0xFF);
    write((v >>> 0) & 0xFF);
  }

  public final void writeChar(int v) throws IOException {
    write((v >>> 8) & 0xFF);
    write((v >>> 0) & 0xFF);
  }

  public final void writeInt(int v) throws IOException {
    write((v >>> 24) & 0xFF);
    write((v >>> 16) & 0xFF);
    write((v >>>  8) & 0xFF);
    write((v >>>  0) & 0xFF);
  }

  public final void writeLong(long v) throws IOException {
    write((int)(v >>> 56) & 0xFF);
    write((int)(v >>> 48) & 0xFF);
    write((int)(v >>> 40) & 0xFF);
    write((int)(v >>> 32) & 0xFF);
    write((int)(v >>> 24) & 0xFF);
    write((int)(v >>> 16) & 0xFF);
    write((int)(v >>>  8) & 0xFF);
    write((int)(v >>>  0) & 0xFF);
  }

  public final void writeChars(String s) throws IOException {
    int len = s.length();
    for (int i = 0 ; i < len ; ++i) {
      int v = s.charAt(i);
      write((v >>> 8) & 0xFF);
      write((v >>> 0) & 0xFF);
    }
  }



  // ---------- Implemented from CellInput ----------

  public int read() throws IOException {
    return buf[count++] & 0x0FF;
  }

  public int read(byte b[], int off, int len) throws IOException {
    if (len <= 0) {
      return 0;
    }
    System.arraycopy(buf, count, b, off, len);
    count += len;
    return len;
  }

  public long skip(long n) throws IOException {
    if (n < 0) {
      return 0;
    }
    count += n;
    return n;
  }

  public int available() throws IOException {
    throw new Error("Not supported");
  }

  public void mark(int readAheadLimit) throws IOException {
    throw new Error("Not supported");
  }

// [ Function clash here but it should be okay ]
//  public void reset() throws IOException {
//    throw new Error("Not supported");
//  }

  public void close() throws IOException {
    throw new Error("Not supported");
  }


  // ---------- Implemented from DataInput ----------

  public void readFully(byte[] b) throws IOException {
    read(b, 0, b.length);
  }

  public void readFully(byte b[], int off, int len) throws IOException {
    read(b, off, len);
  }

  public int skipBytes(int n) throws IOException {
    return (int) skip(n);
  }

  public boolean readBoolean() throws IOException {
    return (read() != 0);
  }

  public byte readByte() throws IOException {
    return (byte) read();
  }

  public int readUnsignedByte() throws IOException {
    return read();
  }

  public short readShort() throws IOException {
    int ch1 = read();
    int ch2 = read();
    return (short)((ch1 << 8) + (ch2 << 0));
  }

  public int readUnsignedShort() throws IOException {
    int ch1 = read();
    int ch2 = read();
    return (ch1 << 8) + (ch2 << 0);
  }

  public char readChar() throws IOException {
    int ch1 = read();
    int ch2 = read();
    return (char)((ch1 << 8) + (ch2 << 0));
  }

  private char[] char_buffer;

  public String readChars(int length) throws IOException {
    if (length <= 8192) {
      if (char_buffer == null) {
        char_buffer = new char[8192];
      }
      for (int i = 0; i < length; ++i) {
        char_buffer[i] = readChar();
      }
      return new String(char_buffer, 0, length);
    }
    else {
      StringBuffer chrs = new StringBuffer(length);
      for (int i = length; i > 0; --i) {
        chrs.append(readChar());
      }
      return new String(chrs);
    }
  }

  public int readInt() throws IOException {
    int ch1 = read();
    int ch2 = read();
    int ch3 = read();
    int ch4 = read();
    return (int)((ch1 << 24) + (ch2 << 16) +
                 (ch3 << 8)  + (ch4 << 0));
  }

  public long readLong() throws IOException {
    return ((long)(readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
  }

  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  public String readLine() throws IOException {
    throw new Error("Not implemented.");
  }

  public String readUTF() throws IOException {
    throw new Error("Not implemented.");
  }

}
