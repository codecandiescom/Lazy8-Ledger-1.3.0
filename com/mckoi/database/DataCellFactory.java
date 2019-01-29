/**
 * com.mckoi.database.DataCellFactory  10 Mar 1998
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

import com.mckoi.database.global.ValueSubstitution;
import com.mckoi.database.global.ObjectTranslator;
import com.mckoi.database.global.ByteLongObject;
import com.mckoi.database.global.CastHelper;
import com.mckoi.database.global.NullObject;
import com.mckoi.database.global.SQLTypes;
import com.mckoi.database.global.Types;
import java.math.BigDecimal;
import java.util.Date;
import java.io.*;

/**
 * A Factory for creating DataCell objects.  This class is used for creating
 * DataCell objects for handling different types of elemental data types.
 * The methods have been grouped together in this function for ease of
 * maintenance.
 * <p>
 * As well as creating DataCell objects from the String type, it also creates
 * DataCell objects from a 'ValueSubstitution' object.
 * <p>
 * @author Tobias Downer
 */

public class DataCellFactory {

  /**
   * These methods check if the sql type matches the given type.
   * @deprecated
   */
  public final static boolean isNumeric(int type) {
    return (type == java.sql.Types.NUMERIC || type == java.sql.Types.INTEGER ||
            type == java.sql.Types.FLOAT   || type == java.sql.Types.REAL    ||
            type == java.sql.Types.DOUBLE);
  }

  /**
   * @deprecated
   */
  public final static boolean isString(int type) {
    return (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR);
  }

  /**
   * @deprecated
   */
  public final static boolean isDateTime(int type) {
    return (type == java.sql.Types.DATE || type == java.sql.Types.TIME);
  }

  /**
   * @deprecated
   */
  public final static boolean isBlob(int type) {
    return (type == java.sql.Types.BINARY || type == java.sql.Types.VARBINARY ||
            type == java.sql.Types.LONGVARBINARY);
  }

  /**
   * Given a TableField object, this method will return a 'null' DataCell
   * object for the field type.
   */
  public final static DataCell tableFieldToDataCell(TableField field) {
    return createDataCell(field);
  }

//  /**
//   * Creates a blob DataCell given a string that represents the blob_key which
//   * points to the file that created the blob.
//   * @deprecated
//   */
//  public static DataCell createBlobDataCell(TableField field, String blob_key) {
//    int type = field.getType();
//    if (type == Types.DB_BINARY) {
//      return new BlobDataCell(blob_key);
//    }
//    throw new RuntimeException("The column type must be a blob type.");
//  }



  private static DataCell createDataCell(int type, int size) {
    if (type == Types.DB_NUMERIC) {
      return new DecimalDataCell();
    }
    if (type == Types.DB_STRING && size > -1) {
      return new StringDataCell(size);
    }
    if (type == Types.DB_BOOLEAN) {
      return new BooleanDataCell();
    }
    if (type == Types.DB_TIME) {
      return new TimeDataCell();
    }
    if (type == Types.DB_BLOB && size > -1) {
      return new BlobDataCell(size);
    }
    if (type == Types.DB_OBJECT) {
      return new SerializedObjectDataCell();
    }

    throw new Error("The given field type is not supported.");
  }

  /**
   * This generates a new DataCell of the given type and sets it to 'null'.
   */
  public static DataCell createDataCell(DataTableColumnDef column_def) {
    final int type = column_def.getDBType();
    final int size = column_def.getSize();
    return createDataCell(type, size);
  }

  /**
   * This generates a new DataCell of the given type and sets it to 'null'.
   */
  public static DataCell createDataCell(TableField field) {
    final int type = field.getType();
    final int size = field.getSize();
    return createDataCell(type, size);
  }

  /**
   * Returns a DataCell object for the given object.  This method does not
   * cast the object to any other type.
   */
  public static DataCell fromObject(int db_type, Object ob) {
    if (ob == null || ob instanceof NullObject) {
      if (db_type == Types.DB_NUMERIC) {
        return NULL_DECIMAL_CELL;
      }
      else if (db_type == Types.DB_STRING) {
        return NULL_STRING_CELL;
      }
      else if (db_type == Types.DB_BOOLEAN) {
        return NULL_BOOLEAN_CELL;
      }
      else if (db_type == Types.DB_TIME) {
        return NULL_TIME_CELL;
      }
      else if (db_type == Types.DB_BLOB) {
        return NULL_BLOB_CELL;
      }
      else if (db_type == Types.DB_OBJECT) {
        return NULL_OBJECT_CELL;
      }
      else {
        throw new Error("Unknown NULL type.");
      }
    }

    if (ob instanceof BigDecimal) {
      return new DecimalDataCell((BigDecimal) ob);
    }
    else if (ob instanceof String) {
      return new StringDataCell(Integer.MAX_VALUE, (String) ob);
    }
    else if (ob instanceof Boolean) {
      if (ob.equals(Boolean.TRUE)) {
        return TRUE_BOOLEAN_CELL;
      }
      else {
        return FALSE_BOOLEAN_CELL;
      }
    }
    else if (ob instanceof ByteLongObject) {
      if (db_type == Types.DB_OBJECT) {
        return new SerializedObjectDataCell((ByteLongObject) ob);
      }
      else {
        return new BlobDataCell(Integer.MAX_VALUE, (ByteLongObject) ob);
      }
    }
    else if (ob instanceof java.util.Date) {
      return new TimeDataCell((java.util.Date) ob);
    }
    else {
      throw new Error("Do not understand type: " + ob.getClass());
    }

  }


//  // We should consider deprecating this method because the work it does
//  // should be handled by global.CastHelper now.
//  private static DataCell generateDataCell(int type,
//                                           int size, Object ob) {
//
//    // Numeric destination...
//    if (type == Types.DB_NUMERIC) {
//      if (ob == Expression.NULL_OBJ) {
//        return NULL_DECIMAL_CELL;
//      }
//      else if (ob instanceof String) {
//        try {
//          return new DecimalDataCell(new BigDecimal(ob.toString()));
//        }
//        catch (Throwable e) {
//          return ZERO_DECIMAL_CELL;
//        }
//      }
//      else if (ob instanceof Boolean) {
//        if (ob.equals(Boolean.TRUE)) {
//          return ONE_DECIMAL_CELL;
//        }
//        else {
//          return ZERO_DECIMAL_CELL;
//        }
//      }
//      else if (ob instanceof BigDecimal) {
//        return new DecimalDataCell((BigDecimal) ob);
//      }
//      else if (ob instanceof Date) {
//        return new DecimalDataCell(new BigDecimal(((Date) ob).getTime()));
//      }
//      else if (ob instanceof ByteLongObject) {
//        // See if we can deserialize this and cast to a number,
//        try {
//          Object ds = ObjectTranslator.deserialize((ByteLongObject) ob);
//          if (ds instanceof Number) {
//            // Use string constructor to BigDecimal for predictable
//            // value parsing.
//            return new DecimalDataCell(new BigDecimal(ds.toString()));
//          }
//          // Try and convert the string form in to a number,
//          return new DecimalDataCell(new BigDecimal(ds.toString()));
//        }
//        catch (Throwable e) {
//          return ZERO_DECIMAL_CELL;
//        }
//      }
//    }
//
//    // String destination...
//    if (type == Types.DB_STRING) {
//      if (ob == Expression.NULL_OBJ) {
//        return NULL_STRING_CELL;
//      }
//      String str_val = null;
//      if (ob instanceof String ||
//          ob instanceof Boolean ||
//          ob instanceof BigDecimal ||
//          ob instanceof Date) {
//        str_val = ob.toString();
//      }
//      else if (ob instanceof ByteLongObject) {
//        // Try and deserialize the object,
//        try {
//          Object ds = ObjectTranslator.deserialize((ByteLongObject) ob);
//          return new StringDataCell(Integer.MAX_VALUE, ds.toString());
//        }
//        catch (Throwable e) {
//          return new StringDataCell(Integer.MAX_VALUE, ob.toString());
//        }
//      }
//
//      if (str_val != null) {
//        return new StringDataCell(Integer.MAX_VALUE, str_val);
//      }
//    }
//
//    // Boolean destination...
//    if (type == Types.DB_BOOLEAN) {
//      if (ob == Expression.NULL_OBJ) {
//        return NULL_BOOLEAN_CELL;
//      }
//      else if (ob instanceof String) {
//        if (ob.toString().equalsIgnoreCase("true")) {
//          return TRUE_BOOLEAN_CELL;
//        }
//        else {
//          return FALSE_BOOLEAN_CELL;
//        }
//      }
//      else if (ob instanceof Boolean) {
//        if (ob.equals(Boolean.TRUE)) {
//          return TRUE_BOOLEAN_CELL;
//        }
//        else {
//          return FALSE_BOOLEAN_CELL;
//        }
//      }
//      else if (ob instanceof BigDecimal) {
//        if (ob.equals(BD_ZERO)) {
//          return FALSE_BOOLEAN_CELL;
//        }
//        else {
//          return TRUE_BOOLEAN_CELL;
//        }
//      }
//      else if (ob instanceof Date) {
//        // behaviour = true if date == 0
//        if (((Date) ob).getTime() == 0) {
//          return FALSE_BOOLEAN_CELL;
//        }
//        else {
//          return TRUE_BOOLEAN_CELL;
//        }
//      }
//      else if (ob instanceof ByteLongObject) {
//        // Try and deserialize the object,
//        try {
//          Object ds = ObjectTranslator.deserialize((ByteLongObject) ob);
//          if (ds instanceof Number) {
//            // If we are a number, see if the number == 0
//            if ( ((Number) ds).toString().equals("0") ) {
//              return TRUE_BOOLEAN_CELL;
//            }
//          }
//          return FALSE_BOOLEAN_CELL;
//        }
//        catch (Throwable e) {
//          // FALSE - can't convert!
//          return FALSE_BOOLEAN_CELL;
//        }
//      }
//    }
//
//    // Date destination...
//    if (type == Types.DB_TIME) {
//      if (ob == Expression.NULL_OBJ) {
//        return NULL_TIME_CELL;
//      }
//      else if (ob instanceof String) {
//        try {
//          return new TimeDataCell(Long.parseLong(ob.toString()));
//        }
//        catch (Throwable e) {
//          return new TimeDataCell(0);
//        }
//      }
//      else if (ob instanceof Boolean) {
//        if (ob.equals(Boolean.FALSE)) {
//          return new TimeDataCell(0);
//        }
//        else {
//          return new TimeDataCell(9000000);
//        }
//      }
//      else if (ob instanceof BigDecimal) {
//        return new TimeDataCell(((BigDecimal) ob).longValue());
//      }
//      else if (ob instanceof Date) {
//        return new TimeDataCell((Date) ob);
//      }
//      else if (ob instanceof ByteLongObject) {
//        // Null - can't convert!
//        return new TimeDataCell();
//      }
//    }
//
//    // BLOB destination
//    if (type == Types.DB_BLOB) {
//      if (ob == Expression.NULL_OBJ) {
//        return NULL_BLOB_CELL;
//      }
//      else if (ob instanceof ByteLongObject) {
//        return new BlobDataCell(size, (ByteLongObject) ob);
//      }
//      else if (ob instanceof byte[]) {
//        return new BlobDataCell(size, new ByteLongObject((byte[]) ob));
//      }
//      else {
//        String str = ob.toString();
//        return new BlobDataCell(size, new ByteLongObject(str.getBytes()));
//      }
//    }
//
//    // Object destination
//    if (type == Types.DB_OBJECT) {
//      if (ob == Expression.NULL_OBJ) {
//        return NULL_OBJECT_CELL;
//      }
//      else if (ob instanceof ByteLongObject) {
//        return new SerializedObjectDataCell((ByteLongObject) ob);
//      }
//      else {
//        return new SerializedObjectDataCell(ObjectTranslator.serialize(ob));
////        // PENDING: Serialize the object?
////        throw new Error("Unable to create the serialized form of " + ob);
//      }
//    }
//
//
//    throw new Error("Unrecognised Object type: " + ob.getClass());
//
//  }


  private static DataCell castToDataCell(Object ob, int db_type, int sql_type,
                                     int size, int scale, String type_name) {
    Object db_object =
        CastHelper.castObjectToSQLType(ob, sql_type, size, scale, type_name);
    return fromObject(db_type, db_object);
  }

  /**
   * Generates a new DataCell from an 'Object' object based on the type of
   * field specified.
   */
  public static DataCell generateDataCell(TableField field, Object ob) {
    return castToDataCell(ob,
               field.getType(), field.getSQLType(),
               field.getSize(), field.getScale(), field.getSQLTypeName());

//    Object db_object = CastHelper.castObjectToSQLType(ob,
//                           field.getSQLType(), field.getSize(),
//                           field.getScale(), field.getSQLTypeName());
//    return fromObject(field.getType(), db_object);
////    return generateDataCell(field.getType(), field.getSize(), db_object);
  }

  /**
   * Generates a new DataCell from an 'Object' object based on the type of
   * field specified.
   */
  public static DataCell generateDataCell(DataTableColumnDef column_def,
                                          Object ob) {
    return castToDataCell(ob,
               column_def.getDBType(), column_def.getSQLType(),
               column_def.getSize(), column_def.getScale(),
               column_def.getSQLTypeString());

//    Object db_object = CastHelper.castObjectToSQLType(ob,
//                     column_def.getSQLType(), column_def.getSize(),
//                     column_def.getScale(), column_def.getSQLTypeString());
//    return fromObject(column_def.getDBType(), db_object);
////    return generateDataCell(column_def.getDBType(), column_def.getSize(),
////                            db_object);
  }


  private static DataCell createDataCell(int type, int sql_type,
                                         int size, int scale,
                                         String type_string,
                                         ValueSubstitution val) {
    int subst_type = val.getType();

    // If the ValueSubstitution type is unknown, then we must bind it here.
    if (subst_type == Types.DB_UNKNOWN) {

      // NOTE: If the type is unknown, then we attempt to perform a 'strong
      //   type' operation on the object stored in the ValueSubstitution.
      // The DB_UNKNOWN type is useful for NULL values where we don't know the
      // conditional type at parse time (where we create the ValueSubstitution
      // objects).

    }
//    else if (type != subst_type) {
//      if (val.getObject() != null) {
//        throw new RuntimeException(
//                        "Field type does not match value substitution type.");
//      }
//    }

    // -----
    if (type == Types.DB_NUMERIC) {
      BigDecimal to_ins;
      if (val.getObject() == null) {
        to_ins = null;
      }
      else {
        if (subst_type == Types.DB_NUMERIC) {
          to_ins = (BigDecimal) val.getObject();
        }
        else if (subst_type == Types.DB_STRING) {
          try {
            to_ins = new BigDecimal((String) val.getObject());
          }
          catch (Exception e) {
            to_ins = null;
          }
        }
        else if (subst_type == Types.DB_BOOLEAN) {
          if (val.getObject().equals(Boolean.TRUE)) {
            to_ins = new BigDecimal(0);  // if true
          }
          else {
            to_ins = new BigDecimal(1);  // if false
          }
        }
        else if (subst_type == Types.DB_TIME) {
          to_ins = new BigDecimal(((Date) val.getObject()).getTime());
        }
        else {
          throw new Error("Can't convert to DB_NUMERIC");
        }
      }
      return new DecimalDataCell(to_ins);
    }
    // -----
    if (type == Types.DB_STRING && size > -1) {
      String to_ins;
      int max_size = size;
      if (val.getObject() == null) {
        to_ins = null;
      }
      else {
        to_ins = val.getObject().toString();
        if (to_ins.length() > max_size) {
          to_ins = to_ins.substring(0, max_size);
        }
      }

      return new StringDataCell(max_size, to_ins);
    }
    // -----
    if (type == Types.DB_BOOLEAN) {
      return new BooleanDataCell((Boolean) val.getObject());
    }
    // -----
    if (type == Types.DB_TIME) {
      return new TimeDataCell((Date) val.getObject());
    }
    // -----
    if (type == Types.DB_BLOB) {
      return castToDataCell(val.getObject(), type, sql_type, size, scale,
                            type_string);
//      return generateDataCell(type, size, val.getObject());
    }
    // -----
    if (type == Types.DB_OBJECT) {
      return castToDataCell(val.getObject(), type, sql_type, size, scale,
                            type_string);
//      return generateDataCell(type, size, val.getObject());
    }


    throw new RuntimeException("The given field type is not supported.");
  }


  /**
   * This generates a new DataCell from a 'ValueSubstitution' object.  Because
   * a ValueSubstitution object describes the type of an object, we can easily
   * generate the proper type.
   */
  public static DataCell createDataCell(TableField field,
                                        ValueSubstitution val) {
    int type = field.getType();
    int sql_type = field.getSQLType();
    int size = field.getSize();
    int scale = field.getScale();
    String type_string = field.getSQLTypeName();
    return createDataCell(type, sql_type, size, scale, type_string, val);
  }

  /**
   * This generates a new DataCell from a 'ValueSubstitution' object.  Because
   * a ValueSubstitution object describes the type of an object, we can easily
   * generate the proper type.
   */
  public static DataCell createDataCell(DataTableColumnDef column_def,
                                        ValueSubstitution val) {
    int type = column_def.getDBType();
    int sql_type = column_def.getSQLType();
    int size = column_def.getSize();
    int scale = column_def.getScale();
    String type_string = column_def.getSQLTypeString();
    return createDataCell(type, sql_type, size, scale, type_string, val);
  }



  // ---------- Variable length IO operations ----------

  /**
   * Writes the DataCell to the output stream.  This writes out the DataCell
   * as a variable length piece of information.  The format is very specific,
   *   (byte) type, (byte) 1:null, 0:not null, (...) content.
   * <p>
   * Returns the amount of space that was written.
   */
  public static int writeDataCell(DataCell cell, DataOutput dout)
                                                          throws IOException {

    // Write out the type of cell this is,
    int type = cell.getExtractionType();
    Object ob = cell.getCell();
    dout.writeByte((byte) type);

    int space_used = 2;    // Running total of space used (header = 2 bytes)

    if (ob == null) {
      // Write out null byte.
      dout.writeByte(1);
    }
    else {
      // Write out used byte.
      dout.writeByte(0);

      if (type == Types.DB_NUMERIC) {
        DecimalDataCell ddc = (DecimalDataCell) cell;
        byte[] buf = ddc.bigint.toByteArray();

        dout.writeShort((short) ddc.val.scale());
        dout.writeInt(buf.length);
        dout.write(buf);

        space_used += 2 + 4 + buf.length;
      }
      else if (type == Types.DB_STRING) {
        String str = (String) ob;

        dout.writeInt(str.length());
        dout.writeChars(str);

        space_used += 4 + (str.length() * 2);
      }
      else if (type == Types.DB_BOOLEAN) {
        Boolean bool = (Boolean) ob;

        dout.writeByte(bool.booleanValue() ? 1 : 0);

        space_used += 1;
      }
      else if (type == Types.DB_TIME) {
        Date date = (Date) ob;

        dout.writeLong(date.getTime());

        space_used += 8;
      }
      else if (type == Types.DB_BLOB) {
        ByteLongObject blob = (ByteLongObject) ob;

        dout.writeInt(blob.length());
        dout.write(blob.getByteArray());

        space_used += 4 + blob.length();
      }
      else if (type == Types.DB_OBJECT) {
        byte[] serialized_form = (byte[]) ob;

        dout.writeInt(serialized_form.length);
        dout.write(serialized_form);

        space_used += 4 + serialized_form.length;
      }



      else {
        throw new Error("Don't know how to write type " + type + " out.");
      }
    }

    return space_used;
  }

  /**
   * Returns the amount of byte to write out the given DataCell.
   */
  public static int sizeToWriteDataCell(DataCell cell) {

    // Write out the type of cell this is,
    int type = cell.getExtractionType();
    Object ob = cell.getCell();

    int space_used = 2;    // Running total of space used (header = 2 bytes)

    if (ob == null) {
    }
    else {
      if (type == Types.DB_NUMERIC) {
        DecimalDataCell ddc = (DecimalDataCell) cell;
        byte[] buf = ddc.bigint.toByteArray();
        space_used += 2 + 4 + buf.length;
      }
      else if (type == Types.DB_STRING) {
        String str = (String) ob;
        space_used += 4 + (str.length() * 2);
      }
      else if (type == Types.DB_BOOLEAN) {
        space_used += 1;
      }
      else if (type == Types.DB_TIME) {
        space_used += 8;
      }
      else if (type == Types.DB_BLOB) {
        ByteLongObject blob = (ByteLongObject) ob;
        space_used += 4 + blob.length();
      }
      else if (type == Types.DB_OBJECT) {
        byte[] serialized_form = (byte[]) ob;
        space_used += 4 + serialized_form.length;
      }

      else {
        throw new Error("Don't know how to write type " + type + " out.");
      }
    }

    return space_used;

  }

  /**
   * Reads the DataCell from the input stream.
   */
  public static DataCell readDataCell(CellInput din) throws IOException {

    // Get the type,
    int type = (int) din.readByte();
    // Get the null byte,
    byte null_byte = din.readByte();
    // If null byte is 1 then return null data cell.
    if (null_byte == 1) {
      if (type == Types.DB_NUMERIC) {
        return NULL_DECIMAL_CELL;
      }
      else if (type == Types.DB_STRING) {
        return NULL_STRING_CELL;
      }
      else if (type == Types.DB_BOOLEAN) {
        return NULL_BOOLEAN_CELL;
      }
      else if (type == Types.DB_TIME) {
        return NULL_TIME_CELL;
      }
      else if (type == Types.DB_BLOB) {
        return NULL_BLOB_CELL;
      }
      else if (type == Types.DB_OBJECT) {
        return NULL_OBJECT_CELL;
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
          return ZERO_DECIMAL_CELL;
        }
        else if (scale == 0 && num_len == 1 && buf[0] == 1) {
          return ONE_DECIMAL_CELL;
        }

        return new DecimalDataCell(buf, scale);
      }
      else if (type == Types.DB_STRING) {
        int str_length = din.readInt();
        // Intern to save memory
        if (str_length == 0) {
          return EMPTY_STRING_CELL;
        }

//        String dastr = helperReadChars(din, str_length);
        String dastr = din.readChars(str_length);
        // NOTE: We intern the string to save memory.
        return new StringDataCell(Integer.MAX_VALUE, dastr.intern());
      }
      else if (type == Types.DB_BOOLEAN) {
        return din.readByte() == 0 ? FALSE_BOOLEAN_CELL : TRUE_BOOLEAN_CELL;
      }
      else if (type == Types.DB_TIME) {
        return new TimeDataCell(din.readLong());
      }
      else if (type == Types.DB_BLOB) {
        int blob_length = din.readInt();
        // Intern to save memory
        if (blob_length == 0) {
          return EMPTY_BLOB_CELL;
        }

        byte[] buf = new byte[blob_length];
        din.readFully(buf, 0, blob_length);
        return new BlobDataCell(Integer.MAX_VALUE, new ByteLongObject(buf));
      }
      else if (type == Types.DB_OBJECT) {
        int sf_length = din.readInt();
        byte[] buf = new byte[sf_length];
        din.readFully(buf, 0, sf_length);
        return new SerializedObjectDataCell(new ByteLongObject(buf));
      }
      else {
        throw new Error("Don't understand type: " + type);
      }

    }

  }

  /**
   * Skips past a variable entry in the given DataInput stream.
   */
  public static void skipDataCell(DataInput din) throws IOException {
    // Get the type,
    int type = (int) din.readByte();
    // Get the length,
    int length = din.readInt();
    // Skip this number of elements.
    din.skipBytes(length);
  }



  // ---------- Intern methods ----------

  /**
   * Returns a DataCell that is the intern of the given DataCell.  This works
   * like the String intern method.
   */
  public static DataCell internDataCell(DataCell cell) {

    // Do some obvious memory saving optimizations....
    // --- DECIMAL ---
    if (cell instanceof DecimalDataCell) {
      BigDecimal val = (BigDecimal) cell.getCell();
      if (val == null) {
        return NULL_DECIMAL_CELL;
      }
      else if (val.equals(BD_ZERO)) {
        return ZERO_DECIMAL_CELL;
      }
      return cell;
    }

    // --- BOOLEAN ---
    else if (cell instanceof BooleanDataCell) {
      Boolean val = (Boolean) cell.getCell();
      if (val == null) {
        return NULL_BOOLEAN_CELL;
      }
      else if (val.equals(Boolean.TRUE)) {
        return TRUE_BOOLEAN_CELL;
      }
      else {
        return FALSE_BOOLEAN_CELL;
      }
    }

    // --- STRING ---
    // NOTE: Difficult optimizing string because of string length variation.

    // Default
    return cell;
  }



  // --- Statics ---

  static final SerializedObjectDataCell NULL_OBJECT_CELL =
                                               new SerializedObjectDataCell();

  static final BlobDataCell NULL_BLOB_CELL =
                                          new BlobDataCell(Integer.MAX_VALUE);
  static final BlobDataCell EMPTY_BLOB_CELL =
         new BlobDataCell(Integer.MAX_VALUE, new ByteLongObject(new byte[0]));

  static final StringDataCell NULL_STRING_CELL =
                                        new StringDataCell(Integer.MAX_VALUE);
  static final StringDataCell EMPTY_STRING_CELL =
                                    new StringDataCell(Integer.MAX_VALUE, "");
  static final TimeDataCell NULL_TIME_CELL = new TimeDataCell();
  static final DecimalDataCell NULL_DECIMAL_CELL =
                                                    new DecimalDataCell(null);
  static final BooleanDataCell NULL_BOOLEAN_CELL =
                                                    new BooleanDataCell(null);

  static final BigDecimal BD_ZERO = new BigDecimal(0);
  static final BigDecimal BD_ONE  = new BigDecimal(1);
  static final DecimalDataCell ZERO_DECIMAL_CELL =
                                                 new DecimalDataCell(BD_ZERO);
  static final DecimalDataCell ONE_DECIMAL_CELL =
                                                 new DecimalDataCell(BD_ONE);
  static final BooleanDataCell TRUE_BOOLEAN_CELL =
                                            new BooleanDataCell(Boolean.TRUE);
  static final BooleanDataCell FALSE_BOOLEAN_CELL =
                                           new BooleanDataCell(Boolean.FALSE);


}
