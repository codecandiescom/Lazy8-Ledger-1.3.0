/**
 * com.mckoi.database.global.CastHelper  11 Oct 2001
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

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.text.DateFormat;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Various utility methods for helping to cast a Java object to a type that
 * is conformant to an SQL type.
 *
 * @author Tobias Downer
 */

public class CastHelper {

  /**
   * A couple of standard BigDecimal statics.
   */
  private static BigDecimal BD_ZERO = BigDecimal.valueOf(0);
  private static BigDecimal BD_ONE = BigDecimal.valueOf(1);

  /**
   * Date, Time and Timestamp parser/formatters
   */
  private static DateFormat date_format_sql;
  private static DateFormat time_format_sql1;
  private static DateFormat time_format_sql2;
  private static DateFormat ts_format_sql1;
  private static DateFormat ts_format_sql2;

  static {
    // The SQL time/date formatters
    date_format_sql = new SimpleDateFormat("yyyy-MM-dd");
    time_format_sql1 = new SimpleDateFormat("HH:mm:ss.S");
    time_format_sql2 = new SimpleDateFormat("HH:mm:ss");
    ts_format_sql1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S ");
    ts_format_sql2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }



  /**
   * Converts the given object to an SQL JAVA_OBJECT type by serializing
   * the object.
   */
  private static Object toJavaObject(Object ob) {
    try {
      return ObjectTranslator.serialize(ob);
    }
    catch (Throwable e) {
      throw new Error("Can't serialize object " + ob.getClass());
    }
  }

  /**
   * Formats the date object as a standard SQL string.
   */
  private static String formatDateAsString(Date d) {
    // ISSUE: We have to assume the date is a time stamp because we don't
    //   know if the date object represents an SQL DATE, TIMESTAMP or TIME.
    return ts_format_sql1.format(d);
//    return new java.sql.Timestamp(d.getTime()).toString();
  }

  /**
   * Returns the given string padded or truncated to the given size.
   */
  private static String paddedString(String str, int size) {
    int dif = size - str.length();
    if (dif > 0) {
      StringBuffer buf = new StringBuffer(str);
      for (int n = 0; n < dif; ++n) {
        buf.append(' ');
      }
      return new String(buf);
    }
    else if (dif < 0) {
      return str.substring(0, size);
    }
    return str;
  }

  /**
   * Returns the given long value as a date object.
   */
  private static Date toDate(long time) {
    return new Date(time);
  }

  /**
   * Converts the given string to a BigDecimal.  Returns 0 if the cast fails.
   */
  private static BigDecimal toBigDecimal(String str) {
    try {
      return new BigDecimal(str);
    }
    catch (Throwable e) {
      return BD_ZERO;
    }
  }

  /**
   * Parses a String as an SQL date.
   */
  private static Date toDate(String str) {
    synchronized(date_format_sql) {
      try {
        return date_format_sql.parse(str);
      }
      catch (ParseException e) {
        throw new Error("Unable to parse string as a date (" +
                        date_format_sql + ")");
      }
    }
  }

  /**
   * Parses a String as an SQL time.
   */
  private static Date toTime(String str) {
    synchronized(time_format_sql1) {
      try {
        return time_format_sql1.parse(str);
      }
      catch (ParseException e) {}
      try {
        return time_format_sql2.parse(str);
      }
      catch (ParseException e) {
        throw new Error("Unable to parse string as a time (" +
                        time_format_sql2 + ")");
      }
    }
  }

  /**
   * Parses a String as an SQL timestamp.
   */
  private static Date toTimeStamp(String str) {
    synchronized(ts_format_sql1) {
      try {
        return ts_format_sql1.parse(str);
      }
      catch (ParseException e) {}
      try {
        return ts_format_sql2.parse(str);
      }
      catch (ParseException e) {
        throw new Error("Unable to parse string as a timestamp (" +
                        ts_format_sql2 + ")");
      }
    }
  }






  /**
   * Casts a Java object to the SQL type specified by the given
   * DataTableColumnDef object.  This is used for the following engine
   * functions;
   * <ol>
   * <li> To prepare a value for insertion into the data store.  For example,
   *   the table column may be STRING but the value here is a BigDecimal.
   * <li> To cast an object to a specific type in an SQL function such as
   *   CAST.
   * </ol>
   * Given any supported object, this will return the internal database
   * representation of the object as either NullObject, BigDecimal, String,
   * Date, Boolean or ByteLongObject.
   *
   * @param ob the Object to cast to the given type
   * @param sql_type the enumerated sql type, eg. SQLTypes.LONGVARCHAR
   * @param sql_size the size of the type.  For example, CHAR(20)
   * @param sql_scale the scale of the numerical type.
   * @param sql_type_string 'sql_type' as a human understandable string,
   *   eg. "LONGVARCHAR"
   */
  public static Object castObjectToSQLType(Object ob,
          int sql_type, int sql_size, int sql_scale, String sql_type_string) {

    if (ob == null) {
      ob = NullObject.NULL_OBJ;
    }

//    int sql_type = col_def.getSQLType();
//    int sql_size = col_def.getSize();
//    int sql_scale = col_def.getScale();
//    String sql_type_string = col_def.getSQLTypeString();

    // If the input object is a ByteLongObject and the output type is not a
    // binary SQL type then we need to attempt to deserialize the object.
    if (ob instanceof ByteLongObject) {
      if ( sql_type != SQLTypes.JAVA_OBJECT &&
           sql_type != SQLTypes.BINARY &&
           sql_type != SQLTypes.VARBINARY &&
           sql_type != SQLTypes.LONGVARBINARY ) {
        // Attempt to deserialize it
        try {
          ob = ObjectTranslator.deserialize((ByteLongObject) ob);
        }
        catch (Throwable e) {
          // Couldn't deserialize so it must be a standard blob which means
          // we are in error.
          throw new Error("Can't cast a BLOB to a " + sql_type_string);
        }
      }
      else {
        // This is a ByteLongObject that is being cast to a binary type so
        // no further processing is necessary.
        return ob;
      }
    }

    // Cast from NULL
    if (ob instanceof NullObject) {
      switch (sql_type) {
        case(SQLTypes.BIT):
          // fall through
        case(SQLTypes.TINYINT):
          // fall through
        case(SQLTypes.SMALLINT):
          // fall through
        case(SQLTypes.INTEGER):
          // fall through
        case(SQLTypes.BIGINT):
          // fall through
        case(SQLTypes.FLOAT):
          // fall through
        case(SQLTypes.REAL):
          // fall through
        case(SQLTypes.DOUBLE):
          // fall through
        case(SQLTypes.NUMERIC):
          // fall through
        case(SQLTypes.DECIMAL):
          // fall through
        case(SQLTypes.CHAR):
          // fall through
        case(SQLTypes.VARCHAR):
          // fall through
        case(SQLTypes.LONGVARCHAR):
          // fall through
        case(SQLTypes.DATE):
          // fall through
        case(SQLTypes.TIME):
          // fall through
        case(SQLTypes.TIMESTAMP):
          // fall through
        case(SQLTypes.NULL):
          // fall through

        case(SQLTypes.BINARY):
          // fall through
        case(SQLTypes.VARBINARY):
          // fall through
        case(SQLTypes.LONGVARBINARY):
          // fall through

        case(SQLTypes.JAVA_OBJECT):
          return NullObject.NULL_OBJ;
        default:
          throw new Error("Can't cast NULL to " + sql_type_string);
      }
    }

    // Cast from a number
    if (ob instanceof Number) {
      Number n = (Number) ob;
      switch (sql_type) {
        case(SQLTypes.BIT):
          return n.intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        case(SQLTypes.TINYINT):
          // fall through
        case(SQLTypes.SMALLINT):
          // fall through
        case(SQLTypes.INTEGER):
//          return new BigDecimal(n.intValue());
          return BigDecimal.valueOf(n.intValue());
        case(SQLTypes.BIGINT):
//          return new BigDecimal(n.longValue());
          return BigDecimal.valueOf(n.longValue());
        case(SQLTypes.FLOAT):
          return new BigDecimal(Float.toString(n.floatValue()));
        case(SQLTypes.REAL):
          return new BigDecimal(n.toString());
        case(SQLTypes.DOUBLE):
          return new BigDecimal(Double.toString(n.doubleValue()));
        case(SQLTypes.NUMERIC):
          // fall through
        case(SQLTypes.DECIMAL):
          return new BigDecimal(n.toString());
        case(SQLTypes.CHAR):
          return paddedString(n.toString(), sql_size);
        case(SQLTypes.VARCHAR):
          return n.toString();
        case(SQLTypes.LONGVARCHAR):
          return n.toString();
        case(SQLTypes.DATE):
          return toDate(n.longValue());
        case(SQLTypes.TIME):
          return toDate(n.longValue());
        case(SQLTypes.TIMESTAMP):
          return toDate(n.longValue());
        case(SQLTypes.BINARY):
          // fall through
        case(SQLTypes.VARBINARY):
          // fall through
        case(SQLTypes.LONGVARBINARY):
          return new ByteLongObject(n.toString().getBytes());
        case(SQLTypes.NULL):
          return NullObject.NULL_OBJ;
        case(SQLTypes.JAVA_OBJECT):
          return toJavaObject(ob);
        default:
          throw new Error("Can't cast number to " + sql_type_string);
      }
    }  // if (ob instanceof Number)

    // Cast from a string
    if (ob instanceof String) {
      String str = (String) ob;
      switch (sql_type) {
        case(SQLTypes.BIT):
          return str.equalsIgnoreCase("true") ? Boolean.FALSE : Boolean.TRUE;
        case(SQLTypes.TINYINT):
          // fall through
        case(SQLTypes.SMALLINT):
          // fall through
        case(SQLTypes.INTEGER):
//          return new BigDecimal(toBigDecimal(str).intValue());
          return BigDecimal.valueOf(toBigDecimal(str).intValue());
        case(SQLTypes.BIGINT):
//          return new BigDecimal(toBigDecimal(str).longValue());
          return BigDecimal.valueOf(toBigDecimal(str).longValue());
        case(SQLTypes.FLOAT):
          return new BigDecimal(
                        Float.toString(toBigDecimal(str).floatValue()));
        case(SQLTypes.REAL):
          return toBigDecimal(str);
        case(SQLTypes.DOUBLE):
          return new BigDecimal(
                        Double.toString(toBigDecimal(str).doubleValue()));
        case(SQLTypes.NUMERIC):
          // fall through
        case(SQLTypes.DECIMAL):
          return toBigDecimal(str);
        case(SQLTypes.CHAR):
          return paddedString(str, sql_size);
        case(SQLTypes.VARCHAR):
          return str;
        case(SQLTypes.LONGVARCHAR):
          return str;
        case(SQLTypes.DATE):
          return toDate(str);
        case(SQLTypes.TIME):
          return toTime(str);
        case(SQLTypes.TIMESTAMP):
          return toTimeStamp(str);
        case(SQLTypes.BINARY):
          // fall through
        case(SQLTypes.VARBINARY):
          // fall through
        case(SQLTypes.LONGVARBINARY):
          return new ByteLongObject(str.getBytes());
        case(SQLTypes.NULL):
          return NullObject.NULL_OBJ;
        case(SQLTypes.JAVA_OBJECT):
          return toJavaObject(str);
        default:
          throw new Error("Can't cast string to " + sql_type_string);
      }
    }  // if (ob instanceof String)

    // Cast from a boolean
    if (ob instanceof Boolean) {
      Boolean b = (Boolean) ob;
      switch (sql_type) {
        case(SQLTypes.BIT):
          return b;
        case(SQLTypes.TINYINT):
          // fall through
        case(SQLTypes.SMALLINT):
          // fall through
        case(SQLTypes.INTEGER):
          // fall through
        case(SQLTypes.BIGINT):
          // fall through
        case(SQLTypes.FLOAT):
          // fall through
        case(SQLTypes.REAL):
          // fall through
        case(SQLTypes.DOUBLE):
          // fall through
        case(SQLTypes.NUMERIC):
          // fall through
        case(SQLTypes.DECIMAL):
          return b.equals(Boolean.TRUE) ? BD_ONE : BD_ZERO;
        case(SQLTypes.CHAR):
          return paddedString(b.toString(), sql_size);
        case(SQLTypes.VARCHAR):
          return b.toString();
        case(SQLTypes.LONGVARCHAR):
          return b.toString();
        case(SQLTypes.NULL):
          return NullObject.NULL_OBJ;
        case(SQLTypes.JAVA_OBJECT):
          return toJavaObject(ob);
        default:
          throw new Error("Can't cast boolean to " + sql_type_string);
      }
    }  // if (ob instanceof Boolean)

    // Cast from a date
    if (ob instanceof Date) {
      Date d = (Date) ob;
      switch (sql_type) {
        case(SQLTypes.TINYINT):
          // fall through
        case(SQLTypes.SMALLINT):
          // fall through
        case(SQLTypes.INTEGER):
          // fall through
        case(SQLTypes.BIGINT):
          // fall through
        case(SQLTypes.FLOAT):
          // fall through
        case(SQLTypes.REAL):
          // fall through
        case(SQLTypes.DOUBLE):
          // fall through
        case(SQLTypes.NUMERIC):
          // fall through
        case(SQLTypes.DECIMAL):
          return BigDecimal.valueOf(d.getTime());
        case(SQLTypes.CHAR):
          return paddedString(formatDateAsString(d), sql_size);
        case(SQLTypes.VARCHAR):
          return formatDateAsString(d);
        case(SQLTypes.LONGVARCHAR):
          return formatDateAsString(d);
        case(SQLTypes.DATE):
          return d;
        case(SQLTypes.TIME):
          return d;
        case(SQLTypes.TIMESTAMP):
          return d;
        case(SQLTypes.NULL):
          return NullObject.NULL_OBJ;
        case(SQLTypes.JAVA_OBJECT):
          return toJavaObject(ob);
        default:
          throw new Error("Can't cast date to " + sql_type_string);
      }
    }  // if (ob instanceof Date)

    // Some obscure types
    if (ob instanceof byte[]) {
      switch (sql_type) {
        case(SQLTypes.BINARY):
          // fall through
        case(SQLTypes.VARBINARY):
          // fall through
        case(SQLTypes.LONGVARBINARY):
          return new ByteLongObject((byte[]) ob);
        default:
          throw new Error("Can't cast byte[] to " + sql_type_string);
      }
    }

    // Finally, the object can only be something that we can cast to a
    // JAVA_OBJECT.
    if (sql_type == SQLTypes.JAVA_OBJECT) {
      return toJavaObject(ob);
    }


    throw new RuntimeException("Can't cast object " + ob.getClass() + " to " +
                               sql_type_string);

  }

}
