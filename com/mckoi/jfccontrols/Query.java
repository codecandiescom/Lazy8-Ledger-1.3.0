/**
 * com.mckoi.jfccontrols.Query  23 Aug 2000
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

package com.mckoi.jfccontrols;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;

import com.mckoi.util.TimeFrame;

/**
 * Encapsulates the information in a query to the database.  This object is
 * used in QueryAgent.
 *
 * @author Tobias Downer
 */

public class Query implements Cloneable {

  /**
   * The string to query.
   */
  private String query_string;

  /**
   * The parameters of the query (if any).
   */
  private ArrayList parameters;

  /**
   * Constructs the query.
   */
  public Query(String query) {
    this.query_string = query;
  }

  /**
   * Sets a parameter.
   */
  private void internalSet(int index, Object ob) {
    if (parameters == null) {
      parameters = new ArrayList();
    }
    for (int i = parameters.size(); i < index; ++i) {
      parameters.add(null);
    }
    Object old = parameters.set(index - 1, ob);
    if (old != null) {
//      Debug.write(Debug.WARNING, this,
//                  "Setting over a previously set parameter.");
    }
  }

  /**
   * Returns the query string.
   */
  public String getString() {
    return query_string;
  }

  /**
   * Returns the number of parameters.
   */
  public int parameterCount() {
    if (parameters == null) {
      return 0;
    }
    return parameters.size();
  }

  /**
   * Returns parameters number 'n' where 0 is the first parameters, etc.
   */
  public Object getParameter(int index) {
    return parameters.get(index);
  }

  /**
   * Returns a copy of this Query object but with a different query string.
   */
  public Query changeSQL(String sql) {
    try {
      Query query = (Query) clone();
      query.query_string = sql;
      return query;
    }
    catch (CloneNotSupportedException e) {
      throw new Error(e.getMessage());
    }
  }

  /**
   * For debugging.
   */
  public String toString() {
    StringBuffer str = new StringBuffer();
    str.append("Query: " + query_string + "\n");
    str.append("Parameters: ");
    for (int i = 0; i < parameterCount(); ++i) {
      str.append(getParameter(i));
      str.append(", ");
    }
    return new String(str);
  }



  // ---------- Methods for adding different types of parameters ----------
  // NOTE: For all these methods, para_index = 1 is the first parameters,
  //   2 is the second, etc.

  public void setString(int para_index, String str) {
    internalSet(para_index, str);
  }

  public void setBoolean(int para_index, boolean val) {
    internalSet(para_index, new Boolean(val));
  }

  public void setBigDecimal(int para_index, BigDecimal val) {
    internalSet(para_index, val);
  }

  public void setInt(int para_index, int val) {
    internalSet(para_index, new BigDecimal(val));
  }

  public void setLong(int para_index, long val) {
    internalSet(para_index, new BigDecimal(val));
  }

  public void setDouble(int para_index, double val) {
    internalSet(para_index, new BigDecimal(val));
  }

  public void setDate(int para_index, Date val) {
    internalSet(para_index, val);
  }

  public void setTimeFrame(int para_index, TimeFrame val) {
    internalSet(para_index, val.getPeriod());
  }

  public void setObject(int para_index, Object val) {
    if (val == null ||
        val instanceof BigDecimal ||
        val instanceof String ||
        val instanceof Date ||
        val instanceof Boolean) {
      internalSet(para_index, val);
    }
    else if (val instanceof TimeFrame) {
      setTimeFrame(para_index, (TimeFrame) val);
    }
    else if (val instanceof Integer) {
      internalSet(para_index, new BigDecimal(((Integer) val).intValue()));
    }
    else if (val instanceof Long) {
      internalSet(para_index, new BigDecimal(((Long) val).longValue()));
    }
    else if (val instanceof Double) {
      internalSet(para_index, new BigDecimal(((Double) val).doubleValue()));
    }
    // Default behaviour for unknown objects is to cast as a String
    // parameter.
    else {
      setString(para_index, val.toString());
    }
  }

}
