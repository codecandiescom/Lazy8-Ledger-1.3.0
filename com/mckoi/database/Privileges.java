/**
 * com.mckoi.database.Privileges  23 Aug 2001
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

import com.mckoi.util.StringListBucket;
import java.util.StringTokenizer;
import java.util.ArrayList;

/**
 * A set of privileges to grant a user for an object.
 *
 * @author Tobias Downer
 */

public class Privileges {

  // ---------- Statics ----------

  /**
   * The priv to allow full access to the database object.  If this is used,
   * it should be the only privilege added.
   */
  public final static String ALL = "ALL";

  /**
   * The priv to SELECT from a database object.
   */
  public final static String SELECT = "SELECT";

  /**
   * The priv to DELETE from a database object.
   */
  public final static String DELETE = "DELETE";

  /**
   * The priv to UPDATE a database object.
   */
  public final static String UPDATE = "UPDATE";

  /**
   * The priv to INSERT to a database object.
   */
  public final static String INSERT = "INSERT";

  /**
   * The priv to REFERENCE a database object.
   */
  public final static String REFERENCES = "REFERENCES";

  /**
   * The priv to see statistics on a database object.
   */
  public final static String USAGE = "USAGE";

  /**
   * The priv to compact a database object.
   */
  public final static String COMPACT = "COMPACT";


  // ---------- Members ----------

  /**
   * The set of privs.
   */
  private ArrayList privs;

  /**
   * Constructor.
   */
  private Privileges(ArrayList privs) {
    this.privs = privs;
  }

  public Privileges() {
    this(new ArrayList());
  }

  /**
   * Adds a privilege.  The priv parameter may be either UPDATE, INSERT,
   * or 'REFERENCES' and the 'columns' parameter must be a list of fully
   * resolved column variable references.
   */
  public void add(String priv, Variable[] col_list) {
    privs.add(new PrivilegeEntry(priv, col_list));
//    StringBuffer buf = new StringBuffer();
//    buf.append(priv);
//    if (col_list != null && col_list.length > 0) {
//      buf.append("(");
//      for (int i = 0; i < col_list.length; ++i) {
//        buf.append(col_list[i].toString());
//        buf.append(",");
//      }
//      buf.append(")");
//    }
//    privs.add(new String(buf));
  }

  /**
   * Adds a privilege.  The priv parameter may be either SELECT, DELETE,
   * or 'USAGE'.
   */
  public void add(String priv) {
    add(priv, null);
  }

  /**
   * Get the PrivilegeEntry for the privilege name or null if it couldn't be
   * found.
   */
  private PrivilegeEntry getPrivilegeEntry(String name) {
    int size = privs.size();
    for (int i = 0; i < size; ++i) {
      PrivilegeEntry entry = (PrivilegeEntry) privs.get(i);
      if (entry.getName().equals(name)) {
        return entry;
      }
    }
    return null;
  }

  /**
   * Returns true if this privileges permits the given priv.
   */
  public boolean permits(String priv) {
    // NOTE: This searches for either the priv that matches the given name
    //   or the ALL priv which means everything is permitted.
    int size = privs.size();
    for (int i = 0; i < size; ++i) {
      PrivilegeEntry entry = (PrivilegeEntry) privs.get(i);
      String name = entry.getName();
      if (name.equals(priv) || name.equals(ALL)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Merges privs from the given privilege object with this set of privs.
   * This performs an OR on all the attributes in the set.  If the entry
   * does not exist in this set then it is added.
   */
  public void merge(Privileges in_privs) {
    // The input privs
    ArrayList list = in_privs.privs;
    int size = list.size();
    for (int i = 0; i < size; ++i) {
      PrivilegeEntry entry = (PrivilegeEntry) list.get(i);
      PrivilegeEntry this_entry = getPrivilegeEntry(entry.getName());
      if (this_entry == null) {
        privs.add(entry);
      }
      else {
        this_entry.merge(entry);
      }
    }
  }

  /**
   * Converts this privilege to an encoded string.
   */
  public String toEncodedString() {
    StringBuffer buf = new StringBuffer();
    buf.append("||");
    int size = privs.size();
    for (int i = 0; i < size; ++i) {
      PrivilegeEntry entry = (PrivilegeEntry) privs.get(i);
      entry.appendToEncodedString(buf);
      buf.append("||");
    }
    return new String(buf);
  }

  /**
   * Converts from an encoded string to a Priviledge object.
   */
  public static Privileges fromEncodedString(String str) {
    StringListBucket bucket = new StringListBucket(str);
    int size = bucket.size();
    ArrayList list = new ArrayList(size);
    for (int i = 0; i < size; ++i) {
      list.add(PrivilegeEntry.fromEncodedStringEntry(bucket.get(i)));
    }
    return new Privileges(list);
  }

  // ---------- More statics ----------

  /**
   * Enable all privs for the object.
   */
  public final static Privileges ALL_PRIVS;

  /**
   * Read privs for the object.
   */
  public final static Privileges READ_PRIVS;

  static {
    ALL_PRIVS = new Privileges();
    ALL_PRIVS.add(ALL);

    READ_PRIVS = new Privileges();
    READ_PRIVS.add(SELECT);
    READ_PRIVS.add(USAGE);
  }

  // ---------- Inner classes ----------

  /**
   * An single priv.
   */
  public static class PrivilegeEntry {

    /**
     * The privilege name (INSERT, SELECT, DELETE, etc).
     */
    private String name;

    /**
     * The column list.
     */
    private Variable[] vars;

    /**
     * Constructs the entry.
     */
    public PrivilegeEntry(String name, Variable[] vars) {
      this.name = name;
      this.vars = vars;
    }

    public PrivilegeEntry(String name) {
      this(name, null);
    }

    /**
     * Returns the name of the priv.
     */
    public String getName() {
      return name;
    }

    /**
     * Returns the list of Variables if affects (or null if none).
     */
    public Variable[] getVars() {
      return vars;
    }

    /**
     * Converts this entry to an encoded string.
     */
    void appendToEncodedString(StringBuffer buf) {
      buf.append(name);
      if (vars != null && vars.length > 0) {
        buf.append("(");
        for (int i = 0; i < vars.length; ++i) {
          buf.append(vars[i].toString());
          buf.append(",");
        }
        buf.append(")");
      }
    }

    /**
     * Merge the given entry with this entry.
     */
    void merge(PrivilegeEntry entry) {
      // If there are no vars in the entry priv or this priv then no merge
      // necessary
      Variable[] in_vars = entry.getVars();
      if (in_vars == null || vars == null) {
        return;
      }
      // This means both vars and the entry vars are not null so we must
      // merge the variables.
      ArrayList merged_vars = new ArrayList();
      for (int i = 0; i < vars.length; ++i) {
        merged_vars.add(vars[i]);
      }
      for (int i = 0; i < in_vars.length; ++i) {
        Variable v = in_vars[i];
        // If the variable is not in the list then add it.
        if (!merged_vars.contains(v)) {
          merged_vars.add(v);
        }
      }
      // The merged list
      vars = (Variable[]) merged_vars.toArray(
                                          new Variable[merged_vars.size()]);
    }

    /**
     * Creates an PrivilegeEntry from an encoded string entry.
     */
    static PrivilegeEntry fromEncodedStringEntry(String str) {
      int params_index = str.indexOf("(");
      if (params_index == -1) {
        return new PrivilegeEntry(str);
      }
      else {
        String name = str.substring(0, params_index);
        String var_list = str.substring(params_index + 1, str.length() - 2);
        StringTokenizer tok = new StringTokenizer(var_list, ",");
        ArrayList v_list = new ArrayList();
        while (tok.hasMoreTokens()) {
          String v = tok.nextToken();
          v_list.add(Variable.resolve(v));
        }
        Variable[] vars =
                   (Variable[]) v_list.toArray(new Variable[v_list.size()]);
        return new PrivilegeEntry(name, vars);
      }

    }



  }


}
