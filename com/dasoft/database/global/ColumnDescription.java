/**
 * com.dasoft.database.global.ColumnDescription  19 Jul 2000
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

package com.dasoft.database.global;

import com.mckoi.database.global.Types;
import java.util.Date;
import java.math.BigDecimal;

/**
 * This is so we can read from old databases that used the old package name
 * space.
 *
 * @author Tobias Downer
 */

public class ColumnDescription implements java.io.Serializable {

  static final long serialVersionUID = 8210197301596138014L;

  /**
   * The name of the field.
   */
  private String name;

  /**
   * The type of the field, from the Types object.
   */
  private int type;

  /**
   * The size of the type.  This is not necessary for some types.
   */
  private int size;

  /**
   * If true, the field may not be null.  If false, the column may contain
   * no information.  This is enforced at the parse stage when adding or
   * altering a table.
   */
  private boolean not_null;

  /**
   * If true, the field may only contain unique values.  This is enforced at
   * the parse stage when adding or altering a table.
   */
  private boolean unique;

  /**
   * This represents the 'unique_group' that this column is in.  If two
   * columns in a table belong to the same unique_group, then the specific
   * combination of the groups columns can not exist more than once in the
   * table.
   * A value of -1 means the column does not belong to any unique group.
   */
  private int unique_group;

  /**
   * The Constructors if the type does require a size.
   */
  public ColumnDescription(String name, int type, int size, boolean not_null) {
    this.name = name;
    this.type = type;
    this.size = size;
    this.not_null = not_null;
    this.unique = false;
    this.unique_group = -1;
  }

  public ColumnDescription(String name, int type, boolean not_null) {
    this(name, type, -1, not_null);
  }

  public ColumnDescription(ColumnDescription cd) {
    this(cd.getName(), cd.getType(), cd.getSize(), cd.isNotNull());
    if (cd.isUnique()) {
      setUnique();
    }
    setUniqueGroup(cd.getUniqueGroup());
  }

  public ColumnDescription(String name, ColumnDescription cd) {
    this(name, cd.getType(), cd.getSize(), cd.isNotNull());
    if (cd.isUnique()) {
      setUnique();
    }
    setUniqueGroup(cd.getUniqueGroup());
  }

  /**
   * Sets this column to unique.
   * NOTE: This can only happen during the setup of the object.  Unpredictable
   *   results will occur otherwise.
   */
  public void setUnique() {
    this.unique = true;
  }

  /**
   * Sets the column to belong to the specified unique group in the table.
   * Setting to -1 sets the column to no unique group.
   * NOTE: This can only happen during the setup of the object.  Unpredictable
   *   results will occur otherwise.
   */
  public void setUniqueGroup(int group) {
    this.unique_group = group;
  }

  /**
   * Returns the name of the field.  The field type returned should be
   * 'ZIP' or 'Address1'.  To resolve to the tables type, we must append
   * an additional 'Company.' or 'Customer.' string to the front.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns an integer representing the type of the field.  The types are
   * outlined in com.mckoi.database.global.Types.
   */
  public int getType() {
    return type;
  }

  /**
   * Returns the class of Java object for this field.
   */
  public Class classType() {
    if (type == Types.DB_STRING) {
      return String.class;
    }
    else if (type == Types.DB_NUMERIC) {
      return BigDecimal.class;
    }
    else if (type == Types.DB_TIME) {
      return Date.class;
    }
    else if (type == Types.DB_BOOLEAN) {
      return Boolean.class;
    }
    else {
      throw new Error("Unknown type.");
    }
  }

  /**
   * Returns the size of the given field.  This is only applicable to a few
   * of the types, ie VARCHAR.  It returns -1 if the field does not support a
   * size.
   */
  public int getSize() {
    return size;
  }

  /**
   * Determines whether the field can contain a null value or not.  Returns
   * true if it is required for the column to contain data.
   */
  public boolean isNotNull() {
    return not_null;
  }

  /**
   * Determines whether the field can contain two items that are identical.
   * Returns true if each element must be unique.
   */
  public boolean isUnique() {
    return unique;
  }

  /**
   * Returns the unique group that this column is in.  If it does not belong
   * to a unique group then the value -1 is returned.
   */
  public int getUniqueGroup() {
    return unique_group;
  }

  /**
   * Returns true if the type of the field is searchable.  Searchable means
   * that the database driver can quantify it, as in determine if a given
   * object of the same type is greater, equal or less.  We can not quantify
   * BINARY types.
   */
  public boolean isQuantifiable() {
    if (type == Types.DB_BINARY) {
      return false;
    }
    return true;
  }

  /**
   * The 'equals' method, used to determine equality between column
   * descriptions.
   */
  public boolean equals(Object ob) {
    ColumnDescription cd = (ColumnDescription) ob;
    return (name.equals(cd.name) &&
            type == cd.type &&
            size == cd.size &&
            not_null == cd.not_null &&
            unique == cd.unique &&
            unique_group == cd.unique_group);
  }

}
