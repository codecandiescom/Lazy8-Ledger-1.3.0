/**
 * com.dasoft.database.TableField  19 Jul 2000
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

package com.dasoft.database;

import com.dasoft.database.global.ColumnDescription;

/**
 * This is so we can read from old databases that used the old package name
 * space.
 *
 * @author Tobias Downer
 */

public class TableField extends ColumnDescription {

  static final long serialVersionUID = 2383040947357446013L;

  /**
   * Fixed size a field of this type takes to store in bytes.
   */
  private int fixed_size;

  /**
   * The Constructors if the type does require a size.
   */
  public TableField(String name, int type, int size, boolean not_null) {
    super(name, type, size, not_null);

//    // Fixed size of the cell.
//    fixed_size = DataCellFactory.tableFieldToDataCell(this).sizeof();
  }

  public TableField(String name, int type, boolean not_null) {
    this(name, type, -1, not_null);
  }

  public TableField(ColumnDescription cd) {
    super(cd);

//    // Fixed size of the cell.
//    fixed_size = DataCellFactory.tableFieldToDataCell(this).sizeof();
  }

  public TableField(String name, TableField field) {
    super(name, field);

//    // Fixed size of the cell.
//    fixed_size = DataCellFactory.tableFieldToDataCell(this).sizeof();
  }


  /**
   * Returns an integer represents the extraction type of this field.  The
   * extraction type is the type as specified in
   * 'com.mckoi.database.extraction.Types' and is more general than the
   * sql type.
   * @deprecated use getType() instead.
   */
  public int getExtractionType() {
    return getType();
  }

  /**
   * Returns the fixed length size of this type of field.
   */
  public int getFixedLength() {
    return fixed_size;
  }

//  /**
//   * Returns a new null DataCell object of this field type used to access the
//   * underlying data stream.  eg. With these objects, we can 'readFrom' to
//   * retrieve information from an underlying DataInput 'stream'.
//   * ISSUE: If we didn't have this method, or the fixed size string we could
//   *   make this class into a global class.
//   */
//  public DataCell createDataCell() {
//    throw new Error("No Longer Working!");
////    return DataCellFactory.tableFieldToDataCell(this);
//  }

  public boolean equals(Object ob) {
    return ( super.equals(ob) &&
             fixed_size == ((TableField)ob).fixed_size );
  }

}
