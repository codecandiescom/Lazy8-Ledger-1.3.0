/**
 * com.mckoi.database.SerializedObjectDataCell  08 Feb 2001
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
import com.mckoi.database.global.ObjectTranslator;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.DataOutput;
import java.io.DataInput;

/**
 * An implementation of DataCell that is a serialization of a Java object.
 * It has similar semantics to a BLOB.
 *
 * @author Tobias Downer
 */

public class SerializedObjectDataCell extends BlobDataCell {

  /**
   * Constructor.
   */
  public SerializedObjectDataCell(ByteLongObject blob) {
    super(Integer.MAX_VALUE, blob);
  }

  public SerializedObjectDataCell() {
    super(Integer.MAX_VALUE);
  }

  public int getExtractionType() {
    return com.mckoi.database.global.Types.DB_OBJECT;
  }

  // ---------- SerializedObjectDataCell specific ----------

  /**
   * Deserializes and returns the object.
   */
  public Object deserialize() {
    // PENDING: Cache these objects?
    return ObjectTranslator.deserialize(blob);
  }

}
