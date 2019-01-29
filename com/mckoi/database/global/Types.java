/**
 * com.mckoi.database.global.Types  11 May 1998
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

/**
 * The possible types used in the database.
 * <p>
 * @author Tobias Downer
 */

public interface Types {

  public static final int DB_UNKNOWN = -1;

  public static final int DB_STRING  = 1;
  public static final int DB_NUMERIC = 2;
  public static final int DB_TIME    = 3;
  public static final int DB_BINARY  = 4;    // @deprecated - use BLOB
  public static final int DB_BOOLEAN = 5;
  public static final int DB_BLOB    = 6;
  public static final int DB_OBJECT  = 7;

// [ These more complex types are handled in the
//   'com.mckoi.mms.queries.handle' package ]
//
//  // The following types are pseudo-database types.  These are types that are
//  // not directly supported by the database, however they are distinct enough
//  // that we need to provide support for them in the front end.
//
//  // This represents a measure of time, and is converted into a DB_NUMBERIC
//  // before being sent to the database.
//  //  eg. 10 seconds = numeric( 10,000 (milliseconds) )
//  public static final int DB_TIME_MEASURE = 101;

}
