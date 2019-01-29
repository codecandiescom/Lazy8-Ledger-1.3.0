/**
 * com.mckoi.database.GrantManager  23 Aug 2001
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

import java.math.BigDecimal;

/**
 * A class that manages the grants on a database for a given database
 * connection and user.
 *
 * @author Tobias Downer
 */

public class GrantManager {

  // ---------- Statics ----------

  /**
   * Represents a TABLE object to grant privs over for the user.
   */
  public final static int TABLE = 1;

  /**
   * Represents a DOMAIN object to grant privs over for the user.
   */
  public final static int DOMAIN = 2;


  // ---------- Members ----------

  /**
   * The DatabaseConnection instance.
   */
  public DatabaseConnection connection;

  /**
   * The QueryContext instance.
   */
  public QueryContext context;

  /**
   * Constructs the GrantManager.
   * Should only be constructed from DatabaseConnection.
   */
  GrantManager(DatabaseConnection connection) {
    this.connection = connection;
    this.context = new DatabaseQueryContext(connection);
  }


  /**
   * Adds a grant on the given database object.
   *
   * @param privs the privileges to grant.
   * @param object the object to grant (TABLE, DOMAIN, etc)
   * @param param the parameter of the object (eg. the table name)
   * @param grantee the user name to grant the privs to.
   * @param grant_option if true, allows the user to pass grants to other
   *                     users.
   * @param granter the user granting.
   */
  public void addGrant(Privileges privs, int object, String param,
                       String grantee, boolean grant_option, String granter)
                                                    throws DatabaseException {

    // The system grants table.
    DataTable grant_table = connection.getTable(Database.SYS_GRANTS);
    // Add the grant to the grants table.
    RowData rdat = new RowData(grant_table);
    rdat.setColumnDataFromObject(0, privs.toEncodedString());
    rdat.setColumnDataFromObject(1, new BigDecimal(object));
    rdat.setColumnDataFromObject(2, param);
    rdat.setColumnDataFromObject(3, grantee);
    rdat.setColumnDataFromObject(4, new Boolean(grant_option));
    rdat.setColumnDataFromObject(5, granter);
    grant_table.add(rdat);

  }

  /**
   * Completely removes all privs granted on the given object for all users.
   * This would typically be used when the object is dropped from the database.
   */
  public void revokeAllGrantsOnObject(int object, String param)
                                                    throws DatabaseException {
    // The system grants table.
    DataTable grant_table = connection.getTable(Database.SYS_GRANTS);

    Variable object_col = grant_table.getResolvedVariable(1);
    Variable param_col = grant_table.getResolvedVariable(2);
    // All that match the given object
    Table t1 = grant_table.simpleSelect(context, object_col,
                  Operator.get("="), new Expression(new BigDecimal(object)));
    // All that match the given parameter
    t1 = t1.simpleSelect(context,
                         param_col, Operator.get("="), new Expression(param));

    // Remove these rows from the table
    grant_table.delete(t1);

  }

  /**
   * Returns all Privileges for the given object for the given grantee (user).
   * This would be used to determine the access a user has to a table.
   * <p>
   * This method will concatanate multiple privs granted on the same
   * object.
   */
  public Privileges userGrants(int object, String param, String username)
                                                    throws DatabaseException {
    // The system grants table.
    DataTable grant_table = connection.getTable(Database.SYS_GRANTS);

    Variable object_col = grant_table.getResolvedVariable(1);
    Variable param_col = grant_table.getResolvedVariable(2);
    Variable grantee_col = grant_table.getResolvedVariable(3);
    Operator EQUALS = Operator.get("=");
    // All that match the given username
    Table t1 = grant_table.simpleSelect(context, grantee_col,
                                        EQUALS, new Expression(username));
    // All that match the given object
    t1 = t1.simpleSelect(context, object_col,
                         EQUALS, new Expression(new BigDecimal(object)));
    // All that match the given object parameter
    t1 = t1.simpleSelect(context, param_col, EQUALS, new Expression(param));

    Privileges privs = new Privileges();
    RowEnumeration e = t1.rowEnumeration();
    while (e.hasMoreRows()) {
      int row_index = e.nextRowIndex();
      String encoded_str = (String) t1.getCellContents(0, row_index).getCell();
      privs.merge(Privileges.fromEncodedString(encoded_str));
    }

    return privs;
  }






}
