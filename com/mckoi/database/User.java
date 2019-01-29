/**
 * com.mckoi.database.User  22 Jul 2000
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

import com.mckoi.util.Cache;

/**
 * Encapsulates the information about a single user logged into the system.
 * The class provides access to information in the user database.
 * <p>
 * This object also serves as a storage for session state information.  For
 * example, this object stores the triggers that this session has created.
 * <p>
 * NOTE: This object is not immutable.  The same user may log into the system
 *   and it will result in a new User object being created.
 *
 * @author Tobias Downer
 */

public final class User {

  /**
   * A variable that turns on/off system sUSRGrant priv lookup.  If this is
   * true then all users get full access to the entire database.
   */
  private final static boolean NO_GRANT_LOOKUP = true;

  /**
   * The name of the user.
   */
  private String user_name;

  /**
   * The database object that this user is currently logged into.
   */
  private Database database;

  /**
   * The connection string that identifies how this user is connected to the
   * database.
   */
  private String connection_string;

  /**
   * A HashMap cache of privileges this user has for the various tables in
   * the database.  This cache is populated as the user 'visits' a table.
   */
  private Cache priv_cache;

  /**
   * The time this user connected.
   */
  private long time_connected;

  /**
   * The last time this user executed a command on the connection.
   */
  private long last_command_time;

  /**
   * The Constructor.  This takes a user name and gets the privs for them.
   * <p>
   * Note that this method should only be created from within a Database
   * object.
   */
  User(String user_name, Database database,
       String connection_string, long time_connected) {
    this.user_name = user_name;
    this.database = database;
    this.connection_string = connection_string;
    this.time_connected = time_connected;
    this.last_command_time = time_connected;
    this.priv_cache = new Cache(101, 101, 20);
  }

  /**
   * Returns the name of the user.
   */
  public String getUserName() {
    return user_name;
  }

  /**
   * Returns the string that describes how this user is connected to the
   * engine.  This is set by the protocol layer.
   */
  public String getConnectionString() {
    return connection_string;
  }

  /**
   * Returns the time the user connected.
   */
  public long getTimeConnected() {
    return time_connected;
  }

  /**
   * Returnst the last time a command was executed by this user.
   */
  public long getLastCommandTime() {
    return last_command_time;
  }

  /**
   * Returns the Database object that this user belongs to.
   */
  public Database getDatabase() {
    return database;
  }

  /**
   * Refreshes the last time a command was executed by this user.
   */
  public final void refreshLastCommandTime() {
    last_command_time = System.currentTimeMillis();
  }

  /**
   * Flushes the privilege cache so the next time the user requests a
   * priv for a table it will cause a full lookup.  This should be called
   * when the sUSRGrant table is modified and the grants for tables in the
   * database have changed.
   */
  public void flushPrivilegeCache() {
    synchronized (priv_cache) {
      priv_cache.removeAll();
    }
  }

  /**
   * Closes this user object and puts it into a invalidated state.  This
   * will remove all listeners that were created on this session.
   * <p>
   * This will also log the user out of the user manager.
   */
  public void close() {
    // Clear all triggers for this user,
//    DatabaseSystem.getTriggerManager().clearAllUserTriggers(this);
    UserManager user_manager = database.getUserManager();
    if (user_manager != null) {
      user_manager.userLoggedOut(this);
    }
    // Invalidate this object.
    user_name = null;
    database = null;
    priv_cache = null;
  }

  /**
   * Returns the Privileges object for this user on the given table of the
   * connection.
   */
  private Privileges getUserPrivs(DatabaseConnection connection,
                                  TableName table) throws DatabaseException {

    // If no grant lookup...
    if (NO_GRANT_LOOKUP) {
      if (table.getSchema().equals("SYS_INFO")) {
        String tname = table.getName();
        if (tname.equals("sUSRFunction") ||
            tname.equals("sUSRFunctionFactory") ||
            tname.equals("sUSRPassword") ||
            tname.equals("sUSRService") ||
            tname.equals("sUSRUserConnectPriv") ||
            tname.equals("sUSRUserPriv")) {
          return Privileges.ALL_PRIVS;
        }
        else {
          return Privileges.READ_PRIVS;
        }
      }
      else {
        return Privileges.ALL_PRIVS;
      }
    }

    Privileges p;
    // Make sure the priv cache is synchronized
    synchronized (priv_cache) {
      p = (Privileges) priv_cache.get(table);
    }
    if (p == null) {
      p = connection.getGrantManager().userGrants(
                        GrantManager.TABLE, table.toString(), getUserName());
      // Make sure the priv cache is synchronized
      synchronized (priv_cache) {
        priv_cache.put(table, p);
      }
    }
    return p;
  }

  /**
   * Returns true if the table given is within the system schema, and therefore
   * should not be able to be changed by any user.
   */
  private boolean isSystemTable(TableName table) {
//    System.out.println(table);
    boolean system_schema = table.getSchema().equals(Database.SYSTEM_SCHEMA);
//    if (system_schema) {
//      // Exceptions (for now)
//      if (table.getName().equals("sUSRPassword") ||
//          table.getName().equals("sUSRUserPriv")) {
//        return false;
//      }
//    }

    return system_schema;
  }

  /**
   * Returns true if this user can create and drop tables.  (ie. checks whether
   * the user belongs to the 'secure access' priv group.
   * <p>
   * ASSUMPTION: Modification of 'sUSR???' tables requires exclusive lock on
   *   the database, and we are currently shared locked.
   */
  public boolean canCreateAndDrop(DatabaseConnection connection)
                                                 throws DatabaseException {
//    // Select all priv groups that the user belongs to.
//    Table t1 = database.getUserPrivTable().select(
//           new Condition("sUSRUserPriv.UserName", Condition.EQUALS,
//                                   ValueSubstitution.fromObject(user_name)));
//    // Select 'secure access' from the list.
//    Table t2 = t1.select(
//           new Condition("sUSRUserPriv.PrivGroupName", Condition.EQUALS,
//                             ValueSubstitution.fromObject("secure access")));
//
//    if (t2.getRowCount() > 0) {
//      return true;
//    }
//    return false;
    return true;
  }

  /**
   * Returns true if this user can create and drop schema.
   */
  public boolean canCreateAndDropSchema(DatabaseConnection connection)
                                                 throws DatabaseException {
    // Currently we return true
    return true;
  }

  /**
   * Returns true if the user is allowed to shut down the database process
   * on the server.
   */
  public boolean canShutDown(DatabaseConnection connection)
                                                 throws DatabaseException {
    return canCreateAndDrop(connection);
  }

  /**
   * Returns true if this user is allowed to create a table with the given
   * table name.
   */
  public boolean canCreateTable(DatabaseConnection connection,
                                TableName table) throws DatabaseException {
    // Forbid creation of tables in SYS_INFO schema
    if (isSystemTable(table)) {
      return false;
    }

    return canCreateAndDrop(connection);
  }

  /**
   * Returns true if this user is allowed to drop a table with the given table
   * name.
   */
  public boolean canDropTable(DatabaseConnection connection,
                              TableName table) throws DatabaseException {
    // Forbid dropping tables in SYS_INFO schema
    if (isSystemTable(table)) {
      return false;
    }

    return canCreateAndDrop(connection);
  }

  /**
   * Returns true if this user can alter the table with the given name.
   */
  public boolean canAlterTable(DatabaseConnection connection,
                               TableName table) throws DatabaseException {

    // Forbid altering tables in SYS_INFO schema
    if (isSystemTable(table)) {
      return false;
    }

    return canCreateAndDrop(connection);
  }

  /**
   * Returns true if this user can compact the table with the given name.
   */
  public boolean canCompactTable(DatabaseConnection connection,
                                 TableName table) throws DatabaseException {

    Privileges user_privs = getUserPrivs(connection, table);
    return user_privs.permits(Privileges.COMPACT);

  }

  /**
   * Returns true if this user can delete records from the table with the
   * given name.
   */
  public boolean canDeleteFromTable(DatabaseConnection connection,
                                    TableName table) throws DatabaseException {

    Privileges user_privs = getUserPrivs(connection, table);
    return user_privs.permits(Privileges.DELETE);

  }

  /**
   * Returns true if this user can select records from the table with the
   * given name.
   */
  public boolean canSelectFromTable(DatabaseConnection connection,
             TableName table, Variable[] columns) throws DatabaseException {

    Privileges user_privs = getUserPrivs(connection, table);
    return user_privs.permits(Privileges.SELECT);

  }

  /**
   * Returns true if this user can insert records to the table with the
   * given name.
   */
  public boolean canInsertIntoTable(DatabaseConnection connection,
             TableName table, Variable[] columns) throws DatabaseException {

    Privileges user_privs = getUserPrivs(connection, table);
    return user_privs.permits(Privileges.INSERT);

  }

  /**
   * Returns true if this user can update records in the table with the
   * given name.
   */
  public boolean canUpdateTable(DatabaseConnection connection,
              TableName table, Variable[] columns) throws DatabaseException {

    Privileges user_privs = getUserPrivs(connection, table);
    return user_privs.permits(Privileges.UPDATE);

  }

  // ---------- Static methods ----------

  /**
   * Tries to authenticate a username/password against the given database.  If
   * we fail to authenticate a 'null' object is returned, otherwise a valid
   * User object is returned.  If a valid object is returned, the user
   * will be logged into the engine via the UserManager object (in
   * DatabaseSystem).  The developer must ensure that 'close' is called before
   * the object is disposed (logs out of the system).
   * <p>
   * NOTE: deprecate this in favour of Database.authenticateUser?
   */
  public static User authenticate(Database database,
                  String username, String password,
                  String connection_string) {

    return database.authenticateUser(username, password, connection_string);

  }

}
