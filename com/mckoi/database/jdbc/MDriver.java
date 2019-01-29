/**
 * com.mckoi.database.jdbc.MDriver  19 Jul 2000
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

package com.mckoi.database.jdbc;

import com.mckoi.database.control.DBConfig;
import com.mckoi.database.control.DefaultDBConfig;

import java.sql.*;
import java.io.*;
import java.util.Properties;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * JDBC implementation of the driver for the Mckoi database.
 * <p>
 * The url protocol is as follows:<p>
 * <pre>
 *  For connecting to a remote database server:
 *    jdbc:mckoi:[//hostname[:portnum]/][schema_name/]
 *
 *  eg.  jdbc:mckoi://db.mckoi.com:7009/
 *
 *  If hostname is not provided then it defaults to localhost.
 *  If portnum is not provided it defaults to 9157.
 *  If schema_name is not provided it defaults to APP.
 *
 *  To start up a database in the local file system the protocol is:
 *    jdbc:mckoi:local://databaseconfiguration/[schema_name/]
 *
 *  eg.  jdbc:mckoi:local://D:/dbdata/db.conf
 *
 *  If schema_name is not provided it defaults to APP.
 *
 *  To create a database in the local file system then you need to supply a
 *  'create=true' assignment in the URL encoding.
 *
 *  eg.  jdbc:mckoi:local://D:/dbdata/db.conf?create=true
 * </pre>
 * <p>
 * A local database runs within the JVM of this JDBC driver.  To boot a
 * local database, you must include the full database .jar release with
 * your application distribution.
 * <p>
 * For connecting to a remote database using the remote URL string, only the
 * JDBC driver need be included in the classpath.
 * <p>
 * NOTE: This needs to be a light-weight object, because a developer could
 *   generate multiple instances of this class.  Making an instance of
 *   'com.mckoi.JDBCDriver' will create at least two instances of this object.
 *
 * @author Tobias Downer
 */

public class MDriver implements Driver {

  // The major and minor version numbers of the driver.  This only changes
  // when the JDBC communcation protocol changes.
  static final int DRIVER_MAJOR_VERSION = 0;
  static final int DRIVER_MINOR_VERSION = 70;

  // The name of the driver.
  static final String DRIVER_NAME = "Mckoi JDBC Driver";
  // The version of the driver as a string.
  static final String DRIVER_VERSION =
          "" + DRIVER_MAJOR_VERSION + "." + DRIVER_MINOR_VERSION + "-beta";


  // The protocol URL header string that signifies a Mckoi JDBC connection.
  private static final String mckoi_protocol_url = "jdbc:mckoi:";


  /**
   * Set to true when this driver is registered.
   */
  private static boolean registered = false;


  // ----- Static methods -----

  /**
   * Static method that registers this driver with the JDBC driver manager.
   */
  public synchronized static void register() {
    if (registered == false) {
      try {
        java.sql.DriverManager.registerDriver(new MDriver());
        registered = true;
      }
      catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  // ----- MDriver -----

  /**
   * The timeout for a query in seconds.
   */
  static int QUERY_TIMEOUT = 60;

  /**
   * A reference to the LocalBootable object for this driver.  This object
   * manages connections to an embedded database running with this driver.
   * It is used if the driver receives connections via 'jdbc:mckoi:local://'
   * etc.  In the future, we may want to extend the driver so it supports
   * connections to multiple database sources in the local JVM.
   */
  private LocalBootable local_bootable;

  /**
   * Constructor is public so that instances of the JDBC driver can be
   * created by developers.
   */
  public MDriver() {
  }

  /**
   * Given a URL encoded arguments string, this will extract the var=value
   * pairs and put them in the given Properties object.  For example,
   * the string 'create=true&user=usr&password=passwd' will extract the three
   * values and put them in the Properties object.
   */
  private static void parseEncodedVariables(String url_vars, Properties info) {

    // Parse the url variables.
    StringTokenizer tok = new StringTokenizer(url_vars, "&");
    while (tok.hasMoreTokens()) {
      String token = tok.nextToken().trim().toLowerCase();
      int split_point = token.indexOf("=");
      if (split_point > 0) {
        String key = token.substring(0, split_point);
        String value = token.substring(split_point + 1);
        // Put the key/value pair in the 'info' object.
        info.put(key, value);
      }
      else {
        System.err.println("Ignoring url variable: '" + token + "'");
      }
    } // while (tok.hasMoreTokens())

  }

  /**
   * Makes a connection to a local database.  If a local database connection
   * has not been made then it is created here.
   * <p>
   * Returns a list of two elements, (DatabaseInterface) db_interface and
   * (String) database_name.
   */
  private synchronized Object[] connectToLocal(String url, String address_part,
                                        Properties info) throws SQLException {

    // If the LocalBootable object hasn't been created yet, do so now via
    // reflection.
    String schema_name = "APP";
    DatabaseInterface db_interface;
    if (local_bootable == null) {
      try {
        Class c = Class.forName(
                        "com.mckoi.database.jdbcserver.DefaultLocalBootable");
        local_bootable = (LocalBootable) c.newInstance();
      }
      catch (Throwable e) {
        throw new SQLException(
                             "Unable to create interface to local database.");
      }
    }

    // Look for the name upto the URL encoded variables
    int url_start = address_part.indexOf("?");
    if (url_start == -1) {
      url_start = address_part.length();
    }

    // The path to the configuration
    String config_path = address_part.substring(8, url_start);

    // Substitute win32 '\' to unix style '/'
    config_path = config_path.replace('\\', '/');

    // Check the config file exists,
    File conf_file = new File(config_path);
    if (!conf_file.exists()) {
      // If the configuration file doesn't exist then we may have a
      // [config_path]/[schema_name] type string on our hands.
      int schema_start = config_path.lastIndexOf('/');
      if (schema_start > 0) {
        schema_name = config_path.substring(schema_start + 1);
        config_path = config_path.substring(0, schema_start);
      }
    }

    // The url variables part
    String url_vars = "";
    if (url_start < address_part.length()) {
      url_vars = address_part.substring(url_start + 1).trim();
    }

    // Is there already a local connection?
    if (local_bootable.isBooted()) {
      // Yes, so simply login.
      db_interface = local_bootable.connectToJVM();
    }
    else {
      // Otherwise we need to boot the local database.

      // If no config_path part to URL then assume db.conf
      if (config_path == null) {
        config_path = "./db.conf";
      }

      // Extract the root part of the configuration path.  This is the root
      // directory.
      File absolute_config_path = new File(
                                      new File(config_path).getAbsolutePath());
      File root_path = new File(absolute_config_path.getParent());

      // Get the configuration bundle that was set as the path,
      DefaultDBConfig config = new DefaultDBConfig(root_path);
      try {
        config.loadFromFile(new File(config_path));
      }
      catch (IOException e) {
        throw new SQLException("Unable to find configuration file: " +
                               config_path);
      }

      // Parse the url variables
      parseEncodedVariables(url_vars, info);

      boolean create_db = false;
      boolean create_db_if_not_exist = false;
      create_db = info.getProperty("create", "").equals("true");
      create_db_if_not_exist =
                       info.getProperty("boot_or_create", "").equals("true");

      // Include any properties from the 'info' object
      Enumeration prop_keys = info.keys();
      while (prop_keys.hasMoreElements()) {
        String key = prop_keys.nextElement().toString();
        if (!key.equals("user") && !key.equals("password")) {
          config.setValue(key, (String) info.get(key));
        }
      }

      // Check if the database exists
      boolean database_exists = local_bootable.checkExists(config);

      // If database doesn't exist and we've been told to create it if it
      // doesn't exist, then set the 'create_db' flag.
      if (create_db_if_not_exist && !database_exists) {
        create_db = true;
      }

      // Error conditions;
      // If we are creating but the database already exists.
      if (create_db && database_exists) {
        throw new SQLException(
                "Can not create database because a database already exists.");
      }
      // If we are booting but the database doesn't exist.
      if (!create_db && !database_exists) {
        throw new SQLException(
           "Can not find a database to start.  Either the database needs to " +
           "be created or the 'database_path' property of the configuration " +
           "must be set to located of the data files.");
      }

      // Are we creating a new database?
      if (create_db) {
        String username = info.getProperty("user", "");
        String password = info.getProperty("password", "");

        db_interface = local_bootable.create(username, password, config);
//        ++local_connections;
      }
      // Otherwise we must be logging onto a database,
      else {
        db_interface = local_bootable.boot(config);
//        ++local_connections;
      }
    }

    // Make up the return parameters.
    Object[] ret = new Object[2];
    ret[0] = db_interface;
    ret[1] = schema_name;

    return ret;

  }

//  /**
//   * Called when a connection closes.
//   */
//  static void connectionClosed(Connection connection) {
//    --local_connections;
//  }


  // ---------- Implemented from Driver ----------

  public Connection connect(String url, Properties info) throws SQLException {
    // We looking for url starting with this protocol
    if (!acceptsURL(url)) {
      // If the protocol not valid then return null as in the spec.
      return null;
    }

    DatabaseInterface db_interface;
    String default_schema = "APP";

    int row_cache_size;
    int max_row_cache_size;

    String address_part = url.substring(url.indexOf(mckoi_protocol_url) +
                                        mckoi_protocol_url.length());
    // If we are to connect this JDBC to a single user database running
    // within this JVM.
    if (address_part.startsWith("local://")) {

      // Returns a list of two Objects, db_interface and database_name.
      Object[] ret_list = connectToLocal(url, address_part, info);
      db_interface = (DatabaseInterface) ret_list[0];
      default_schema = (String) ret_list[1];

      // Internal row cache setting are set small.
      row_cache_size = 43;
      max_row_cache_size = 4092000;

    }
    else {
      int port = 9157;
      String host = "127.0.0.1";

      // Otherwise we must be connecting remotely.
      if (address_part.startsWith("//")) {

        String args_string = "";
        int arg_part = address_part.indexOf('?', 2);
        if (arg_part != -1) {
          args_string = address_part.substring(arg_part + 1);
          address_part = address_part.substring(0, arg_part);
        }

//        System.out.println("ADDRESS_PART: " + address_part);

        int end_address = address_part.indexOf("/", 2);
        if (end_address == -1) {
          end_address = address_part.length();
        }
        String remote_address = address_part.substring(2, end_address);
        int delim = remote_address.indexOf(':');
        if (delim == -1) {
          delim = remote_address.length();
        }
        host = remote_address.substring(0, delim);
        if (delim < remote_address.length() - 1) {
          port = Integer.parseInt(remote_address.substring(delim + 1));
        }

//        System.out.println("REMOTE_ADDRESS: '" + remote_address + "'");

        // Schema name?
        String schema_part = "";
        if (end_address < address_part.length()) {
          schema_part = address_part.substring(end_address + 1);
        }
        String schema_string = schema_part;
        int schema_end = schema_part.indexOf('/');
        if (schema_end != -1) {
          schema_string = schema_part.substring(0, schema_end);
        }
        else {
          schema_end = schema_part.indexOf('?');
          if (schema_end != -1) {
            schema_string = schema_part.substring(0, schema_end);
          }
        }

//        System.out.println("SCHEMA_STRING: '" + schema_string + "'");

        // Argument part?
        if (!args_string.equals("")) {
//          System.out.println("ARGS: '" + args_string + "'");
          parseEncodedVariables(args_string, info);
        }

        // Is there a schema or should we default?
        if (schema_string.length() > 0) {
          default_schema = schema_string;
        }

      }
      else {
        if (address_part.trim().length() > 0) {
          throw new SQLException("Malformed URL: " + address_part);
        }
      }

//      database_name = address_part;
//      if (database_name == null || database_name.trim().equals("")) {
//        database_name = "DefaultDatabase";
//      }

      // BUG WORKAROUND:
      // There appears to be a bug in the socket code of some VM
      // implementations.  With the IBM Linux JDK, if a socket is opened while
      // another is closed while blocking on a read, the socket that was just
      // opened breaks.  This was causing the login code to block indefinitely
      // and the connection thread causing a null pointer exception.
      // The workaround is to put a short pause before the socket connection
      // is established.
      try {
        Thread.sleep(85);
      }
      catch (InterruptedException e) { /* ignore */ }

      // Make the connection
      TCPStreamDatabaseInterface tcp_db_interface =
                                   new TCPStreamDatabaseInterface(host, port);
      // Attempt to open a socket to the database.
      tcp_db_interface.connectToDatabase();

      db_interface = tcp_db_interface;

      // For remote connection, row cache uses more memory.
      row_cache_size = 4111;
      max_row_cache_size = 8192000;

    }

//    System.out.println("DEFAULT SCHEMA TO CONNECT TO: " + default_schema);

    // Create the connection object on the given database,
    MConnection connection = new MConnection(url, db_interface,
                                        row_cache_size, max_row_cache_size);
    // Try and login (throws an SQLException if fails).
    connection.login(info, default_schema);

    return connection;
  }

  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(mckoi_protocol_url) ||
           url.startsWith(":" + mckoi_protocol_url);
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
                                                          throws SQLException {
    // Is this for asking for usernames and passwords if they are
    // required but not provided?

    // Return nothing for now, assume required info has been provided.
    return new DriverPropertyInfo[0];
  }

  public int getMajorVersion() {
    return DRIVER_MAJOR_VERSION;
  }

  public int getMinorVersion() {
    return DRIVER_MINOR_VERSION;
  }

  public boolean jdbcCompliant() {
    // Certified compliant? - perhaps one day...
    return false;
  }

}
