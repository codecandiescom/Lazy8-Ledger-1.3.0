/**
 * com.mckoi.dbcontrol.DBController  26 Mar 2002
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

package com.mckoi.database.control;

import com.mckoi.database.Database;
import com.mckoi.database.DatabaseSystem;
import com.mckoi.database.DatabaseException;
import com.mckoi.debug.*;
import com.mckoi.util.LogWriter;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * An object that provides methods for creating and controlling database
 * systems in the current JVM.
 *
 * @author Tobias Downer
 */

public final class DBController {

  /**
   * This object can not be constructed outside of this package.
   */
  DBController() {
  }

  /**
   * Returns true if a Mckoi database exists in the given directory of the
   * file system, otherwise returns false if the path doesn't contain a
   * database.
   * <p>
   * The path string must be formatted using Unix '/' deliminators at
   * directory separators.
   *
   * @param config the configuration of the database to check the existence
   *   of.
   * @returns true if a database exists at the given path, false otherwise.
   */
  public boolean databaseExists(DBConfig config) {
    Database database = createDatabase(config);
    return database.exists();
  }

  /**
   * Creates a database in the local JVM (and filesystem) given the
   * configuration in DBConfig and returns a DBSystem object.  When this
   * method returns, the database created will be up and running providing
   * there was no failure during the database creation process.
   * <p>
   * A failure might happen because the database path does not exist.
   *
   * @param admin_user the username of the administrator for the new database.
   * @param admin_pass the password of the administrator for the new database.
   * @param config the configuration of the database to create and start in the
   *   local JVM.
   * @returns the DBSystem object used to access the database created.
   */
  public DBSystem createDatabase(DBConfig config,
                                 String admin_user, String admin_pass) {

    // Create the Database object with this configuration.
    Database database = createDatabase(config);
    DatabaseSystem system = database.getSystem();

    // Create the database.
    try {
      database.create(admin_user, admin_pass);
      database.init();
    }
    catch (DatabaseException e) {
      system.Debug().write(Lvl.ERROR, this, "Database create failed");
      system.Debug().writeException(e);
      throw new RuntimeException(e.getMessage());
    }

    // Return the DBSystem object for the newly created database.
    return new DBSystem(this, config, database);

  }

  /**
   * Starts a database in the local JVM given the configuration in DBConfig
   * and returns a DBSystem object.  When this method returns, the database
   * will be up and running providing there was no failure to initialize the
   * database.
   * <p>
   * A failure might happen if the database does not exist in the path given
   * in the configuration.
   *
   * @param config the configuration of the database to start in the local
   *   JVM.
   * @returns the DBSystem object used to access the database started.
   */
  public DBSystem startDatabase(DBConfig config) {

    // Create the Database object with this configuration.
    Database database = createDatabase(config);
    DatabaseSystem system = database.getSystem();

    // First initialise the database
    try {
      database.init();
    }
    catch (DatabaseException e) {
      system.Debug().write(Lvl.ERROR, this, "Database init failed");
      system.Debug().writeException(e);
      throw new RuntimeException(e.getMessage());
    }

    // Return the DBSystem object for the newly created database.
    return new DBSystem(this, config, database);

  }


  // ---------- Static methods ----------

  /**
   * Parses a file string to an absolute position in the file system.  We must
   * provide the path to the root directory (eg. the directory where the
   * config bundle is located).
   */
  private static File parseFileString(File root_path, String root_info,
                                      String path_string) {
    File path = new File(path_string);
    File res;
    // If the path is absolute then return the absoluate reference
    if (path.isAbsolute()) {
      res = path;
    }
    else {
      // If the root path source is the jvm then just return the path.
      if (root_info != null &&
          root_info.equals("jvm")) {
        return path;
      }
      // If the root path source is the configuration file then
      // concat the configuration path with the path string and return it.
      else {
        res = new File(root_path, path_string);
      }
    }
    return res;
  }

  /**
   * Sets up the log file from the config information.
   */
  public static void setupLog(DatabaseSystem system, DBConfig config) {
    String log_path_string = config.getValue("log_path");
    String root_path_var = config.getValue("root_path");

    // If log directory is set...
    if (log_path_string != null) {
      // First set up the debug information in this VM for the 'Debug' class.
      File log_path = parseFileString(config.currentPath(), root_path_var,
                                      log_path_string);
      // If the path doesn't exist the make it.
      if (!log_path.exists()) {
        log_path.mkdirs();
      }
      // Set the log directory in the DatabaseSystem
      system.setLogDirectory(log_path);

      File debug_log_file = new File(log_path,
                                     config.getValue("debug_log_file"));

      LogWriter f_writer;
      try {
        debug_log_file = new File(debug_log_file.getCanonicalPath());

        // Allow log size to grow to 512k and allow 12 archives of the log
        f_writer = new LogWriter(debug_log_file, 512 * 1024, 12);
        f_writer.write("**** Debug log started: " +
                       new Date(System.currentTimeMillis()) + " ****\n");
        f_writer.flush();
      }
      catch (IOException e) {
        throw new RuntimeException(
                       "Unable to open debug file '" + debug_log_file + "'");
      }
      system.setDebugOutput(f_writer);
    }

    int debug_level = Integer.parseInt(config.getValue("debug_level"));
    if (debug_level == -1) {
      system.setDebugLevel(255);
    }
    else {
      system.setDebugLevel(debug_level);
    }
  }

  /**
   * Creates a Database object for the given DBConfig configuration.
   */
  private static Database createDatabase(DBConfig config) {

    DatabaseSystem system = new DatabaseSystem();

    // Set up the log file
    // -------------------
    // ISSUE: Move this into DatabaseSystem initialization?
    setupLog(system, config);

    // Initialize the DatabaseSystem first,
    // ------------------------------------

    // This will throw an Error exception if the database system has already
    // been initialized.
    system.init(config);

    // Start the database class
    // ------------------------

    // The path where the database data files are stored.
    String database_path = config.getValue("database_path");
    // The root path variable
    String root_path_var = config.getValue("root_path");

    // Note, currently we only register one database, and it is named
    //   'DefaultDatabase'.
    Database database = new Database(system, "DefaultDatabase",
                parseFileString(config.currentPath(), root_path_var,
                                database_path));

    // Start up message
    system.Debug().write(Lvl.MESSAGE, DBController.class,
                         "Starting Database Server: " + database_path);

    return database;
  }

  /**
   * Returns the static controller for this JVM.
   */
  public static DBController getDefault() {
    return VM_DB_CONTROLLER;
  }

  /**
   * The static DBController object.
   */
  private final static DBController VM_DB_CONTROLLER = new DBController();

}
