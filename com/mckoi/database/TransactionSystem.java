/**
 * com.mckoi.database.TransactionSystem  24 Mar 2002
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

import com.mckoi.util.Stats;
import com.mckoi.util.StringUtil;
import com.mckoi.debug.*;
import com.mckoi.database.control.DBConfig;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
//import java.util.ResourceBundle;
//import java.util.MissingResourceException;
import java.util.Properties;

/**
 * A class that provides information and global functions for the transaction
 * layer in the engine.  Shared information includes configuration details,
 * logging, etc.
 *
 * @author Tobias Downer
 */

public class TransactionSystem {

  /**
   * The stats object that keeps track of database statistics.
   */
  private final Stats stats = new Stats();

  /**
   * A logger to output any debugging messages.
   * NOTE: This MUST be final, because other objects may retain a reference
   *   to the object.  If it is not final, then different objects will be
   *   logging to different places if this reference is changed.
   */
  private final DefaultDebugLogger logger;

  /**
   * The ResourceBundle that contains properties of the entire database
   * system.
   */
  private DBConfig config = null;

  /**
   * Set to true if lookup comparison lists are enabled.
   */
  private boolean lookup_comparison_list_enabled = false;

  /**
   * Set to true if the database is in read only mode.  This is set from the
   * configuration file.
   */
  private boolean read_only_access = false;

  /**
   * Set to true if locking checks should be performed each time a table is
   * accessed.
   */
  private boolean table_lock_check = false;

  /**
   * Set to false if there is conservative index memory storage.  If true,
   * all root selectable schemes are stored behind a soft reference that will
   * be garbage collected.
   */
  private boolean soft_index_storage = false;

  /**
   * If this is set to true, during boot up the engine will reindex all the
   * tables that weren't closed.  If false, the engine will only reindex the
   * tables that have unchecked in modifications.
   */
  private boolean always_reindex_dirty_tables = false;

  /**
   * Set to true if the file handles should NOT be synchronized with the
   * system file IO when the indices are written.  If this is true, then the
   * database is not as fail safe, however File IO performance is improved.
   */
  private boolean dont_synch_filesystem = false;

  /**
   * Set to true if the parser should ignore case when searching for a schema,
   * table or column using an identifier.
   */
  private boolean ignore_case_for_identifiers = false;

  /**
   * Transaction option, if this is true then a transaction error is generated
   * during commit if a transaction selects data from a table that has
   * committed changes to it during commit time.
   * <p>
   * True by default.
   */
  private boolean transaction_error_on_dirty_select = true;

  /**
   * The DataCellCache that is a shared resource between on database's.
   */
  private DataCellCache data_cell_cache = null;

  /**
   * The list of FunctionFactory objects that handle different functions from
   * SQL.
   */
  private ArrayList function_factory_list;

  /**
   * The FunctionLookup object that can resolve a FunctionDef object to a
   * Function object.
   */
  private DSFunctionLookup function_lookup;

  /**
   * The regular expression library bridge for the library we are configured
   * to use.
   */
  private RegexLibrary regex_library;




  /**
   * The log directory.
   */
  private File log_directory;


  /**
   * Constructor.
   */
  public TransactionSystem() {
    // Setup generate properties from the JVM.
    logger = new DefaultDebugLogger();
    Properties p = System.getProperties();
    stats.set(0, "Runtime.java.version: " + p.getProperty("java.version"));
    stats.set(0, "Runtime.java.vendor: " + p.getProperty("java.vendor"));
    stats.set(0, "Runtime.java.vm.name: " + p.getProperty("java.vm.name"));
    stats.set(0, "Runtime.os.name: " + p.getProperty("os.name"));
    stats.set(0, "Runtime.os.arch: " + p.getProperty("os.arch"));
    stats.set(0, "Runtime.os.version: " + p.getProperty("os.version"));
  }

  /**
   * Returns a configuration value, or the default if it's not found.
   */
  public final String getConfigString(String property, String default_val) {
    String v = config.getValue(property);
    if (v == null) {
      return default_val;
    }
    return v.trim();
  }

  /**
   * Returns a configuration value, or the default if it's not found.
   */
  public final int getConfigInt(String property, int default_val) {
    String v = config.getValue(property);
    if (v == null) {
      return default_val;
    }
    return Integer.parseInt(v);
  }

  /**
   * Returns a configuration value, or the default if it's not found.
   */
  public final boolean getConfigBoolean(String property, boolean default_val) {
    String v = config.getValue(property);
    if (v == null) {
      return default_val;
    }
    return v.trim().equalsIgnoreCase("enabled");
  }


  /**
   * Inits the TransactionSystem with the configuration properties of the
   * system.
   * This can only be called once, and should be called at database boot time.
   */
  public void init(DBConfig config) {

    function_factory_list = new ArrayList();
    function_lookup = new DSFunctionLookup();

    if (config != null) {
      this.config = config;

      // Register the internal function factory,
      addFunctionFactory(new InternalFunctionFactory());

      String status;

      // Set up the DataCellCache from the values in the configuration
      int max_cache_size = 0, max_cache_entry_size = 0;

      max_cache_size = getConfigInt("data_cache_size", 0);
      max_cache_entry_size = getConfigInt("max_cache_entry_size", 0);

      if (max_cache_size >= 4096 &&
          max_cache_entry_size >= 16 &&
          max_cache_entry_size < (max_cache_size / 2)) {

        Debug().write(Lvl.MESSAGE, this,
                "Internal Data Cache size:          " + max_cache_size);
        Debug().write(Lvl.MESSAGE, this,
                "Internal Data Cache max cell size: " + max_cache_entry_size);

        // Find a prime hash size depending on the size of the cache.
        int hash_size = DataCellCache.closestPrime(max_cache_size / 55);

        // Set up the data_cell_cache
        data_cell_cache = new DataCellCache(this,
                            max_cache_size, max_cache_entry_size, hash_size);

      }
      else {
        Debug().write(Lvl.MESSAGE, this,
                    "Internal Data Cache disabled.");
      }

      // Are lookup comparison lists enabled?
      lookup_comparison_list_enabled =
                            getConfigBoolean("lookup_comparison_list", false);
      Debug().write(Lvl.MESSAGE, this,
                "lookup_comparison_list = " + lookup_comparison_list_enabled);

      // Should we open the database in read only mode?
      read_only_access = getConfigBoolean("read_only", false);
      Debug().write(Lvl.MESSAGE, this,
                    "read_only = " + read_only_access);
      if (read_only_access) stats.set(1, "DatabaseSystem.read_only");

      // Hard Sync file system whenever we update index files?
      dont_synch_filesystem = getConfigBoolean("dont_synch_filesystem", false);
      Debug().write(Lvl.MESSAGE, this,
                    "dont_synch_filesystem = " + dont_synch_filesystem);

      // Generate transaction error if dirty selects are detected?
      transaction_error_on_dirty_select =
               getConfigBoolean("transaction_error_on_directy_select", true);
      Debug().write(Lvl.MESSAGE, this, "transaction_error_on_dirty_select = " +
                    transaction_error_on_dirty_select);

      // Case insensitive identifiers?
      ignore_case_for_identifiers =
                       getConfigBoolean("ignore_case_for_identifiers", false);
      Debug().write(Lvl.MESSAGE, this,
              "ignore_case_for_identifiers = " + ignore_case_for_identifiers);

      // What regular expression library are we using?
      // If we engine to support other libraries then include the additional
      // entries here.  The bridge interface for the additional libraries must
      // also be written.

      // Test to see if the regex API exists
      boolean regex_api_exists;
      try {
        Class.forName("java.util.regex.Pattern");
        regex_api_exists = true;
        Debug().write(Lvl.MESSAGE, this,
                      "Using Java regex API.");
      }
      catch (ClassNotFoundException e) {
        // Internal API doesn't exist
        regex_api_exists = false;
        Debug().write(Lvl.MESSAGE, this,
                      "Java regex API not available.");
      }

      String regex_bridge = null;
      String lib = getConfigString("regex_library", null);
      if (regex_api_exists) {
        regex_bridge = "com.mckoi.database.regexbridge.JavaRegex";
      }
      else if (lib != null) {
        if (lib.equals("org.apache.regexp")) {
          regex_bridge = "com.mckoi.database.regexbridge.ApacheRegex";
        }
        else if (lib.equals("gnu.regexp")) {
          regex_bridge = "com.mckoi.database.regexbridge.GNURegex";
        }
      }

      if (regex_bridge != null) {
        try {
          Class c = Class.forName(regex_bridge);
          regex_library = (RegexLibrary) c.newInstance();
        }
        catch (Throwable e) {
          Debug().write(Lvl.WARNING, this,
                      "Unable to load regex bridge: " + regex_bridge);
          Debug().writeException(Lvl.WARNING, e);
        }
      }
      else {
        Debug().write(Lvl.WARNING, this,
                      "Regex library not known: " + lib);
      }

      // ---------- Plug ins ---------

      try {
        // The 'function_factories' property.
        String function_factories =
                                 getConfigString("function_factories", null);
        if (function_factories != null) {
          List factories = StringUtil.explode(function_factories, ";");
          for (int i = 0; i < factories.size(); ++i) {
            String factory_class = factories.get(i).toString();
            Class c = Class.forName(factory_class);
            FunctionFactory fun_factory = (FunctionFactory) c.newInstance();
            addFunctionFactory(fun_factory);
            Debug().write(Lvl.MESSAGE, this,
                    "Successfully added function factory: " + factory_class);
          }
        }
        else {
          Debug().write(Lvl.MESSAGE, this,
                      "No 'function_factories' config property found.");
          // If resource missing, do nothing...
        }
      }
      catch (Throwable e) {
        Debug().write(Lvl.ERROR, this,
                "Error parsing 'function_factories' configuration property.");
        Debug().writeException(e);
      }

      // Flush the contents of the function lookup object.
      flushCachedFunctionLookup();

    }

  }

  /**
   * Hack - set up the DataCellCache in DatabaseSystem so we can use the
   * MasterTableDataSource object without having to boot a new DatabaseSystem.
   */
  public void setupRowCache(int max_cache_size,
                            int max_cache_entry_size) {
    // Set up the data_cell_cache
    data_cell_cache =
               new DataCellCache(this, max_cache_size, max_cache_entry_size);
  }

  /**
   * Returns true if the database is in read only mode.  In read only mode,
   * any 'write' operations are not permitted.
   */
  public boolean readOnlyAccess() {
    return read_only_access;
  }

  /**
   * Returns true if the database should perform checking of table locks.
   */
  public boolean tableLockingEnabled() {
    return table_lock_check;
  }

  /**
   * Returns true if we should generate lookup caches in InsertSearch otherwise
   * returns false.
   */
  public boolean lookupComparisonListEnabled() {
    return lookup_comparison_list_enabled;
  }

  /**
   * Returns true if all table indices are kept behind a soft reference that
   * can be garbage collected.
   */
  public boolean softIndexStorage() {
    return soft_index_storage;
  }

  /**
   * Returns the status of the 'always_reindex_dirty_tables' property.
   */
  public boolean alwaysReindexDirtyTables() {
    return always_reindex_dirty_tables;
  }

  /**
   * Returns true if we shouldn't synchronize with the file system when
   * important indexing information is flushed to the disk.
   */
  public boolean dontSynchFileSystem() {
    return dont_synch_filesystem;
  }

  /**
   * Returns true if during commit the engine should look for any selects
   * on a modified table and fail if they are detected.
   */
  public boolean transactionErrorOnDirtySelect() {
    return transaction_error_on_dirty_select;
  }

  /**
   * Returns true if the parser should ignore case when searching for
   * schema/table/column identifiers.
   */
  public boolean ignoreIdentifierCase() {
    return ignore_case_for_identifiers;
  }

  /**
   * Returns the regular expression library from the configuration file.
   */
  public RegexLibrary getRegexLibrary() {
    if (regex_library != null) {
      return regex_library;
    }
    throw new Error("No regular expression library found in classpath " +
                    "and/or in configuration file.");
  }

  // ---------- Debug logger methods ----------

  /**
   * Sets the Writer output for the debug logger.
   */
  public final void setDebugOutput(java.io.Writer writer) {
//    System.out.println("**** Setting debug log output ****" + writer);
//    System.out.println(logger);
    logger.setOutput(writer);
  }

  /**
   * Sets the debug minimum level that is output to the logger.
   */
  public final void setDebugLevel(int level) {
    logger.setDebugLevel(level);
  }

  /**
   * Returns the DebugLogger object that is used to log debug message.  This
   * method must always return a debug logger that we can log to.
   */
  public final DebugLogger Debug() {
    return logger;
  }

  // ---------- Function factories ----------

  /**
   * Registers a new FunctionFactory with the database system.  The function
   * factories are used to resolve a function name into a Function object.
   * Function factories are checked in the order they are added to the database
   * system.
   */
  public void addFunctionFactory(FunctionFactory factory) {
    synchronized (function_lookup) {
      function_factory_list.add(factory);
    }
    factory.init();
  }

//  /**
//   * Uses the function factory list to resolve a function name into a
//   * Function object that is used within an expression.  The given
//   * Expression[] parameter is the list of arguments to give to the
//   * function.  This method may throw an exception if the expression
//   * parameters are of the wrong type or there are an incorrect number
//   * of them given.
//   * <p>
//   * Returns 'null' if there are no function factories that can handle this
//   * function name.
//   */
//  public Function resolveFunctionName(
//                           String function_name, Expression[] parameters) {
//    for (int i = 0 ; i < function_factory_list.size(); ++i) {
//      FunctionFactory factory =
//                            (FunctionFactory) function_factory_list.get(i);
//      Function fun = factory.generateFunction(function_name, parameters);
//      if (fun != null) {
//        return fun;
//      }
//    }
//    return null;
//  }

  /**
   * Flushes the 'FunctionLookup' object returned by the getFunctionLookup
   * method.  This should be called if the function factory list has been
   * modified in some way.
   */
  public void flushCachedFunctionLookup() {
    synchronized (function_lookup) {
      FunctionFactory[] factories = (FunctionFactory[])
               function_factory_list.toArray(
                          new FunctionFactory[function_factory_list.size()]);
      function_lookup.flushContents(factories);
    }
  }

  /**
   * Returns a FunctionLookup object that will search through the function
   * factories in this database system and find and resolve a function.  The
   * returned object may throw an exception from the 'generateFunction' method
   * if the FunctionDef is invalid.  For example, if the number of parameters
   * is incorrect or the name can not be found.
   */
  public FunctionLookup getFunctionLookup() {
    synchronized (function_lookup) {
      return function_lookup;
    }
  }

  /**
   * Returns an ExpressionPreparer object that uses the DatabaseSystem
   * FunctionLookup to resolve functions.
   */
  public ExpressionPreparer getFunctionExpressionPreparer() {
    final FunctionLookup function_lookup = getFunctionLookup();
    return new ExpressionPreparer() {
      public boolean canPrepare(Object element) {
        return element instanceof FunctionDef;
      }
      public Object prepare(Object element) throws DatabaseException {
        FunctionDef fdef = (FunctionDef) element;
        Function f = function_lookup.generateFunction(fdef);
        if (f != null) {
          return f;
        }
        throw new DatabaseException("Function '" + fdef.getName() +
                                    "' not found.");
      }
    };
  }

  // ---------- System preparers ----------

  /**
   * Given a Transaction.CheckExpression, this will prepare the expression and
   * return a new prepared CheckExpression.  The default implementation of this
   * is to do nothing.  However, a sub-class of the system choose to prepare
   * the expression, such as resolving the functions via the function lookup,
   * and resolving the sub-queries, etc.
   */
  public Transaction.CheckExpression prepareTransactionCheckConstraint(
                 DataTableDef table_def, Transaction.CheckExpression check) {

    ExpressionPreparer expression_preparer = getFunctionExpressionPreparer();
    // Resolve the expression to this table and row and evaluate the
    // check constraint.
    Expression exp = check.expression;
    table_def.resolveColumns(ignoreIdentifierCase(), exp);
    try {
      // Prepare the functions
      exp.prepare(expression_preparer);
    }
    catch (Exception e) {
      Debug().writeException(e);
      throw new RuntimeException(e.getMessage());
    }

    return check;
  }

  // ---------- Database System Statistics Methods ----------

  /**
   * Returns a com.mckoi.util.Stats object that can be used to keep track
   * of database statistics for this VM.
   */
  public final Stats stats() {
    return stats;
  }

  // ---------- Log directory management ----------

  /**
   * Sets the log directory.  This should preferably be called during
   * initialization.  If the log directory is not set or is set to 'null' then
   * no logging to files occurs.
   */
  public final void setLogDirectory(File log_path) {
    this.log_directory = log_path;
  }

  /**
   * Returns the current log directory or null if no logging should occur.
   */
  public final File getLogDirectory() {
    return log_directory;
  }

  // ---------- Trigger methods ----------

  /**
   * The database system TriggerManager object.
   */
  private TriggerManager trigger_manager;

  /**
   * Returns the TriggerManager for this database system.  This object can
   * be used to manage triggers.  If used correctly, this will dispatch
   * trigger events to the correct listeners on the DatabaseDispatcher thread.
   */
  public TriggerManager getTriggerManager() {
    synchronized(this) {
      if (trigger_manager == null) {
        trigger_manager = new TriggerManager(this);
      }
      return trigger_manager;
    }
  }

  // ---------- Cache Methods ----------

  /**
   * Returns a DataCellCache object that is a shared resource between all
   * database's running on this VM.  If this returns 'null' then the internal
   * cache is disabled.
   */
  DataCellCache getDataCellCache() {
    return data_cell_cache;
  }

  // ---------- Dispatch methods ----------

  /**
   * The dispatcher.
   */
  private DatabaseDispatcher dispatcher;

  /**
   * Returns the DatabaseDispatcher object.
   */
  private DatabaseDispatcher getDispatcher() {
    synchronized (this) {
      if (dispatcher == null) {
        dispatcher = new DatabaseDispatcher(this);
      }
      return dispatcher;
    }
  }

  /**
   * Creates an event object that is passed into 'postEvent' method
   * to run the given Runnable method after the time has passed.
   * <p>
   * The event created here can be safely posted on the event queue as many
   * times as you like.  It's useful to create an event as a persistant object
   * to service some event.  Just post it on the dispatcher when you want
   * it run!
   */
  Object createEvent(Runnable runnable) {
    return getDispatcher().createEvent(runnable);
  }

  /**
   * Adds a new event to be dispatched on the queue after 'time_to_wait'
   * milliseconds has passed.
   * <p>
   * 'event' must be an event object returned via 'createEvent'.
   */
  void postEvent(int time_to_wait, Object event) {
    getDispatcher().postEvent(time_to_wait, event);
  }


  /**
   * Disposes this object.
   */
  void dispose() {
    regex_library = null;
    data_cell_cache = null;
    config = null;
    log_directory = null;
    function_factory_list = null;
    trigger_manager = null;
    dispatcher = null;
  }


  // ---------- Inner classes ----------

  /**
   * A FunctionLookup implementation that will look up a function from a
   * list of FunctionFactory objects provided with.
   */
  private static class DSFunctionLookup implements FunctionLookup {

    private FunctionFactory[] factories;

    public Function generateFunction(FunctionDef function_def) {
      for (int i = 0; i < factories.length; ++i) {
        Function f = factories[i].generateFunction(function_def);
        if (f != null) {
          return f;
        }
      }
      return null;
    }

    public void flushContents(FunctionFactory[] factories) {
      this.factories = factories;
    }

  }

}
