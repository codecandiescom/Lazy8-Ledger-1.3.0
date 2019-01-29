/**
 * com.mckoi.tools.DataFileConvertTool  23 Nov 2000
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

package com.mckoi.tools;

import java.io.*;
import com.mckoi.database.*;
import com.mckoi.util.CommandLine;
import com.mckoi.debug.Debug;

/**
 * A tool for converting between different versions of the database file
 * system.
 *
 * @author Tobias Downer
 */

public class DataFileConvertTool {

  /**
   * Converts pre 0.88 style data.
   */
  private static void convertPre088(File src_path, File dst_path) {
    try {
      TransactionSystem system = new TransactionSystem();

      // Set up the row cache.
      system.setupRowCache(4 * 1024 * 1024, 8000);

      TableDescriptions table_desc = new TableDescriptions(src_path);
      table_desc.load();

      // Make a new conglomerate to store this stuff...
      TableDataConglomerate conglomerate = new TableDataConglomerate(system);
      conglomerate.create(dst_path, "DefaultDatabase");

      String[] list = table_desc.getTableList();
      // For each table.
      for (int i = 0; i < list.length; ++i) {

        String tname = list[i];
//        if (!tname.startsWith("sUSR")) {
//          continue;
//        }
        System.out.println("Processing table: " + tname);

        // Load the source table.
        VariableSizeDataTableFile in_table =
                             new VariableSizeDataTableFile(system, src_path);
        in_table.load(tname, true);  // Load read only.
        // The DataTableDef
        DataTableDef table_def = in_table.getDataTableDef();
        table_def.setName(tname);
        if (tname.startsWith("sUSR")) {
          table_def.setSchema("SYS_INFO");
        }
        else {
          table_def.setSchema("");
        }

        // Start a new transaction
        Transaction transaction = conglomerate.createTransaction();
        // Create a table with this name,
        transaction.createTable(table_def);
        MutableTableDataSource out_table = transaction.getTable(
                   new TableName(table_def.getSchema(), table_def.getName()));
        // Set the unique id in the new table with the unique id from the
        // old.
        long unique_id = in_table.nextUniqueKey() + 1;
        for (int p = 0; p < unique_id; ++p) {
          transaction.nextUniqueID(
                   new TableName(table_def.getSchema(), table_def.getName()));
        }

        int col_count = table_def.columnCount();
        int raw_row_count = in_table.vsdtRawRowCount();
        int count = 0;
        for (int n = 0; n < raw_row_count; ++n) {
          if (in_table.isRowValid(n)) {
            if ((n % 18) == 0) {
              System.out.print(".");
            }
            // Read the row contents into a RowData object.
            RowData row_data = new RowData(out_table);
            for (int p = 0; p < col_count; ++p) {
              row_data.setColumnData(p, in_table.getCellContents(p, n));
            }
            // And add to the destination source.
            out_table.addRow(row_data);
            ++count;
          }
        }
        System.out.println("\n[ Count: " + count + "/" +
                           raw_row_count + " ]   Please Wait...");
        try {
          transaction.closeAndCommit();
        }
        catch (TransactionException e) {
          System.out.println("TRANSACTION ERROR: " + e.getMessage());
          throw new Error(e.getMessage());
        }
        // Flush all the journal entries for the table.
        conglomerate.flushJournals(table_def.getSchema(), tname);

      }

      // Close the destination conglomerate.
      conglomerate.close();

    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Converts from one version of the file system to the new version.
   */
  private static void convert(String src_path, String dst_path) {

    File dst = new File(dst_path);
    if (!dst.exists()) {
      dst.mkdirs();
    }

    File path = new File(src_path);
    // If the file, 'MckoiDB.desc' exists then we are converting from
    // pre version 0.88 style format.
    if (new File(path, "MckoiDB.desc").exists()) {
      convertPre088(path, dst);
    }
    else {
      System.out.println("ERROR: Couldn't find a database source to convert.");
    }

  }

  /**
   * Prints the syntax.
   */
  private static void printSyntax() {
    System.out.println("DataFileConvertTool -dbpathsrc [src directory] " +
                       "-dbpathdst [dst directory]");
  }

  /**
   * Application start point.
   */
  public static void main(String[] args) {
    CommandLine cl = new CommandLine(args);

    String dbpathsrc = cl.switchArgument("-dbpathsrc");
    String dbpathdst = cl.switchArgument("-dbpathdst");

    Debug.setDebugLevel(Debug.ALERT);

    if (dbpathsrc == null) {
      printSyntax();
      System.out.println("Error: -dbpathsrc not found on command line.");
      System.exit(-1);
    }
    if (dbpathdst == null) {
      printSyntax();
      System.out.println("Error: -dbpathdst not found on command line.");
      System.exit(-1);
    }

    // Start the conversion.
    convert(dbpathsrc, dbpathdst);

  }

}
