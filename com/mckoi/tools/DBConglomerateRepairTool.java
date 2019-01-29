/**
 * com.mckoi.tools.DBConglomerateRepairTool  11 Apr 2001
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

import com.mckoi.database.*;
import com.mckoi.util.CommandLine;
import com.mckoi.util.ShellUserTerminal;
import com.mckoi.debug.Debug;
import javax.swing.*;
import javax.swing.event.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;

/**
 * A command line repair tool for repairing a corrupted conglomerate.
 *
 * @author Tobias Downer
 */

public class DBConglomerateRepairTool {

  private static void repair(String path, String name) {

    ShellUserTerminal terminal = new ShellUserTerminal();

    TransactionSystem system = new TransactionSystem();
    final TableDataConglomerate conglomerate =
                                          new TableDataConglomerate(system);
    // Check it.
    conglomerate.fix(new File(path), name, terminal);

  }

  /**
   * Prints the syntax.
   */
  private static void printSyntax() {
    System.out.println("DBConglomerateRepairTool -path [data directory] " +
                       "[-name [database name]]");
  }

  /**
   * Application start point.
   */
  public static void main(String[] args) {
    CommandLine cl = new CommandLine(args);

    String path = cl.switchArgument("-path");
    String name = cl.switchArgument("-name", "DefaultDatabase");

    Debug.setDebugLevel(50000);

    if (path == null) {
      printSyntax();
      System.out.println("Error: -path not found on command line.");
      System.exit(-1);
    }

    // Start the tool.
    repair(path, name);

  }


}
