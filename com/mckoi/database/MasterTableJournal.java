/**
 * com.mckoi.database.MasterTableJournal  19 Nov 2000
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

import com.mckoi.util.IntegerVector;
import java.io.*;

/**
 * A journal of changes that occured to a table in a data conglomerate during
 * a transaction.
 *
 * @author Tobias Downer
 */

final class MasterTableJournal {

  /**
   * Journal commands.
   */
  final static byte TABLE_ADD    = 1;  // Add a row to a table.
                                       // (params: table_id, row_index)
  final static byte TABLE_REMOVE = 2;  // Remove a row from a table.
                                       // (params: table_id, row_index)

  /**
   * The commit id given to this change when it is committed.  This is only
   * set when the journal is a committed change to the database.
   */
  private long commit_id;


  /**
   * The master table id.
   */
  private int table_id;

  /**
   * The number of entries in this journal.
   */
  private int journal_entries;

  /**
   * A byte[] array that represents the set of commands a transaction
   * performed on this table.
   */
  private byte[] command_journal;

  /**
   * An IntegerVector that is filled with parameters from the command journal.
   * For example, a 'TABLE_ADD' journal log will have as parameters the
   * row_index that was added to this table.
   */
  private IntegerVector command_parameters;

  /**
   * Constructs the master table journal.
   */
  MasterTableJournal(int table_id) {
    this.table_id = table_id;
    command_journal = new byte[16];
    command_parameters = new IntegerVector(32);
  }

  MasterTableJournal() {
    this(-1);
  }

  /**
   * Sets the 'commit_id'.  This is only set when this change becomes a
   * committed change to the database.
   */
  void setCommitID(long commit_id) {
    this.commit_id = commit_id;
  }

  /**
   * Adds a command to the journal.
   */
  private void addCommand(byte command) {
    if (journal_entries >= command_journal.length) {
      // Resize command array.
      int grow_size = Math.min(4000, journal_entries);
      grow_size = Math.max(4, grow_size);
      byte[] new_command_journal = new byte[journal_entries + grow_size];
      System.arraycopy(command_journal, 0, new_command_journal, 0,
                       journal_entries);
      command_journal = new_command_journal;
    }

    command_journal[journal_entries] = command;
    ++journal_entries;
  }

  /**
   * Adds a parameter to the journal command parameters.
   */
  private void addParameter(int param) {
    command_parameters.addInt(param);
  }

  /**
   * Adds a new command to this journal.
   */
  void addEntry(byte command, int row_index) {
    addCommand(command);
    addParameter(row_index);
  }

  // ---------- Getters ----------
  // These methods assume the journal has been setup and no more entries
  // will be made.

  /**
   * Returns the commit_id that has been set for this journal.
   */
  long getCommitID() {
    return commit_id;
  }

  /**
   * Returns the table id of the master table this journal is for.
   */
  int getTableID() {
    return table_id;
  }

  /**
   * Returns the total number of journal entries.
   */
  int entries() {
    return journal_entries;
  }

  /**
   * Returns the command of the nth entry in the journal.
   */
  byte getCommand(int n) {
    return command_journal[n];
  }

  /**
   * Returns the row index of the nth entry in the journal.
   */
  int getRowIndex(int n) {
    return command_parameters.intAt(n);
  }

  /**
   * Returns a normalized list of all rows that were added in this journal,
   * but not including those rows also removed.  For example, if rows
   * 1, 2, and 3 were added and 2 was removed, this will return a list of
   * 1 and 3.
   */
  int[] normalizedAddedRows() {
    IntegerVector list = new IntegerVector();
    int size = entries();
    for (int i = 0; i < size; ++i) {
      byte tc = getCommand(i);
      if (tc == TABLE_ADD) {
        int row_index = getRowIndex(i);
        // If row added, add to list
        list.addInt(row_index);
      }
      else if (tc == TABLE_REMOVE) {
        // If row removed, if the row is already in the list
        // it's removed from the list, otherwise we leave as is.
        int row_index = getRowIndex(i);
        int found_at = list.indexOf(row_index);
        if (found_at != -1) {
          list.removeIntAt(found_at);
        }
      }
      else {
        throw new Error("Unknown command in journal.");
      }
    }

    return list.toIntArray();
  }

  /**
   * Returns a normalized list of all rows that were removed from this
   * journal.
   */
  int[] normalizedRemovedRows() {
    IntegerVector list = new IntegerVector();
    int size = entries();
    for (int i = 0; i < size; ++i) {
      byte tc = getCommand(i);
      if (tc == TABLE_REMOVE) {
        // If removed add to the list.
        int row_index = getRowIndex(i);
        list.addInt(row_index);
      }
    }
    return list.toIntArray();
  }




  // ---------- Testing methods ----------

  /**
   * Throws a transaction clash exception if it detects a clash between
   * journal entries.  It assumes that this journal is the journal that is
   * attempting to be compatible with the given journal.  A journal clashes
   * when they both contain a row that is deleted.
   */
  void testCommitClash(MasterTableJournal journal)
                                                 throws TransactionException {
    // Very nasty search here...
//    int cost = entries() * journal.entries();
//    System.out.print(" CLASH COST = " + cost + " ");

    for (int i = 0; i < entries(); ++i) {
      byte tc = getCommand(i);
      if (tc == 2) {   // command - row remove
        int row_index = getRowIndex(i);
//        System.out.println("* " + row_index);
        for (int n = 0; n < journal.entries(); ++n) {
//          System.out.print(" " + journal.getRowIndex(n));
          if (journal.getCommand(n) == 2 &&
              journal.getRowIndex(n) == row_index) {
            throw new TransactionException(
               TransactionException.ROW_REMOVE_CLASH,
               "Concurrent Serializable Transaction Conflict(1): " +
               "Current row remove clash ( row: " + row_index + " )");
          }
        }
//        System.out.println();
      }
    }
  }


  // ---------- Stream serialization methods ----------

  /**
   * Writes this journal entries to the given DataOutputStream.
   */
  void writeTo(DataOutputStream dout) throws IOException {
    throw new Error("Method no longer available");
//    dout.writeInt(commit_id);
//    dout.writeInt(table_id);
//    dout.writeInt(journal_entries);
//    dout.write(command_journal, 0, journal_entries);
//    int size = command_parameters.size();
//    dout.writeInt(size);
//    for (int i = 0; i < size; ++i) {
//      dout.writeInt(command_parameters.intAt(i));
//    }
  }

  /**
   * Reads the journal entries from the given DataInputStream to this object.
   */
  void readFrom(DataInputStream din) throws IOException {
    throw new Error("Method no longer available");
//    commit_id = din.readInt();
//    table_id = din.readInt();
////    if (table_id != din.readInt()) {
////      throw new IOException(
////        "'table id' in given format is not the same.  Does this journal " +
////        "really belong to another table?");
////    }
//    journal_entries = din.readInt();
//    command_journal = new byte[journal_entries];
//    din.readFully(command_journal, 0, journal_entries);
//    int size = din.readInt();
//    for (int i = 0; i < size; ++i) {
//      command_parameters.addInt(din.readInt());
//    }
  }

  /**
   * Debugging.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[MasterTableJournal] [");
    buf.append(commit_id);
    buf.append("] (");
    for (int i = 0; i < entries(); ++i) {
      byte c = getCommand(i);
      int row_index = getRowIndex(i);
      buf.append("(");
      buf.append(c);
      buf.append(")");
      buf.append(row_index);
      buf.append(" ");
    }
    buf.append(")");
    return new String(buf);
  }

}
