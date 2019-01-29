/**
 * com.mckoi.database.MasterTableGarbageCollector  28 Nov 2000
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

import com.mckoi.util.BlockIntegerList;
import com.mckoi.debug.*;
import java.io.IOException;

/**
 * A row garbage collector for a master table data source that manages
 * garbage collection over a MasterTableDataSource object.  Each time a row
 * is committed deleted from a master table, this object is notified.  When
 * the master table has no root locks on it, then the garbage collector
 * can kick in and mark all deleted rows as reclaimable.
 *
 * @author Tobias Downer
 */

final class MasterTableGarbageCollector {

  /**
   * The MasterTableDataSource that this collector is managing.
   */
  private MasterTableDataSource data_source;

  /**
   * If this is true, then a full sweep of the table is due to reclaim all
   * deleted rows from the table.
   */
  private boolean full_sweep_due;

  /**
   * The list of all rows from the master table that we have been notified
   * of being deleted.
   * <p>
   * NOTE: This list shouldn't get too large.  If it does, we should clear it
   *   and toggle the 'full_sweep_due' variable to true.
   */
  private BlockIntegerList deleted_rows;

  /**
   * The garbage collection event.
   */
  private Object collection_event;

  /**
   * Constructs the garbage collector.
   */
  MasterTableGarbageCollector(MasterTableDataSource data_source) {
    this.data_source = data_source;
    full_sweep_due = false;
    deleted_rows = new BlockIntegerList();

    collection_event =
                data_source.getSystem().createEvent(new CollectionEvent());
  }

  /**
   * Returns the DebugLogger object that we can use to log debug messages.
   */
  public final DebugLogger Debug() {
    return data_source.Debug();
  }

  /**
   * Called by the MasterTableDataSoruce to notify the collector that a row
   * has been marked as committed deleted.
   * <p>
   * SYNCHRONIZATION: We must be synchronized over 'data_source' when this
   *   is called.  (This is guarenteed if called from MasterTableDataSource).
   */
  void markRowAsDeleted(int row_index) {
    if (full_sweep_due == false) {
      boolean b = deleted_rows.uniqueInsertSort(row_index);
      if (b == false) {
        throw new Error("Row marked twice for deletion.");
      }
    }
  }

  /**
   * Called by the MasterTableDataSoruce to notify the collector to do a full
   * sweep and remove of records in the table at the next scheduled collection.
   * <p>
   * SYNCHRONIZATION: We must be synchronized over 'data_source' when this
   *   is called.  (This is guarenteed if called from MasterTableDataSource).
   */
  void markFullSweep() {
    full_sweep_due = true;
    if (deleted_rows.size() > 0) {
      deleted_rows = new BlockIntegerList();
    }
  }

  /**
   * Schedules a table clean up to occur some time in the future.  This
   * should be called by the MasterTableDataSource at various key check
   * points.  A good place to call this is when the last root lock is removed
   * from the MasterTableDataSource.
   * <p>
   * When the garbage collector kicks in, it does not remove rows from a
   * master table that is root locked.
   * <p>
   * This method returns immediately, the garbage collection actually happens
   * from the database dispatcher thread.
   */
  void scheduleCollection() {
    if (full_sweep_due || deleted_rows.size() > 0) {
      // Currently waits 300ms
      data_source.getSystem().postEvent(300, collection_event);
    }
  }



  // ---------- Inner classes ----------

  /**
   * The garbage collection event.  This is an event run from the database
   * dispatcher thread that performs the garbage collection of committed
   * deleted rows on the data source.  This can not delete rows from a table
   * that has its roots locked.
   */
  private class CollectionEvent implements Runnable {

    public void run() {

      try {
        int check_count = 0;
        int delete_count = 0;

        // Synchronize over the master data table source so no other threads
        // can interfere when we collect this information.
        synchronized (data_source) {

          if (data_source.isClosed()) {
            return;
          }

          // If root is locked, or has transaction changes pending, then we
          // can't delete any rows marked as deleted because they could be
          // referenced by transactions or result sets.
          if (!data_source.isRootLocked() &&
              !data_source.hasTransactionChangesPending()) {
            // Are we due a full sweep?
            if (full_sweep_due) {
              int raw_row_count = data_source.rawRowCount();
              for (int i = 0; i < raw_row_count; ++i) {
                // Synchronized in data_source.
                boolean b = data_source.hardCheckAndReclaimRow(i);
                if (b) {
                  ++delete_count;
                }
                ++check_count;
              }
              full_sweep_due = false;
            }
            else {
              // Are there any rows marked as deleted?
              int size = deleted_rows.size();
              if (size > 0) {
                // Go remove all rows marked as deleted.
                for (int i = 0; i < size; ++i) {
                  int row_index = deleted_rows.get(i);
                  // Synchronized in data_source.
                  data_source.hardRemoveRow(row_index);
                  ++delete_count;
                  ++check_count;
                }
              }
              deleted_rows = new BlockIntegerList();
            }

            if (check_count > 0) {
              if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                          "Row GC: [" + data_source.getName() +
                          "] check_count=" + check_count +
                          " delete count=" + delete_count);
                Debug().write(Lvl.INFORMATION, this,
                          "GC row sweep deleted " + delete_count + " rows.");
              }
//              System.out.println("Row GC: [" + data_source.getName() +
//                                 "] check_count=" + check_count +
//                                 " delete_count=" + delete_count);
            }

          } // if not roots locked and not transactions pending
        } // synchronized
      }
      catch (IOException e) {
        Debug().writeException(e);
      }

    }

  }

}
