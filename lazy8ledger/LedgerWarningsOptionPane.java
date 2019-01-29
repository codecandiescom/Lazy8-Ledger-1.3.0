/*
 * MenuOptionPane.java - General options panel
 * Copyright (C) 1998, 1999, 2000, 2001 Slava Pestov
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package lazy8ledger;

import javax.swing.*;
import java.awt.event.*;
import java.awt.*;
import java.io.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.util.Log;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;

public class LedgerWarningsOptionPane extends AbstractOptionPane
{
  private JFrame view;
	public LedgerWarningsOptionPane(JFrame view )
	{
		super(Translator.getTranslation("Warnings"));
    this.view=view;
	}

	// protected members
	protected void _init()
	{
    jWarnings = new JCheckBox[setupWarnings.length];

    for (int i = 0; i < setupWarnings.length; i++)
      if (setupWarnings[i].compareTo("") != 0) {
        jWarnings[i] = new HelpedCheckBox(textWarnings[i],
            SetupInfo.getBoolProperty(setupWarnings[i]), textHelp[i], "setup",view);
        addComponent(jWarnings[i]);
      }
    //the first warning is ALLOW_TRANSACTION_CHANGES which may
    //never be changed, but it should be shown here anyway
    jWarnings[0].setEnabled(false);
    jWarnings[1].setEnabled(false);
	}

	protected void _save()
	{
    for (int i = 0; i < setupWarnings.length; i++)
        SetupInfo.setBoolProperty(setupWarnings[i], jWarnings[i].isSelected());
    SetupInfo.store();
	}

	// private members
  JCheckBox[] jWarnings;

  final String[] setupWarnings =
      {SetupInfo.ALLOW_TRANSACTION_CHANGES,
      SetupInfo.REQUIRE_LOGIN,
      SetupInfo.WARNING_GOOD_ADD, 
      SetupInfo.WARNING_BEFORE_DELETE,
      SetupInfo.INPUT_TABSTOP_CUSTOMER, SetupInfo.WARNING_AFTER_DELETE,
      SetupInfo.WARNING_CLEAR_AFTER_ADD,
      SetupInfo.WARNING_CLEAR_AFTER_DELETE,
      SetupInfo.SHOW_CONTEXTHELP,
      SetupInfo.WARN_TO_DO_BACKUP,
      SetupInfo.REQUIRE_BACKUP_PASSWORD,
      SetupInfo.SHOW_ADVANCED_MENUS};
  final String[] textWarnings =
      {Translator.getTranslation("Allow changes to transactions"),
      Translator.getTranslation("Require password at start"),
      Translator.getTranslation("Confirm after a record is added"),
      Translator.getTranslation("Confirm before deleting a record"),
      Translator.getTranslation("Tab stop on customer during input"),
      Translator.getTranslation("Confirm after deleting a record"),
      Translator.getTranslation("Clear screen after adding a record"),
      Translator.getTranslation("Clear screen after deleting a record"),
      Translator.getTranslation("Show context help"),
      Translator.getTranslation("Warn to backup data if changes made"),
      Translator.getTranslation("Require encrypted backup files"),
      Translator.getTranslation("Allow company changes in all windows")
      };
  final String[] textHelp =
      {"Allow-changes-to-transactions",
      "Require-password-at-start",
      "Confirm-after-a-record-is-added",
      "Confirm-before-deleting-a-record",
      "Tab-stop-on-customer-during-input",
      "Confirm-after-deleting-a-record",
      "Clear-screen-after-adding-a-record",
      "Clear-screen-after-deleting-a-record",
      "Show-context-help",
      "Warn-to-backup-data-if-changes-made",
      "Require-encrypted-backup-files",
      "Allow-company-changes-in-all-windows"
      };
}
