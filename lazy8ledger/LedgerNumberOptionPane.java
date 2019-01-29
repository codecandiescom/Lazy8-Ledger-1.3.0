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

public class LedgerNumberOptionPane extends AbstractOptionPane
{
  private JFrame view;
	public LedgerNumberOptionPane(JFrame view)
	{
		super(Translator.getTranslation("Number formats"));
    this.view=view;
	}

  private class ChooseNumberFormatsPanel extends JPanel {
    private JRadioButton numStandardButton, currancyStandButton, numSpecialButton;
    private JRadioButton leftJustifyButton, RightJustifyButton;
    private JComboBox specialFormat;
    private String JustifySetup, typeFormatSetup, specialFormatSetup;
    private final static String NUMBER_STANDARD = "OS System standard number";
    private final static String CURENCY_STANDARD = "OS System standard currency";
    private final static String SPECIAL_FORMAT = "Special format";
    private final static String RIGHT_JUSTIFY = "Right justify";
    private final static String LEFT_JUSTIFY = "Left justify";

    public ChooseNumberFormatsPanel(boolean showCurrency, String FieldPrefix,
        String JustifySetupDef,
        String typeFormatSetupDef, String specialFormatSetupDef) {
      String[] patternExamples = {
          "#,##0.00",
          "#,##0.00¤;#,##0.00¤",
          "#,##0.00;(#,##0.00)",
          "#,##0.0",
          "#,##0.0¤;#,##0.0¤",
          "#,##0.0;(#,##0.0)",
          "#,##0",
          "#,##0¤;#,##0¤",
          "#,##0;(#,##0)",
          };
      setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
      JustifySetup = JustifySetupDef;
      typeFormatSetup = typeFormatSetupDef;
      specialFormatSetup = specialFormatSetupDef;
      numStandardButton = new HelpedRadioButton(Translator.getTranslation("OS System standard number"),
          FieldPrefix + "Standard", "setup",view);
      currancyStandButton = new HelpedRadioButton(Translator.getTranslation("OS System standard currency"),
          FieldPrefix + "Currency", "setup",view);
      numSpecialButton = new HelpedRadioButton(Translator.getTranslation("Special format"),
          FieldPrefix + "SpecialButton", "setup",view);
      leftJustifyButton = new HelpedRadioButton(Translator.getTranslation("Left justify"),
          FieldPrefix + "LeftJustify", "setup",view);
      RightJustifyButton = new HelpedRadioButton(Translator.getTranslation("Right justify"),
          FieldPrefix + "RightJustify", "setup",view);
      numStandardButton.setActionCommand(NUMBER_STANDARD);
      currancyStandButton.setActionCommand(CURENCY_STANDARD);
      numSpecialButton.setActionCommand(SPECIAL_FORMAT);
      leftJustifyButton.setActionCommand(LEFT_JUSTIFY);
      RightJustifyButton.setActionCommand(RIGHT_JUSTIFY);

      specialFormat = new HelpedComboBox(patternExamples,
          FieldPrefix + "Patterns", "setup",view);
      specialFormat.setEditable(true);

      JPanel jp5 = new JPanel();
      add(jp5);
      jp5.setLayout(new GridLayout(2, 1));
      jp5.setBorder(new javax.swing.border.TitledBorder(
          Translator.getTranslation("Number justification")));

      // Group the radio buttons.
      ButtonGroup group2 = new ButtonGroup();
      group2.add(leftJustifyButton);
      group2.add(RightJustifyButton);

      // Register a listener for the radio buttons.
      RadioFormatListener myListener2 = new RadioFormatListener();
      leftJustifyButton.addActionListener(myListener2);
      RightJustifyButton.addActionListener(myListener2);

      jp5.add(leftJustifyButton);
      jp5.add(RightJustifyButton);

      JPanel jp4 = new JPanel();
      add(jp4);
      jp4.setLayout(new GridLayout(3, 1));
      jp4.setBorder(new javax.swing.border.TitledBorder(
          Translator.getTranslation("Number format type")));

      // Group the radio buttons.
      ButtonGroup group1 = new ButtonGroup();
      group1.add(numStandardButton);
      if (showCurrency)
        group1.add(currancyStandButton);
      group1.add(numSpecialButton);

      // Register a listener for the radio buttons.
      RadioFormatListener myListener1 = new RadioFormatListener();
      numStandardButton.addActionListener(myListener1);
      if (showCurrency)
        currancyStandButton.addActionListener(myListener1);
      numSpecialButton.addActionListener(myListener1);

      jp4.add(numStandardButton);
      if (showCurrency)
        jp4.add(currancyStandButton);
      JPanel jp6 = new JPanel();
      jp6.setLayout(new BorderLayout());
      jp6.add(numSpecialButton, BorderLayout.WEST);
      jp6.add(specialFormat, BorderLayout.CENTER);
      jp4.add(jp6);

      //set initial values
      specialFormat.setVisible(false);
      if (SetupInfo.getBoolProperty(JustifySetup))
        leftJustifyButton.setSelected(true);
      else
        RightJustifyButton.setSelected(true);
      specialFormat.setSelectedItem(SetupInfo.getProperty(specialFormatSetup));
      if (SetupInfo.getProperty(typeFormatSetup).compareTo("1") == 0)
        numStandardButton.setSelected(true);
      else if (SetupInfo.getProperty(typeFormatSetup).compareTo("2") == 0
           && showCurrency)
        currancyStandButton.setSelected(true);
      else if (SetupInfo.getProperty(typeFormatSetup).compareTo("3") == 0) {
        specialFormat.setVisible(true);
        numSpecialButton.setSelected(true);
      }
    }

    /**
     *  Description of the Class
     *
     *@author     Administrator
     *@created    den 9 mars 2002
     */
    class RadioFormatListener implements ActionListener {
      /**
       *  Description of the Method
       *
       *@param  e  Description of the Parameter
       */
      public void actionPerformed(ActionEvent e) {
        String lnfName = e.getActionCommand();
        if (lnfName.compareTo(SPECIAL_FORMAT) == 0) {
          specialFormat.setVisible(true);
          validate();
        }
        else if (lnfName.compareTo(CURENCY_STANDARD) == 0 ||
            lnfName.compareTo(NUMBER_STANDARD) == 0) {
          specialFormat.setVisible(false);
          validate();
        }
      }
    }
    public void SaveValues() {

      SetupInfo.setBoolProperty(JustifySetup,
          leftJustifyButton.isSelected());
      if (specialFormat.getSelectedItem() != null)
        SetupInfo.setProperty(specialFormatSetup,
            (String) (specialFormat.getSelectedItem()));
      else
        SetupInfo.setProperty(specialFormatSetup, "");

      if (numStandardButton.isSelected())
        SetupInfo.setProperty(typeFormatSetup, "1");
      else if (currancyStandButton.isSelected())
        SetupInfo.setProperty(typeFormatSetup, "2");
      else if (numSpecialButton.isSelected())
        SetupInfo.setProperty(typeFormatSetup, "3");
    }

  }
	// protected members
	protected void _init()
	{
    reportNumbers = new ChooseNumberFormatsPanel(true, "Report",
        SetupInfo.REPORT_NUMBER_JUSTIFY_LEFT,
        SetupInfo.REPORT_NUMBER_FORMAT,
        SetupInfo.REPORT_NUMBER_SPECIALFORMAT);
    inputNumbers = new ChooseNumberFormatsPanel(false, "Input",
        SetupInfo.DATAENTRY_JUSTIFY_LEFT,
        SetupInfo.DATAENTRY_NUMBER_FORMAT,
        SetupInfo.DATAENTRY_NUMBER_SPECIALFORMAT);
    JPanel jCenteredPanel = new JPanel();
    jCenteredPanel.add(reportNumbers);
    addComponent(jCenteredPanel);
    jCenteredPanel.setBorder(new javax.swing.border.TitledBorder(
          Translator.getTranslation("Report number formats")));
    jCenteredPanel = new JPanel();
    jCenteredPanel.add(inputNumbers);
    addComponent(jCenteredPanel);
    jCenteredPanel.setBorder(new javax.swing.border.TitledBorder(
          Translator.getTranslation("Input number formats")));
	}

	protected void _save()
	{
    reportNumbers.SaveValues();
    inputNumbers.SaveValues();
    SetupInfo.store();
	}

	// private members
  ChooseNumberFormatsPanel reportNumbers;
  ChooseNumberFormatsPanel inputNumbers;
}
