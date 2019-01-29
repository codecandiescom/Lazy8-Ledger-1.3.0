/*
 *  Copyright (C) 2002 Lazy Eight Data HB, Thomas Dilts This program is free
 *  software; you can redistribute it and/or modify it under the terms of the
 *  GNU General Public License as published by the Free Software Foundation;
 *  either version 2 of the License, or (at your option) any later version. This
 *  program is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 *  details. You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For more
 *  information, surf to www.lazy8.nu or email lazy8@telia.com
 */
package org.lazy8.nu.ledger.forms;

import javax.swing.*;
import javax.swing.event.*;
import java.awt.*;
import java.awt.event.*;
import java.sql.*;
import java.io.*;
import javax.swing.table.*;
import java.beans.PropertyChangeListener;
import javax.swing.border.*;
import java.util.Calendar;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.jedit.gui.*;
import org.gjt.sp.util.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class TransactionTableForm extends DataExchangeForm implements ItemListener {
  private static long lastTimeHere;
  public TransactionTableForm(JFrame view) {
    super("transaction",view,"lazy8ledger-transaction");
    
    DataConnection dc=DataConnection.getInstance(view);
    if(dc==null || !dc.bIsConnectionMade){
      Exit();
      return ;
    }
    boolean bIsFastEntry=true;
    /*
    FocusTraversalPolicy policy =
      new FocusTraversalPolicy() {

        public Component getComponentAfter(Container focusCycleRoot,
            Component aComponent) {
            if (nextComp != null) {
            Component nxt = nextComp;
            nextComp = null;
            return nxt;
          }
          else
            return invoiceDateField;
        }

        public Component getComponentBefore(Container focusCycleRoot,
            Component aComponent) {
          return invoiceDateField;
        }

        public Component getFirstComponent(Container focusCycleRoot) {
          return invoiceDateField;
        }

        public Component getLastComponent(Container focusCycleRoot) {
          return invoiceDateField;
        }

        public Component getDefaultComponent(Container focusCycleRoot) {
          return invoiceDateField;
        }

        public Component getInitialComponent(Window window) {
          return invoiceDateField;
        }

      };
      

    setFocusTraversalPolicy(policy);
    */
    bIsFastEntry = false;
    //...Create the GUI and put it in the window...
    bFastEntry = bIsFastEntry;
    JPanel jPanel4 = new javax.swing.JPanel();

    label1 = new HelpedLabel(Translator.getTranslation("RegDate"), "RegDate", "transaction",view);
    labelTodaysDate = new HelpedLabel("", "RegDate", "transaction",view);
    label2 = new HelpedLabel(Translator.getTranslation("InvDate"), "InvDate", "transaction",view);
    invoiceDateField =
      new DateField("InvDate", "transaction",view) {
        public void gotEnter() {
          try {
            //invoice date
            invoiceDateField.getDate();
          }
          catch (Exception e) {
            JOptionPane.showMessageDialog(TransactionTableForm.this,
                Translator.getTranslation("Date must be in the following format") +
                " : " +
                DateField.getTodaysDateString(),
                Translator.getTranslation("Update not entered"),
                JOptionPane.PLAIN_MESSAGE);
            return;
          }
          try {
            if (invoiceDateField.getDate().compareTo((Date) cc.comboBoxPeriod.getSelectedItemsKey()) < 0 ||
                invoiceDateField.getDate().compareTo((Date) cc.comboBoxPeriod.getSelectedItemsSecondaryKey()) > 0) {
              JOptionPane.showMessageDialog(TransactionTableForm.this,
                  Translator.getTranslation("Date is not within the specified time period.") +
                  "  " +
                  (String) cc.comboBoxPeriod.getSelectedItem(),
                  Translator.getTranslation("Update not entered"),
                  JOptionPane.PLAIN_MESSAGE);
              return;
            }
          }
          catch (Exception e) {
          }
          commentsTextField.requestFocus();
        }
      };

    label3 = new HelpedLabel(Translator.getTranslation("Commentary"), "Commentary", "transaction",view);
    commentsTextField =
      new HelpedTextField("Commentary", "transaction",view) {
        public void gotEnter() {
          SetSelectionFirstNoEdit();
          tableView.requestFocus();
          tableView.editCellAt(0, COL_ACCOUNTNUM);
        }
      };

    label4 = new HelpedLabel(Translator.getTranslation("Fileing information"), "FileInfo", "transaction",view);
    fileInfoTextField =
      new HelpedTextField("FileInfo", "transaction",view) {
        public void gotEnter() {
          invoiceDateField.requestFocus();
        }
      };

    label6 = new HelpedLabel(Translator.getTranslation("Act_id"), "ActId", "transaction",view);
    lableTransaction = new HelpedLabel("", "ActId", "transaction",view);

    JPanel jPanel6 = new javax.swing.JPanel();
    JPanel jPanel7 = new javax.swing.JPanel();

    transactionAccess = new TransactionAccess(view);

    setLayout(new BorderLayout());

    jPanel4.setLayout(new GridLayout(4, 4));

    cc = new CompanyComponents(jPanel4, Translator.getTranslation("Company"),
        false, "transaction",view);
    cc.AddPeriod(jPanel4, Translator.getTranslation("Period"), false);
    if (bIsFastEntry)
      cc.comboBox.setEnabled(false);
    cc.comboBox.addItemListener(this);
    //      jPanel4.add(new JLabel(""));
    //      jPanel4.add(new JLabel(""));

    jPanel4.add(label1);

    JPanel panelDate = new JPanel();
    panelDate.setLayout(new GridLayout(1, 1));
    labelTodaysDate.setEnabled(false);
    panelDate.setBorder(LineBorder.createGrayLineBorder());
    JPanel panelDate2 = new JPanel();
    panelDate2.setBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2));
    panelDate2.add(labelTodaysDate);
    panelDate2.setLayout(new GridLayout(1, 1));
    panelDate.add(panelDate2);
    jPanel4.add(panelDate);

    jPanel4.add(label4);
    jPanel4.add(fileInfoTextField);

    jPanel4.add(label2);
    jPanel4.add(invoiceDateField);

    jPanel4.add(new JLabel());
    jPanel4.add(new JLabel());
    /*
     *  label5 = new HelpedLabel(Translator.getTranslation("Customer"), "Customer", "Transaction",view);
     *  comboBox = new DataComboBox(new JdbcTable("Customer", 2), true, "Customer", "Transaction",view);
     *  if (bIsFastEntry)
     *  new IntegerPairedField(comboBox, jPanel4, "FastAccount", "Transaction",view);
     *  else
     *  jPanel4.add(comboBox);
     */
    jPanel4.add(label3);
    jPanel4.add(commentsTextField);

    jPanel4.add(label6);

    JPanel panelTransaction = new JPanel();
    panelTransaction.setLayout(new GridLayout(1, 1));
    lableTransaction.setEnabled(false);
    panelTransaction.setBorder(LineBorder.createGrayLineBorder());
    JPanel panelTransaction2 = new JPanel();
    panelTransaction2.setBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2));
    panelTransaction2.add(lableTransaction);
    panelTransaction2.setLayout(new GridLayout(1, 1));
    panelTransaction.add(panelTransaction2);
    jPanel4.add(panelTransaction);

    JPanel jPanel10 = new JPanel();

    dataMovementPane = new DataMovementPane(
        (JdbcTable) transactionAccess, this,view);
    jPanel10.setLayout(new BoxLayout(jPanel10, BoxLayout.X_AXIS));
    jPanel10.add(dataMovementPane);
    jPanel10.add(Box.createHorizontalGlue());

    JPanel JLabelTotal= new JPanel();
    JLabelTotal.setLayout(new BorderLayout());

    HelpedLabel hl=new HelpedLabel(Translator.getTranslation("Total"),"DebitTotal", "transaction",view);
    Dimension ddm2=dataMovementPane.getPreferredSize();
    Dimension ddm3=hl.getPreferredSize();
    ddm3.height=ddm2.height;
    JLabelTotal.setPreferredSize(ddm3);
    JLabelTotal.setMaximumSize(ddm3);
    JLabelTotal.add(hl,BorderLayout.NORTH);
    jPanel10.add(JLabelTotal);

    JPanel JPanelDebit= new JPanel();
    JPanelDebit.setLayout(new BorderLayout());
    debitTotal=new DoubleField("DebitTotal", "transaction",view);
    Dimension ddm=debitTotal.getPreferredSize();
    ddm2.width=accountTotalSize;
    ddm.width=accountTotalSize;
    JPanelDebit.setPreferredSize(ddm2);
    JPanelDebit.setMaximumSize(ddm2);
    debitTotal.setPreferredSize(ddm);
    debitTotal.setMaximumSize(ddm);
    debitTotal.setMinimumSize(ddm);
    JPanelDebit.add(debitTotal,BorderLayout.NORTH);
    jPanel10.add(JPanelDebit);
    JPanel JPanelCredit= new JPanel();
    JPanelCredit.setLayout(new BorderLayout());
    creditTotal=new DoubleField("CreditTotal", "transaction",view);
    creditTotal.setPreferredSize(ddm);
    JPanelCredit.setPreferredSize(ddm2);
    JPanelCredit.setMaximumSize(ddm2);
    creditTotal.setMaximumSize(ddm);
    creditTotal.setMinimumSize(ddm);
    JPanelCredit.add(creditTotal,BorderLayout.NORTH);
    jPanel10.add(JPanelCredit);
    
    add(jPanel10, BorderLayout.SOUTH);

    add(jPanel4, BorderLayout.NORTH);

    getTable();
    JScrollPane scrollpane = new JScrollPane(tableView);

    scrollpane.setBorder(new BevelBorder(BevelBorder.LOWERED));
    scrollpane.setPreferredSize(new Dimension(230, 360));
    scrollpane.setMaximumSize(new Dimension(230, 360));
    scrollpane.setMinimumSize(new Dimension(230, 360));

    add(scrollpane, BorderLayout.CENTER);

    slideoverColumnSizes();
//    pack();
/*
    int width;
    int height;
    Rectangle rvMain = desktop.getBounds();
    Rectangle rvThis = new Rectangle();
    getBounds(rvThis);
    if (rvThis.width > rvMain.width)
      width = rvMain.width;
    else
      width = rvThis.width;
    if (rvThis.height > rvMain.height)
      height = rvMain.height;
    else
      height = rvThis.height;
    setSize(width, height);
    changeColumnSizes(width);
    pack();
    setSize(width, height);
    setLocation(0, 0);
*/
  }

  /**
   *  Gets the table attribute of the TransactionTableForm object
   *
   *@param  helpViewer  Description of the Parameter
   */
  public void getTable() {
    // Create a model of the data.
    tableFields=new StringBuffer[TABLE_ROWS][COL_LAST];
    for (int i = 0; i < TABLE_ROWS; i++) {
      for (int j= 0; j < COL_LAST; j++) {
        tableFields[i][j]=new StringBuffer();
      }
    }
    dataModel =
      new AbstractTableModel() {
        // These methods always need to be implemented.
        public int getColumnCount() {
          return COL_LAST;
        }

        public int getRowCount() {
          return TABLE_ROWS;
        }

        public Object getValueAt(int row, int col) {
          switch (col) {
            case COL_ROWNUM:
              return IntegerField.ConvertIntToLocalizedString(new Integer(row+1));
            default:
              return tableFields[row][col].toString();
          }
        }

        // The default implementations of these methods in
        // AbstractTableModel would work, but we can refine them.
        public String getColumnName(int column) {
          switch (column) {
            case COL_ROWNUM:
              return "";
            case COL_ACCOUNTNUM:
              return Translator.getTranslation("Account number");
            case COL_ACCOUNTNAME:
              return Translator.getTranslation("Account name");
            case COL_TEXT:
              return Translator.getTranslation("Commentary");
            case COL_CUSTOMERNUM:
              return Translator.getTranslation("CustId");
            case COL_CUSTOMERNAME:
              return Translator.getTranslation("Customer name");
            case COL_DEBIT:
              return Translator.getTranslation("Debit");
            case COL_CREDIT:
              return Translator.getTranslation("Credit");
          }
          return "";
        }

        public Class getColumnClass(int c) {
          switch (c) {
            case COL_ACCOUNTNUM:
              return accountEditor.getClass();
            case COL_CUSTOMERNUM:
              return customerEditor.getClass();
            case COL_DEBIT:
              return creditEditor.getClass();
            case COL_CREDIT:
              return debitEditor.getClass();
            default:
              return getValueAt(0, c).getClass();
          }
        }

        public boolean isCellEditable(int row, int col) {
          return col!=COL_ROWNUM;
        }

        public void setValueAt(Object aValue, int row, int col) {
Log.log(Log.DEBUG,"setValueAt","setValueAt="
  + row + ":" + col+":" + aValue.toString());
          String tempValue;
          switch (col) {
            case COL_ROWNUM:
              break;
            case COL_ACCOUNTNUM:
              tempValue=IntegerField.ConvertIntToLocalizedString(IntegerField.ConvertLocalizedStringToInt(aValue.toString()));
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(tempValue);
              accountCombo.setSelectedItemFromKey(
                  IntegerField.ConvertLocalizedStringToInt(tempValue));
              tableFields[row][COL_ACCOUNTNAME].delete(0,tableFields[row][COL_ACCOUNTNAME].length()).append(
                accountCombo.getSelectedItemsSecondaryKey().toString());

              //repaints whole table
              tableView.setRowHeight(tableView.getRowHeight());
              break;
            case COL_ACCOUNTNAME:
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(
                accountCombo.getSelectedItemsSecondaryKey().toString());
                tableFields[row][COL_ACCOUNTNUM].delete(0,tableFields[row][COL_ACCOUNTNUM].length()).append(
                  IntegerField.ConvertIntToLocalizedString((Integer) accountCombo.getSelectedItemsKey()));
              //repaints whole table
              tableView.setRowHeight(tableView.getRowHeight());
              break;
            case COL_TEXT:
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(aValue.toString());
              break;
            case COL_CUSTOMERNUM:
              tempValue=IntegerField.ConvertIntToLocalizedString(IntegerField.ConvertLocalizedStringToInt(aValue.toString()));
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(tempValue);
              customerCombo.setSelectedItemFromKey(
                  IntegerField.ConvertLocalizedStringToInt(tempValue));
              if (((Integer) customerCombo.getSelectedItemsKey()).intValue() == 0)
                tableFields[row][COL_CUSTOMERNAME].delete(0,tableFields[row][COL_CUSTOMERNAME].length());
              else
                tableFields[row][COL_CUSTOMERNAME].delete(0,tableFields[row][COL_CUSTOMERNAME].length()).append(
                  customerCombo.getSelectedItemsSecondaryKey().toString());
              //repaints whole table
              tableView.setRowHeight(tableView.getRowHeight());
              break;
            case COL_CUSTOMERNAME:
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(
                customerCombo.getSelectedItemsSecondaryKey().toString());
              Integer IKey = (Integer) customerCombo.getSelectedItemsKey();
              if (IKey.intValue() != 0)
                tableFields[row][COL_CUSTOMERNUM].delete(0,tableFields[row][COL_CUSTOMERNUM].length()).append(
                  IntegerField.ConvertIntToLocalizedString(IKey));
              else {
                tableFields[row][col].delete(0,tableFields[row][col].length());
                tableFields[row][COL_CUSTOMERNUM].delete(0,tableFields[row][COL_CUSTOMERNUM].length());
              }
              //repaints whole table
              tableView.setRowHeight(tableView.getRowHeight());
              break;
            case COL_DEBIT:
              tempValue=DoubleField.ConvertDoubleToLocalizedString(DoubleField.ConvertLocalizedStringToDouble(aValue.toString()));
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(tempValue);
              tableFields[row][COL_CREDIT].delete(0,tableFields[row][COL_CREDIT].length());
              TotalDebitAndCredit();
              //repaints whole table
              tableView.setRowHeight(tableView.getRowHeight());
              break;
            case COL_CREDIT:
              tempValue=DoubleField.ConvertDoubleToLocalizedString(DoubleField.ConvertLocalizedStringToDouble(aValue.toString()));
              tableFields[row][col].delete(0,tableFields[row][col].length()).append(tempValue);
              tableFields[row][COL_DEBIT].delete(0,tableFields[row][COL_DEBIT].length());
              TotalDebitAndCredit();
              //repaints whole table
              tableView.setRowHeight(tableView.getRowHeight());
              break;
          }
        }
      };
    // Create the table
    tableView = new JTable(dataModel);
    /*the backspace must be eaten.  Otherwise it effects the buffer....*/
    KeyListener keylst =
      new KeyListener() {
        public void keyTyped(KeyEvent e) {
          if (e.getKeyCode() == KeyEvent.VK_BACK_SPACE)
            e.consume();
        }

        public void keyPressed(KeyEvent e) {
          if (e.getKeyCode() == KeyEvent.VK_BACK_SPACE)
            e.consume();
        }

        public void keyReleased(KeyEvent e) {
          if (e.getKeyCode() == KeyEvent.VK_BACK_SPACE)
            e.consume();
        }
      };
    tableView.addKeyListener(keylst);

    // Turn off auto-resizing so that we can set column sizes programmatically.
    // In this mode, all columns will get their preferred widths, as set blow.
    tableView.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    DefaultTableCellRenderer numberRenderer = new DefaultTableCellRenderer();
    if (SetupInfo.getBoolProperty(SetupInfo.DATAENTRY_JUSTIFY_LEFT))
      numberRenderer.setHorizontalAlignment(JLabel.LEFT);
    else
      numberRenderer.setHorizontalAlignment(JLabel.RIGHT);
    TableColumn column;
    column = tableView.getColumn(dataModel.getColumnName(COL_ROWNUM));
    column.setPreferredWidth(rownumSize);
    column.setCellRenderer(new JTableHeader().getDefaultRenderer());
    
    column = tableView.getColumn(dataModel.getColumnName(COL_TEXT));
    textCommentsEditor=new HelpedTextField("TextComments", "transaction",view);
    column.setCellEditor(new DefaultCellEditor(textCommentsEditor));
    column.setPreferredWidth(textCommentSize);


    column = tableView.getColumn(dataModel.getColumnName(COL_ACCOUNTNUM));
    accountEditor = new IntegerField("Account", "transaction",view);
    column.setCellEditor(new DefaultCellEditor(accountEditor));
    column.setPreferredWidth(accountIntFieldSize);
    column.setCellRenderer(numberRenderer);

    accountCombo = new DataComboBox(new JdbcTable("Account", 2,view), true, "Account", "transaction",view);
    accountCombo.loadComboBox("AccDesc", "Account", (Integer) cc.comboBox.getSelectedItemsKey());
    column = tableView.getColumn(dataModel.getColumnName(COL_ACCOUNTNAME));
    column.setCellEditor(new DefaultCellEditor(accountCombo));
    column.setPreferredWidth(accountNameFieldSize);

    column = tableView.getColumn(dataModel.getColumnName(COL_CUSTOMERNUM));
    customerEditor = new IntegerField("Customer", "transaction",view);
    column.setCellEditor(new DefaultCellEditor(customerEditor));
    column.setPreferredWidth(accountIntFieldSize);
    column.setCellRenderer(numberRenderer);

    customerCombo = new DataComboBox(new JdbcTable("Customer", 2,view), true, "Customer", "transaction",view);
    customerCombo.loadComboBox("CustName", "CustId", (Integer) cc.comboBox.getSelectedItemsKey());
    column = tableView.getColumn(dataModel.getColumnName(COL_CUSTOMERNAME));
    column.setPreferredWidth(accountNameFieldSize);
    column.setCellEditor(new DefaultCellEditor(customerCombo));
    
    column = tableView.getColumn(dataModel.getColumnName(COL_DEBIT));
    column.setPreferredWidth(accountTotalSize);
    debitEditor = new DoubleField("DebitAccountTotal", "transaction",view);
    column.setCellEditor(new DefaultCellEditor(debitEditor));
    column.setCellRenderer(numberRenderer);
    
    column = tableView.getColumn(dataModel.getColumnName(COL_CREDIT));
    column.setPreferredWidth(accountTotalSize);
    creditEditor = new DoubleField("CreditAccountTotal", "transaction",view);
    column.setCellEditor(new DefaultCellEditor(creditEditor));
    column.setCellRenderer(numberRenderer);

    tableView.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

    ListSelectionModel colSM =
        tableView.getColumnModel().getSelectionModel();
    colSM.addListSelectionListener(
      new ListSelectionListener() {
        public void valueChanged(ListSelectionEvent e) {
          //Ignore extra messages.
          //if(true)return;
          if (e.getValueIsAdjusting())
            return;
          if ((Calendar.getInstance().getTime().getTime() - lastTimeHere) < 100)
            return;
          lastTimeHere=Calendar.getInstance().getTime().getTime();
          ListSelectionModel lsm = (ListSelectionModel) e.getSource();
          if (!lsm.isSelectionEmpty())
            switch (lsm.getMinSelectionIndex()) {
              case COL_TEXT:
                textCommentsEditor.showHelp();
                break;
              case COL_ACCOUNTNUM:
                accountEditor.showHelp();
                break;
              case COL_ACCOUNTNAME:
                accountEditor.showHelp();
                break;
              case COL_CUSTOMERNUM:
                customerEditor.showHelp();
                break;
              case COL_CUSTOMERNAME:
                customerEditor.showHelp();
                break;
              case COL_DEBIT:
                debitEditor.showHelp();
                break;
              case COL_CREDIT:
                creditEditor.showHelp();
                break;
            }
        }
      });

    KeyStroke aKeyStroke = KeyStroke.getKeyStroke("ENTER");
    InputMap im = tableView.getInputMap(JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
    im.remove(aKeyStroke);
    im.getParent().remove(aKeyStroke);
    tableView.setInputMap(JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT, im);
    im = tableView.getInputMap(JComponent.WHEN_FOCUSED);
    im.remove(aKeyStroke);
    tableView.setInputMap(JComponent.WHEN_FOCUSED, im);
    im = tableView.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
    im.remove(aKeyStroke);
    tableView.setInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW, im);
    tableView.getInputMap(JComponent.WHEN_FOCUSED).put(aKeyStroke, "eatme");
    tableView.getInputMap(JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT).put(aKeyStroke, "eatme");
    Action anAction =
      new Action() {

        public Object getValue(String key) {
          return "";
        }

        public void putValue(String key, Object value) { }

        public void setEnabled(boolean b) { }

        public boolean isEnabled() {
          return true;
        }

        public void addPropertyChangeListener(PropertyChangeListener listener) { }

        public void removePropertyChangeListener(PropertyChangeListener listener) { }

        public void actionPerformed(ActionEvent e) {
          ListSelectionModel rsm = tableView.getSelectionModel();
          ListSelectionModel csm = tableView.getColumnModel().getSelectionModel();

          int selectedRow = rsm.getAnchorSelectionIndex();
          int selectedColumn = csm.getAnchorSelectionIndex();
Log.log(Log.DEBUG,"actionPerformed","actionPerformed="
  + selectedRow + ":" + selectedColumn+":");

          switch (selectedColumn) {
            case COL_ACCOUNTNUM:
              if (SetupInfo.getBoolProperty(SetupInfo.INPUT_TABSTOP_CUSTOMER)) {
                tableView.editCellAt(selectedRow, COL_CUSTOMERNUM);
                csm.setSelectionInterval(COL_CUSTOMERNUM,COL_CUSTOMERNUM);
              }
              else {
                tableView.editCellAt(selectedRow, COL_DEBIT);
                csm.setSelectionInterval(COL_DEBIT, COL_DEBIT);
              }
              if (debitTotal.getDouble().compareTo(
                  creditTotal.getDouble()) == 0 &&
                  creditTotal.getDouble().doubleValue() != 0 &&
                  tableFields[selectedRow][COL_ACCOUNTNUM].length() == 0) {

                tableView.editCellAt(selectedRow, COL_ACCOUNTNUM);
                csm.setSelectionInterval(COL_ACCOUNTNUM, COL_ACCOUNTNUM);
                nextComp = dataMovementPane.butAdd;
                dataMovementPane.butAdd.requestFocus();
                //tableView.transferFocus();//this doesnt do anything anymore
              }
              else
                  if (tableFields[selectedRow][COL_ACCOUNTNAME].length() == 0) {
                tableView.editCellAt(selectedRow, COL_ACCOUNTNAME);
                csm.setSelectionInterval(COL_ACCOUNTNAME, COL_ACCOUNTNAME);
                //create a model dialog to get a name to the new account number
                JDialog dialog=new JDialog((Frame)GUIUtilities.getView(tableView),true);
                AccountForm af=new AccountForm(true,tableFields[selectedRow][COL_ACCOUNTNUM].toString(),dialog,view);
                dialog.getContentPane().setLayout(new GridLayout(1,1));
                dialog.getContentPane().add(af);
                dialog.pack();
                dialog.setLocationRelativeTo(GUIUtilities.getView(tableView));
                //This next command will not return untill the dialog dissappears. 
                dialog.setVisible(true);
                if(af.isOk()){
                  accountCombo = new DataComboBox(new JdbcTable("Account", 2,view), true, "Account", "transaction",view);
                  accountCombo.loadComboBox("AccDesc", "Account", (Integer) cc.comboBox.getSelectedItemsKey());
                  TableColumn column = tableView.getColumn(dataModel.getColumnName(COL_ACCOUNTNAME));
                  column.setCellEditor(new DefaultCellEditor(accountCombo));
                  tableView.setValueAt(af.accountNum(),selectedRow,COL_ACCOUNTNUM);
                  if (SetupInfo.getBoolProperty(SetupInfo.INPUT_TABSTOP_CUSTOMER)) {
                    tableView.editCellAt(selectedRow, COL_CUSTOMERNUM);
                    csm.setSelectionInterval(COL_CUSTOMERNUM,COL_CUSTOMERNUM);
                  }
                  else {
                    tableView.editCellAt(selectedRow, COL_DEBIT);
                    csm.setSelectionInterval(COL_DEBIT, COL_DEBIT);
                  }
                }
                else{
                  accountCombo.showPopup();
                }
              }

              break;
            case COL_CUSTOMERNUM:
              tableView.editCellAt(selectedRow, COL_DEBIT);
              csm.setSelectionInterval(COL_DEBIT,COL_DEBIT);
              if (tableFields[selectedRow][COL_CUSTOMERNAME].length() == 0) {
                tableView.editCellAt(selectedRow, COL_CUSTOMERNAME);
                csm.setSelectionInterval(COL_CUSTOMERNAME, COL_CUSTOMERNAME);
                //create a model dialog to get a name to the new customer number
                JDialog dialog=new JDialog((Frame)GUIUtilities.getView(tableView),true);
                CustomerForm cf=new CustomerForm(true,tableFields[selectedRow][COL_CUSTOMERNUM].toString(),dialog,view);
                dialog.getContentPane().setLayout(new GridLayout(1,1));
                dialog.getContentPane().add(cf);
                dialog.pack();
                dialog.setLocationRelativeTo(GUIUtilities.getView(tableView));
                //This next command will not return untill the dialog dissappears. 
                dialog.setVisible(true);
                if(cf.isOk()){
                  customerCombo = new DataComboBox(new JdbcTable("Customer", 2,view), true, "Customer", "transaction",view);
                  customerCombo.loadComboBox("CustName", "CustId", (Integer) cc.comboBox.getSelectedItemsKey());
                  TableColumn column = tableView.getColumn(dataModel.getColumnName(COL_CUSTOMERNAME));
                  column.setCellEditor(new DefaultCellEditor(customerCombo));
                  tableView.setValueAt(cf.customerNum(),selectedRow,COL_CUSTOMERNUM);
                  tableView.editCellAt(selectedRow, COL_DEBIT);
                  csm.setSelectionInterval(COL_DEBIT,COL_DEBIT);
                }
                else{
                  customerCombo.showPopup();
                }
              }
              break;
            case COL_DEBIT:
            case COL_CREDIT:
              tableView.editCellAt(selectedRow + 1, COL_ACCOUNTNUM);
              csm.setSelectionInterval(COL_ACCOUNTNUM, COL_ACCOUNTNUM);
              rsm.setSelectionInterval(selectedRow + 1, selectedRow + 1);
              BalanceDebitCreditColumns(selectedRow);
              break;
          }
        }

      };

    tableView.getActionMap().put("eatme", anAction);
//    AccountColNames[TABLE_ROWS - 1] = Translator.getTranslation("Total");

  }

  private void slideoverColumnSizes() {
    int width1 = (int) customerCombo.getPreferredSize().getWidth();
    if (accountNameFieldSize > width1)
    {
      tableView.getColumn(dataModel.getColumnName(COL_CUSTOMERNAME)).
        setPreferredWidth(width1);
      int width2=(int) accountCombo.getPreferredSize().getWidth();
      if((2*accountNameFieldSize - width1)<width2)
        width2=2*accountNameFieldSize - width1;
      tableView.getColumn(dataModel.getColumnName(COL_ACCOUNTNAME)).
        setPreferredWidth(width2);
    }
  }
  /**
   *  Description of the Method
   *
   *@param  maxWidth  Description of the Parameter
   */
  private void changeColumnSizes(int maxWidth) {
    TableColumn column;
    int initialwidth=accountIntFieldSize*2+accountTotalSize*2;
    int width1 = (int) customerCombo.getPreferredSize().getWidth();
    if ((maxWidth - initialwidth) / 2 < width1)
      width1 = (maxWidth - initialwidth) / 2;
    column = tableView.getColumn(dataModel.getColumnName(3));
    column.setPreferredWidth(width1);

    int width = (int) accountCombo.getPreferredSize().getWidth();
    if ((maxWidth - initialwidth - width1) < width)
      width = (maxWidth - initialwidth - width1);
    column = tableView.getColumn(dataModel.getColumnName(1));
    column.setPreferredWidth(width);
  }

  /**
   *  Description of the Method
   *
   *@param  selectedRow  Description of the Parameter
   *@return              Description of the Return Value
   */
  private boolean BalanceDebitCreditColumns(int selectedRow) {
    if (tableFields[selectedRow][COL_CREDIT].length() == 0 &&
        tableFields[selectedRow][COL_DEBIT].length() == 0 &&
        debitTotal.getDouble().compareTo(
        creditTotal.getDouble()) != 0) {
      if (debitTotal.getDouble().compareTo(
          creditTotal.getDouble()) > 0)
        tableFields[selectedRow][COL_CREDIT].delete(0,tableFields[selectedRow][COL_CREDIT].length()).append(
          DoubleField.ConvertDoubleToLocalizedString(new
            Double(debitTotal.getDouble().doubleValue() -
            creditTotal.getDouble().doubleValue())));
      else
        tableFields[selectedRow][COL_DEBIT].delete(0,tableFields[selectedRow][COL_DEBIT].length()).append(
          DoubleField.ConvertDoubleToLocalizedString(new
            Double(creditTotal.getDouble().doubleValue() -
            debitTotal.getDouble().doubleValue())));
      TotalDebitAndCredit();
//      dataMovementPane.PutFocusOnButtonAdd();
      return true;
    }
    return false;
  }

  /**
   *  Description of the Method
   */
  private void TotalDebitAndCredit() {
    double debitSum = 0;
    double creditSum = 0;
    double debit = 0;
    double credit = 0;
    int numEmptyFields=0;
    for (int i = 0; i < TABLE_ROWS ; i++) {
      debit=DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_DEBIT].toString()).doubleValue();
      credit=DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_CREDIT].toString()).doubleValue();
      debitSum += debit;
      creditSum += credit;
      if(debitSum == 0 && creditSum == 0)numEmptyFields++;
      else numEmptyFields=0;
      if(numEmptyFields>15)break;
    }
    debitTotal.setDouble(new Double(debitSum));
    creditTotal.setDouble(new Double(creditSum));
  }

  /**
   *  Description of the Method
   *
   *@param  e  Description of the Parameter
   */
  public void itemStateChanged(ItemEvent e) {
    dataMovementPane.buttonClearActionPerformed(null);
    Integer i = (Integer) cc.comboBox.getSelectedItemsKey();
    accountCombo.loadComboBox("AccDesc", "Account", i);
    customerCombo.loadComboBox("CustName", "CustId", i);
  }

  /**
   *  Description of the Method
   *
   *@param  bEnabled  Description of the Parameter
   */
  public void ChangeButtonEnabled(boolean bEnabled) {
    if (bFastEntry)
      return;
    if (bEnabled)
      cc.comboBox.setEnabled(!bEnabled);
    else
      cc.comboBox.setEnabled(SetupInfo.getBoolProperty(
          SetupInfo.SHOW_ADVANCED_MENUS));
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsChangeOK() {
    if (!SetupInfo.getBoolProperty(SetupInfo.ALLOW_TRANSACTION_CHANGES)) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("You are not allowed to make changes"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    return IsWriteOK();
  }

  /**
   *  Description of the Method
   */
  public void AfterGoodWrite() {
    super.AfterGoodWrite();
    //skipNextEnter = true;
    //invoiceDateField.grabFocus();
    invoiceDateField.requestFocus();
  }

  /**
   *  Description of the Method
   *
   *@return    Description of the Return Value
   */
  public boolean IsWriteOK() {
    if (((Integer) cc.comboBox.getSelectedItemsKey()).intValue() == 0) {
      CannotUpdateMessage();
      return false;
    }
    try {
      //invoice date
      invoiceDateField.getDate();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Date must be in the following format") +
          " : " +
          DateField.getTodaysDateString(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    try {
      if (invoiceDateField.getDate().compareTo((Date) cc.comboBoxPeriod.getSelectedItemsKey()) < 0 ||
          invoiceDateField.getDate().compareTo((Date) cc.comboBoxPeriod.getSelectedItemsSecondaryKey()) > 0) {
        JOptionPane.showMessageDialog(this,
            Translator.getTranslation("Date is not within the specified time period.") +
            "  " +
            (String) cc.comboBoxPeriod.getSelectedItem(),
            Translator.getTranslation("Update not entered"),
            JOptionPane.PLAIN_MESSAGE);
        return false;
      }
    }
    catch (Exception e) {
    }
    boolean bFoundGoodRecord = false;
    for (int i = 0; i < TABLE_ROWS ; i++)
      //search for a sum with no account
      if ((DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_DEBIT].toString()).doubleValue() != 0 ||
          DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_CREDIT].toString()).doubleValue() != 0)) {
        bFoundGoodRecord = true;
        
        if (IntegerField.ConvertLocalizedStringToInt(tableFields[i][COL_ACCOUNTNUM].toString()).intValue() == 0
             || (DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_DEBIT].toString()).doubleValue() < 0 ||
            DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_CREDIT].toString()).doubleValue() < 0)) {
          JOptionPane.showMessageDialog(this,
              Translator.getTranslation("The debit credit information is not ok"),
              Translator.getTranslation("Update not entered"),
              JOptionPane.PLAIN_MESSAGE);
          return false;
        }
      }

    if (!bFoundGoodRecord) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("The debit credit information is not ok"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
      return false;
    }
    if (debitTotal.getDouble().compareTo(
        creditTotal.getDouble()) == 0 &&
        creditTotal.getDouble().doubleValue() != 0)
      return super.IsWriteOK();
    else {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("The debit credit information is not ok"),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);

      return false;
    }
  }

  /**
   *  Gets the TransactionId attribute of the TransactionTableForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getTransactionId(int i) {
    transactionAccess.setObject(
        IntegerField.ConvertLocalizedStringToInt(lableTransaction.getText()),
        "Act_id");
  }

  /**
   *  Gets the company attribute of the TransactionTableForm object
   *
   *@param  i  Description of the Parameter
   */
  private void getCompany(int i) {
    transactionAccess.transactionAccess.setObject(cc.comboBox.getSelectedItemsKey(),
        "CompId");
  }

  /**
   *  Gets the andAddAmounts attribute of the TransactionTableForm object
   *
   *@param  iActId  Description of the Parameter
   *@param  compId  Description of the Parameter
   */
  private void getAndAddAmounts(Integer iActId, Integer compId) {
    for (int i = 0; i < TABLE_ROWS ; i++)
      if (tableFields[i][COL_ACCOUNTNUM].length() != 0 &&
          (DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_DEBIT].toString()).doubleValue() != 0 ||
          DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_CREDIT].toString()).doubleValue() != 0)) {

        transactionAccess.amountAccess.setObject(iActId, "Act_id");
        //account number
        transactionAccess.amountAccess.setObject(IntegerField.ConvertLocalizedStringToInt(tableFields[i][COL_ACCOUNTNUM].toString()), "Account");
        transactionAccess.amountAccess.setObject(IntegerField.ConvertLocalizedStringToInt(tableFields[i][COL_CUSTOMERNUM].toString()), "Customer");
        transactionAccess.amountAccess.setObject(tableFields[i][COL_TEXT].toString(), "Notes");
        //company id
        transactionAccess.amountAccess.setObject(compId, "CompId");
        if (DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_DEBIT].toString()).doubleValue()!=0) {
          transactionAccess.amountAccess.setObject(DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_DEBIT].toString()), "Amount");
          transactionAccess.amountAccess.setObject(new Integer(1),
              "IsDebit");
        }
        else {
          transactionAccess.amountAccess.setObject(DoubleField.ConvertLocalizedStringToDouble(tableFields[i][COL_CREDIT].toString()), "Amount");
          transactionAccess.amountAccess.setObject(new Integer(0),
              "IsDebit");
        }
        transactionAccess.amountAccess.AddRecord();
      }

  }

  /**
   *  Gets the wholeList attribute of the TransactionTableForm object
   *
   *@param  ii       Description of the Parameter
   *@param  iWhyGet  Description of the Parameter
   */
  private void getWholeList(int ii, int iWhyGet) {
    Integer compId = (Integer) cc.comboBox.getSelectedItemsKey();
    Integer iActId = IntegerField.ConvertLocalizedStringToInt(lableTransaction.getText());
    if (iWhyGet == DataMovementPane.ADD) {
      UniqNumGenerator a = new UniqNumGenerator();
      iActId = new Integer(a.GetUniqueNumber("Act_Id", 1, 9999999, compId));
      getAndAddAmounts(iActId, compId);
      lableTransaction.setText(IntegerField.ConvertIntToLocalizedString(iActId));
    }

    transactionAccess.transactionAccess.setObject(iActId, "Act_id");

    transactionAccess.transactionAccess.setObject(compId, "CompId");
    try {
      //todays date
      transactionAccess.transactionAccess.setObject(
          new java.sql.Date(DateField.getTodaysDate().getTime()),
          "RegDate");

      //invoice date
      transactionAccess.transactionAccess.setObject(
          new java.sql.Date(invoiceDateField.getDate().getTime()),
          "InvDate");
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this,
          Translator.getTranslation("Date must be in the following format") +
          " : " + DateField.getTodaysDateString(),
          Translator.getTranslation("Update not entered"),
          JOptionPane.PLAIN_MESSAGE);
    }

    transactionAccess.transactionAccess.setObject(
        commentsTextField.getText(), "Notes");
    transactionAccess.transactionAccess.setObject(
        fileInfoTextField.getText(), "FileInfo");
  }

  /**
   *  Gets the fields attribute of the TransactionTableForm object
   *
   *@param  WhyGet  Description of the Parameter
   */
  public void getFields(int WhyGet) {
    SetSelectionFirstNoEdit();
    switch (WhyGet) {
      case DataMovementPane.FIRST:
        getCompany(1);
        break;
      case DataMovementPane.CHANGE:
        //delete the old amounts
        transactionAccess.amountAccess.setObject(IntegerField.ConvertLocalizedStringToInt(lableTransaction.getText()),
            "Act_id");
        transactionAccess.amountAccess.setObject(cc.comboBox.getSelectedItemsKey(),
            "CompId");
        transactionAccess.amountAccess.DeleteRecord();
        //change the Transaction
        getWholeList(1, WhyGet);
        //readd the amounts
        getAndAddAmounts(IntegerField.ConvertLocalizedStringToInt(lableTransaction.getText()),
            (Integer) cc.comboBox.getSelectedItemsKey());
        break;
      case DataMovementPane.ADD:
        getWholeList(1, WhyGet);
        break;
      case DataMovementPane.DELETE:
        getTransactionId(1);
        getCompany(2);
        break;
    }

  }

  /**
   *  Description of the Method
   */
  private void SetSelectionFirstNoEdit() {
    ListSelectionModel rsm = tableView.getSelectionModel();
    ListSelectionModel csm = tableView.getColumnModel().getSelectionModel();
    tableView.editingCanceled(null);
    csm.setSelectionInterval(COL_ACCOUNTNUM, COL_ACCOUNTNUM);
    rsm.setSelectionInterval(0, 0);
  }

  /**
   *  Description of the Method
   */
  public void putFields() {
    SetSelectionFirstNoEdit();
    lableTransaction.setText(IntegerField.ConvertIntToLocalizedString((Integer)
        transactionAccess.transactionAccess.getObject("Act_id", null)));
    Integer key = (Integer)
        transactionAccess.transactionAccess.getObject("CompId", null);
    //    if(key.intValue()==0)
    cc.comboBox.setSelectedItemFromKey(key);
    cc.updatePeriod();
    labelTodaysDate.setText(DateField.ConvertDateToLocalizedString((Date)
        transactionAccess.transactionAccess.getObject("RegDate", null)));
    invoiceDateField.setDate((Date)
        transactionAccess.transactionAccess.getObject("InvDate", null));
    commentsTextField.setText((String)
        transactionAccess.transactionAccess.getObject("Notes", null));
    fileInfoTextField.setText((String)
        transactionAccess.transactionAccess.getObject("FileInfo", null));
//    comboBox.setSelectedItemFromKey((Integer)
//        TransactionAccess.TransactionAccess.getObject("Customer", null));
    clearTable();
    int iRow = 0;
    do {
      
      tableFields[iRow][COL_ACCOUNTNUM].append(IntegerField.ConvertIntToLocalizedString((Integer)
          transactionAccess.amountAccess.getObject("Account", null)));
      accountCombo.setSelectedItemFromKey((Integer)
          transactionAccess.amountAccess.getObject("Account", null));
      tableFields[iRow][COL_ACCOUNTNAME].append(accountCombo.getSelectedItemsSecondaryKey().toString());

      tableFields[iRow][COL_TEXT].append((String)
        transactionAccess.amountAccess.getObject("Notes", null));

      tableFields[iRow][COL_CUSTOMERNUM].append(IntegerField.ConvertIntToLocalizedString((Integer)
          transactionAccess.amountAccess.getObject("Customer", null)));
      if (tableFields[iRow][COL_CUSTOMERNUM].length() != 0) {
        customerCombo.setSelectedItemFromKey((Integer)
          transactionAccess.amountAccess.getObject("Customer", null));
        tableFields[iRow][COL_CUSTOMERNAME].append(customerCombo.getSelectedItemsSecondaryKey().toString());
      }
      if (((Integer)
          transactionAccess.amountAccess.getObject("IsDebit", null)).intValue() == 1)
        tableFields[iRow][COL_DEBIT].append(DoubleField.ConvertDoubleToLocalizedString((Double)
            transactionAccess.amountAccess.getObject("Amount", null)));

      else
        tableFields[iRow][COL_CREDIT].append(DoubleField.ConvertDoubleToLocalizedString((Double)
            transactionAccess.amountAccess.getObject("Amount", null)));
      iRow++;
    } while (transactionAccess.amountAccess.GetNextRecord())
        ;
    TotalDebitAndCredit();
    //repaints whole table
    tableView.setRowHeight(tableView.getRowHeight());
  }

  /**
   *  Description of the Method
   */
  public void clearTable() {
    for (int i = 0; i < TABLE_ROWS; i++) {
      for (int j= 0; j < COL_LAST; j++) {
        if(tableFields[i][j].length()!=0)
        tableFields[i][j].delete(0,tableFields[i][j].length());
      }
    }
  }
  /**
   *  Description of the Method
   */
  public void clearFields() {
    labelTodaysDate.setText("");
    invoiceDateField.setText("");
    commentsTextField.setText("");
    fileInfoTextField.setText("");
    lableTransaction.setText("");
    clearTable();
    TotalDebitAndCredit();
    SetSelectionFirstNoEdit();
    //repaints whole table
    tableView.setRowHeight(tableView.getRowHeight());
  }

  /**
   *  Description of the Field
   */
  protected JLabel label1;
  /**
   *  Description of the Field
   */
  protected JLabel labelTodaysDate;
  /**
   *  Description of the Field
   */
  protected JLabel label2;
  /**
   *  Description of the Field
   */
  protected DateField invoiceDateField;
  /**
   *  Description of the Field
   */
  protected JLabel label3;
  /**
   *  Description of the Field
   */
  protected JTextField commentsTextField;
  /**
   *  Description of the Field
   */
  protected JLabel label4;
  /**
   *  Description of the Field
   */
  protected JTextField fileInfoTextField;
  /**
   *  Description of the Field
   */
  protected JLabel label5;
  /**
   *  Description of the Field
   */
  protected JLabel label6;
  /**
   *  Description of the Field
   */
  protected JLabel lableTransaction;
  /**
   *  Description of the Field
   */
  protected TransactionAccess transactionAccess;
  /**
   *  Description of the Field
   */
  protected CompanyComponents cc;
  /**
   *  Description of the Field
   */
  protected boolean bFastEntry;
  private final int TABLE_ROWS = 300;
  private StringBuffer[][] tableFields;
  private JTable tableView;
  private IntegerField accountEditor;
  private HelpedTextField textCommentsEditor;
  private IntegerField customerEditor;
  private DataComboBox accountCombo;
  private DataComboBox customerCombo;
  private DoubleField creditEditor;
  private DoubleField debitEditor;
  private JComponent nextComp;
  private TableModel dataModel;
  private DoubleField creditTotal;
  private DoubleField debitTotal;
  private static final int accountIntFieldSize=40;
  private static final int accountNameFieldSize=170;
  private static final int accountTotalSize=90;
  private static final int rownumSize=30;
  private static final int textCommentSize=90;
  private static final int COL_ROWNUM=0;
  private static final int COL_ACCOUNTNUM=1;
  private static final int COL_ACCOUNTNAME=2;
  private static final int COL_TEXT=3;
  private static final int COL_CUSTOMERNUM=4;
  private static final int COL_CUSTOMERNAME=5;
  private static final int COL_DEBIT=6;
  private static final int COL_CREDIT=7;
  private static final int COL_LAST=8;
  
}


