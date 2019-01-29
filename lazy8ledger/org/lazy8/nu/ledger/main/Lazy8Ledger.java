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
package org.lazy8.nu.ledger.main;

import javax.swing.*;
import java.io.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.net.URL;
import javax.swing.text.*;
import javax.swing.event.*;
import javax.swing.border.*;
import javax.swing.text.rtf.*;
import javax.swing.undo.*;
import javax.swing.plaf.*;
import org.lazy8.nu.ledger.forms.*;
import org.lazy8.nu.ledger.reports.*;
import org.lazy8.nu.ledger.jdbc.*;
import org.lazy8.nu.util.help.*;
import org.lazy8.nu.util.gen.*;
import org.gjt.sp.jedit.pluginmgr.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.util.*;
import org.gjt.sp.jedit.gui.DockableWindowManager;
import infoviewer.*;
/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class Lazy8Ledger {
  public static String getBuild() {
    String a;
    //this is put here just to get through the beautification
    // (major).(minor).(<99 = preX, 99 = final).(bug fix)
    return "04.00.00.00";
  }

  public static void ShowTutorial(org.gjt.sp.jedit.View view){
	  JDialog jd=new JDialog(view,Translator.getTranslation("Tutorial"),false);
	  InfoViewer iv=new InfoViewer(view,"");
	  String urlString=null;
	  int inset = 100;
	  try{
		  urlString= Fileio.getURL("tutorialmain." + 
			SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE) + ".html", "tutorial").toString();
		  iv.gotoURL(urlString);
		  jd.getContentPane().add(iv);
		  jd.pack();
		  Rectangle rv = new Rectangle();
		  if(view!=null){
		    view.getBounds(rv);
	            jd.setSize((int) (rv.width- inset), (int) (rv.height- inset));
		    jd.setLocationRelativeTo(view);
		  }else{
		    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		    jd.setBounds(inset / 2, inset / 2,
		    	screenSize.width - inset,
		    	screenSize.height - inset);
		  }
		  jd.setVisible(true);

		













	  }catch(Exception e){
		  Log.log(Log.ERROR,"ShowTutorial","could not find helpfile="
		  	+ urlString + " : error=" +e);
	  }
	  
  }
  public static void ShowHelp(JFrame viewChild, String filename ,String extention){
    if(filename!=null && filename.length()!=0 && viewChild!=null)
    {
      String urlString="";
      try{
        urlString= Fileio.getURL(filename+"." + 
          SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE) + ".html", "doc").toString();
        if(extention!=null && extention.length()!=0)
          urlString=urlString+"#"+extention;
        infoviewer.InfoViewerPlugin.openURL((org.gjt.sp.jedit.View)viewChild,urlString);
        //viewChild.requestFocus();
      }catch(Exception e){
          Log.log(Log.ERROR,"ShowContextHelp","could not find helpfile="
            + urlString + " : error=" +e);
      }
    }
  }
  public static void ShowContextHelp(JFrame viewChild, String filename ,String extention){
   if(filename!=null && filename.length()!=0 && SetupInfo.getBoolProperty(SetupInfo.SHOW_CONTEXTHELP)
       && viewChild!=null)
    {
      String urlString="";
      try{
        urlString= Fileio.getURL(filename+"." + 
          SetupInfo.getProperty(SetupInfo.PRESENT_LANGUAGE) + ".html", "doc").toString();
        if(extention!=null && extention.length()!=0)
          urlString=urlString+"#"+extention;
        DockableWindowManager mgr = ((org.gjt.sp.jedit.View)viewChild).getDockableWindowManager();
        //mgr.showDockableWindow("infoviewer");
        InfoViewer iv = (InfoViewer) mgr.getDockable("infoviewer");
        if(iv==null)return;
        iv.gotoURL(new URL(urlString), true);
      }catch(Exception e){
          Log.log(Log.ERROR,"ShowContextHelp","could not find helpfile="
            + urlString + " : error=" +e);
      }
    }
  }
  public Lazy8Ledger() {
  }

  /**
   *  Description of the Field
   */
  public final static String sCopyright =
      "Copyright (c) 2000 by Lazy Eight Data HB . "
       + "          GNU GENERAL PUBLIC LICENSE \n"
       + "   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION \n"
       + "\n"
       + "  0. This License applies to any program or other work which contains "
       + "a notice placed by the copyright holder saying it may be distributed "
       + "under the terms of this General Public License.  The 'Program', below, "
       + "refers to any such program or work, and a 'work based on the Program' "
       + "means either the Program or any derivative work under copyright law: "
       + "that is to say, a work containing the Program or a portion of it, "
       + "either verbatim or with modifications and/or translated into another "
       + "language.  (Hereinafter, translation is included without limitation in "
       + "the term 'modification'.)  Each licensee is addressed as 'you'. \n"
       + "\n"
       + "Activities other than copying, distribution and modification are not "
       + "covered by this License; they are outside its scope.  The act of "
       + "running the Program is not restricted, and the output from the Program "
       + "is covered only if its contents constitute a work based on the "
       + "Program (independent of having been made by running the Program). "
       + "Whether that is true depends on what the Program does. \n"
       + "\n"
       + "  1. You may copy and distribute verbatim copies of the Program's "
       + "source code as you receive it, in any medium, provided that you "
       + "conspicuously and appropriately publish on each copy an appropriate "
       + "copyright notice and disclaimer of warranty; keep intact all the "
       + "notices that refer to this License and to the absence of any warranty; "
       + "and give any other recipients of the Program a copy of this License "
       + "along with the Program. \n"
       + "\n"
       + "You may charge a fee for the physical act of transferring a copy, and "
       + "you may at your option offer warranty protection in exchange for a fee.\n "
       + "\n"
       + "  2. You may modify your copy or copies of the Program or any portion "
       + "of it, thus forming a work based on the Program, and copy and "
       + "distribute such modifications or work under the terms of Section 1 "
       + "above, provided that you also meet all of these conditions: \n"
       + "\n"
       + "    a) You must cause the modified files to carry prominent notices "
       + "    stating that you changed the files and the date of any change. \n"
       + "\n"
       + "    b) You must cause any work that you distribute or publish, that in "
       + "    whole or in part contains or is derived from the Program or any "
       + "    part thereof, to be licensed as a whole at no charge to all third "
       + "    parties under the terms of this License. \n"
       + "\n"
       + "    c) If the modified program normally reads commands interactively "
       + "    when run, you must cause it, when started running for such "
       + "    interactive use in the most ordinary way, to print or display an "
       + "    announcement including an appropriate copyright notice and a "
       + "    notice that there is no warranty (or else, saying that you provide "
       + "    a warranty) and that users may redistribute the program under "
       + "    these conditions, and telling the user how to view a copy of this "
       + "    License.  (Exception: if the Program itself is interactive but "
       + "    does not normally print such an announcement, your work based on "
       + "    the Program is not required to print an announcement.) \n"
       + "\n"
       + "These requirements apply to the modified work as a whole.  If "
       + "identifiable sections of that work are not derived from the Program, "
       + "and can be reasonably considered independent and separate works in "
       + "themselves, then this License, and its terms, do not apply to those "
       + "sections when you distribute them as separate works.  But when you "
       + "distribute the same sections as part of a whole which is a work based "
       + "on the Program, the distribution of the whole must be on the terms of "
       + "this License, whose permissions for other licensees extend to the "
       + "entire whole, and thus to each and every part regardless of who wrote it.\n "
       + "\n"
       + "Thus, it is not the intent of this section to claim rights or contest "
       + "your rights to work written entirely by you; rather, the intent is to "
       + "exercise the right to control the distribution of derivative or "
       + "collective works based on the Program. \n"
       + "\n"
       + "In addition, mere aggregation of another work not based on the Program "
       + "with the Program (or with a work based on the Program) on a volume of "
       + "a storage or distribution medium does not bring the other work under "
       + "the scope of this License. \n"
       + "\n"
       + "  3. You may copy and distribute the Program (or a work based on it, "
       + "under Section 2) in object code or executable form under the terms of "
       + "Sections 1 and 2 above provided that you also do one of the following: \n"
       + "\n"
       + "    a) Accompany it with the complete corresponding machine-readable "
       + "    source code, which must be distributed under the terms of Sections "
       + "    1 and 2 above on a medium customarily used for software interchange; or, \n"
       + "\n"
       + "    b) Accompany it with a written offer, valid for at least three "
       + "    years, to give any third party, for a charge no more than your "
       + "    cost of physically performing source distribution, a complete "
       + "    machine-readable copy of the corresponding source code, to be "
       + "    distributed under the terms of Sections 1 and 2 above on a medium "
       + "    customarily used for software interchange; or, \n"
       + "\n"
       + "    c) Accompany it with the information you received as to the offer "
       + "    to distribute corresponding source code.  (This alternative is "
       + "    allowed only for noncommercial distribution and only if you "
       + "    received the program in object code or executable form with such "
       + "    an offer, in accord with Subsection b above.) \n"
       + "\n"
       + "The source code for a work means the preferred form of the work for "
       + "making modifications to it.  For an executable work, complete source "
       + "code means all the source code for all modules it contains, plus any "
       + "associated interface definition files, plus the scripts used to "
       + "control compilation and installation of the executable.  However, as a "
       + "special exception, the source code distributed need not include "
       + "anything that is normally distributed (in either source or binary "
       + "form) with the major components (compiler, kernel, and so on) of the "
       + "operating system on which the executable runs, unless that component "
       + "itself accompanies the executable. \n"
       + "\n"
       + "If distribution of executable or object code is made by offering "
       + "access to copy from a designated place, then offering equivalent "
       + "access to copy the source code from the same place counts as "
       + "distribution of the source code, even though third parties are not "
       + "compelled to copy the source along with the object code. \n"
       + "\n"
       + "  4. You may not copy, modify, sublicense, or distribute the Program "
       + "except as expressly provided under this License.  Any attempt "
       + "otherwise to copy, modify, sublicense or distribute the Program is "
       + "void, and will automatically terminate your rights under this License. "
       + "However, parties who have received copies, or rights, from you under "
       + "this License will not have their licenses terminated so long as such "
       + "parties remain in full compliance. \n"
       + "\n"
       + "  5. You are not required to accept this License, since you have not "
       + "signed it.  However, nothing else grants you permission to modify or "
       + "distribute the Program or its derivative works.  These actions are "
       + "prohibited by law if you do not accept this License.  Therefore, by "
       + "modifying or distributing the Program (or any work based on the "
       + "Program), you indicate your acceptance of this License to do so, and "
       + "all its terms and conditions for copying, distributing or modifying "
       + "the Program or works based on it. \n"
       + "\n"
       + "  6. Each time you redistribute the Program (or any work based on the "
       + "Program), the recipient automatically receives a license from the "
       + "original licensor to copy, distribute or modify the Program subject to "
       + "these terms and conditions.  You may not impose any further "
       + "restrictions on the recipients' exercise of the rights granted herein. "
       + "You are not responsible for enforcing compliance by third parties to "
       + "this License. \n"
       + "\n"
       + "  7. If, as a consequence of a court judgment or allegation of patent "
       + "infringement or for any other reason (not limited to patent issues), "
       + "conditions are imposed on you (whether by court order, agreement or "
       + "otherwise) that contradict the conditions of this License, they do not "
       + "excuse you from the conditions of this License.  If you cannot "
       + "distribute so as to satisfy simultaneously your obligations under this "
       + "License and any other pertinent obligations, then as a consequence you "
       + "may not distribute the Program at all.  For example, if a patent "
       + "license would not permit royalty-free redistribution of the Program by "
       + "all those who receive copies directly or indirectly through you, then "
       + "the only way you could satisfy both it and this License would be to "
       + "refrain entirely from distribution of the Program. \n"
       + "\n"
       + "If any portion of this section is held invalid or unenforceable under "
       + "any particular circumstance, the balance of the section is intended to "
       + "apply and the section as a whole is intended to apply in other "
       + "circumstances. \n"
       + "\n"
       + "It is not the purpose of this section to induce you to infringe any "
       + "patents or other property right claims or to contest validity of any "
       + "such claims; this section has the sole purpose of protecting the "
       + "integrity of the free software distribution system, which is "
       + "implemented by public license practices.  Many people have made "
       + "generous contributions to the wide range of software distributed "
       + "through that system in reliance on consistent application of that "
       + "system; it is up to the author/donor to decide if he or she is willing "
       + "to distribute software through any other system and a licensee cannot "
       + "impose that choice. \n"
       + "\n"
       + "This section is intended to make thoroughly clear what is believed to "
       + "be a consequence of the rest of this License. \n"
       + "\n"
       + "  8. If the distribution and/or use of the Program is restricted in "
       + "certain countries either by patents or by copyrighted interfaces, the "
       + "original copyright holder who places the Program under this License "
       + "may add an explicit geographical distribution limitation excluding "
       + "those countries, so that distribution is permitted only in or among "
       + "countries not thus excluded.  In such case, this License incorporates "
       + "the limitation as if written in the body of this License. \n"
       + "\n"
       + "  9. The Free Software Foundation may publish revised and/or new versions "
       + "of the General Public License from time to time.  Such new versions will "
       + "be similar in spirit to the present version, but may differ in detail to "
       + "address new problems or concerns.\n "
       + "\n"
       + "Each version is given a distinguishing version number.  If the Program "
       + "specifies a version number of this License which applies to it and 'any "
       + "later version', you have the option of following the terms and conditions "
       + "either of that version or of any later version published by the Free "
       + "Software Foundation.  If the Program does not specify a version number of "
       + "this License, you may choose any version ever published by the Free Software "
       + "Foundation. \n"
       + "\n"
       + "  10. If you wish to incorporate parts of the Program into other free "
       + "programs whose distribution conditions are different, write to the author "
       + "to ask for permission.  For software which is copyrighted by the Free "
       + "Software Foundation, write to the Free Software Foundation; we sometimes "
       + "make exceptions for this.  Our decision will be guided by the two goals "
       + "of preserving the free status of all derivatives of our free software and "
       + "of promoting the sharing and reuse of software generally. \n"
       + "\n"
       + "       NO WARRANTY \n"
       + "\n"
       + "  11. BECAUSE THE PROGRAM IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY "
       + "FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW.  EXCEPT WHEN "
       + "OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES "
       + "PROVIDE THE PROGRAM 'AS IS' WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED "
       + "OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF "
       + "MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.  THE ENTIRE RISK AS "
       + "TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU.  SHOULD THE "
       + "PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING, "
       + "REPAIR OR CORRECTION.\n "
       + "\n"
       + "  12. IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING "
       + "WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR "
       + "REDISTRIBUTE THE PROGRAM AS PERMITTED ABOVE, BE LIABLE TO YOU FOR DAMAGES, "
       + "INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL OR CONSEQUENTIAL DAMAGES ARISING "
       + "OUT OF THE USE OR INABILITY TO USE THE PROGRAM (INCLUDING BUT NOT LIMITED "
       + "TO LOSS OF DATA OR DATA BEING RENDERED INACCURATE OR LOSSES SUSTAINED BY "
       + "YOU OR THIRD PARTIES OR A FAILURE OF THE PROGRAM TO OPERATE WITH ANY OTHER "
       + "PROGRAMS), EVEN IF SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE "
       + "POSSIBILITY OF SUCH DAMAGES. \n"
       + "\n"
       + "     END OF TERMS AND CONDITIONS ";

  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  public static class AcceptCopyrightDialog extends JDialog {
    /**
     *  Constructor for the AcceptCopyrightDialog object
     *
     *@param  frame  Description of the Parameter
     */
    Lazy8Ledger lzExit;

    /**
     *  Constructor for the AcceptCopyrightDialog object
     *
     *@param  frame  Description of the Parameter
     *@param  lz     Description of the Parameter
     */
    public AcceptCopyrightDialog(javax.swing.JFrame frame, Lazy8Ledger lz) {
      super(frame, "", true);
      lzExit = lz;

      //Quit this app when the big window closes if not from the "I accept button.
      addWindowListener(
        new WindowAdapter() {
          public void windowClosing(WindowEvent e) {
            //lzExit.exitingProgram();
          }
        }
          );

      getContentPane().setLayout(new BorderLayout());

      JTextPane m_monitor;
      StyleContext m_context;
      DefaultStyledDocument m_doc;
      RTFEditorKit m_kit;
      m_monitor = new JTextPane();
      m_kit = new RTFEditorKit();
      m_monitor.setEditorKit(m_kit);
      m_context = new StyleContext();
      m_doc = new DefaultStyledDocument(m_context);
      m_monitor.setDocument(m_doc);
      m_monitor.setEnabled(false);
      m_monitor.replaceSelection(sCopyright);

      JScrollPane ps = new JScrollPane(m_monitor);
      getContentPane().add(ps, BorderLayout.CENTER);
      m_monitor.setCaretPosition(0);
      //attempt to move the cursor up

      JPanel jpButtons = new JPanel();
      jpButtons.setLayout(new GridLayout(1, 2));
      getContentPane().add(jpButtons, BorderLayout.SOUTH);
      JButton buttonAccept = new JButton(Translator.getTranslation("I accept"));
      buttonAccept.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            SetupInfo.setBoolProperty(SetupInfo.ACCEPTED_COPYRIGHT, true);
            //save all information
            dispose();
          }
        }
          );
      JButton buttonCancel = new JButton(Translator.getTranslation("Cancel"));
      buttonCancel.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            dispose();
            //lzExit.exitingProgram();
          }
        }
          );
      jpButtons.add(buttonCancel);
      jpButtons.add(buttonAccept);
      pack();
      setLocationRelativeTo(frame);
      Rectangle rv = new Rectangle(400,600);
      //Lazy8Ledger.desktop.getBounds(rv);
      setSize((int) (0.80 * rv.width), (int) (0.80 * rv.height));
      setLocationRelativeTo(frame);
      setVisible(true);
    }
  }

  /**
   *  Description of the Class
   *
   *@author     Lazy Eight Data HB, Thomas Dilts
   *@created    den 5 mars 2002
   */
  public static class AboutDialog extends JDialog {
    /**
     *  Constructor for the AboutDialog object
     *
     *@param  frame  Description of the Parameter
     */
    public AboutDialog(javax.swing.JFrame frame) {
      super(frame, "", true);

      getContentPane().setLayout(new GridLayout(3, 1));

      JPanel jpCopyright = new JPanel();
      jpCopyright.setLayout(new GridLayout(1, 2));

      JPanel jp1 = new JPanel();
      jp1.setLayout(new GridLayout(4, 1));
      jpCopyright.add(jp1);
      jp1.add(new JLabel(
          Translator.getTranslation("Copyright information")));
      jp1.add(new JPanel());
      jp1.add(new JPanel());
      jp1.add(new JPanel());
      /*
       *  jp1.add(new JPanel());
       *  jp1.add(new JPanel());
       *  jp1.add(new JPanel());
       *  jp1.add(new JPanel());
       */
      JTextPane m_monitor;
      StyleContext m_context;
      DefaultStyledDocument m_doc;
      RTFEditorKit m_kit;
      m_monitor = new JTextPane();
      m_kit = new RTFEditorKit();
      m_monitor.setEditorKit(m_kit);
      m_context = new StyleContext();
      m_doc = new DefaultStyledDocument(m_context);
      m_monitor.setDocument(m_doc);
      m_monitor.setEnabled(false);
      m_monitor.replaceSelection(sCopyright);

      JScrollPane ps = new JScrollPane(m_monitor);
      jpCopyright.add(ps);

      Rectangle rv = new Rectangle(400,600);
      //Lazy8Ledger.desktop.getBounds(rv);
      m_monitor.setMaximumSize(new Dimension((int) (0.35 * rv.width), (int) (0.25 * rv.height)));
      m_monitor.setMinimumSize(new Dimension((int) (0.35 * rv.width), (int) (0.25 * rv.height)));
      m_monitor.setPreferredSize(new Dimension((int) (0.35 * rv.width), (int) (0.25 * rv.height)));
      ps.setMaximumSize(new Dimension((int) (0.35 * rv.width), (int) (0.25 * rv.height)));
      ps.setMinimumSize(new Dimension((int) (0.35 * rv.width), (int) (0.25 * rv.height)));
      ps.setPreferredSize(new Dimension((int) (0.35 * rv.width), (int) (0.25 * rv.height)));

      m_monitor.setCaretPosition(0);
      //attempt to move the cursor up
      JPanel jpCenterInformation = new JPanel();
      jpCenterInformation.setLayout(new GridLayout(6, 2));

      getContentPane().add(jpCopyright);
      getContentPane().add(jpCenterInformation);

      jpCenterInformation.add(new JLabel(
          Translator.getTranslation("Program name")));
      jpCenterInformation.add(new JLabel(
          Translator.getTranslation("Lazy 8 ledger")));
      jpCenterInformation.add(new JLabel(
          Translator.getTranslation("Program version")));
      jpCenterInformation.add(new JLabel(getBuild()));
      jpCenterInformation.add(new JLabel(
          Translator.getTranslation("Written by")));
      jpCenterInformation.add(new JLabel(
          "Lazy Eight Data HB"));
      jpCenterInformation.add(new JLabel());
      jpCenterInformation.add(new JLabel(
          "lazy8@telia.com"));
      jpCenterInformation.add(new JLabel(
          Translator.getTranslation("In association with")));
      jpCenterInformation.add(new JLabel(
          "Ceterus AB"));
      jpCenterInformation.add(new JLabel());
      jpCenterInformation.add(new JLabel(
          "info@ceterus.nu"));

      JPanel jpBottom = new JPanel();
      jpBottom.setLayout(new BorderLayout());

      JPanel jpIconPane = new JPanel();
      jpIconPane.setLayout(new GridLayout(1, 2));

      jpIconPane.add(new JLabel(new ImageIcon(getClass().
          getResource("/images/lazy8color.jpg"), "Lazy Eight Data HB")));
      jpIconPane.add(new JLabel(new ImageIcon(getClass().
          getResource("/images/ceterus.gif"), "Ceterus AB")));

      jpBottom.add(jpIconPane, BorderLayout.CENTER);

      JButton button3 = new JButton(Translator.getTranslation("OK"));
      button3.addActionListener(
        new java.awt.event.ActionListener() {
          public void actionPerformed(java.awt.event.ActionEvent evt) {
            buttonOK();
          }
        }
          );
      JPanel jp2 = new JPanel();
      jp2.add(button3);
      jpBottom.add(jp2, BorderLayout.SOUTH);
      getContentPane().add(jpBottom);

      pack();
      setLocationRelativeTo(frame);
      setVisible(true);
    }

    /**
     *  Description of the Method
     */
    public void buttonOK() {
      dispose();
    }
  }

  /**
   *  The main program for the Lazy8Ledger class
   *
   *@param  args  The command line arguments
   */
  public static void mainInit(String[] args) {
    String javaVersion = System.getProperty("java.version");
    if (javaVersion.compareTo("1.4") < 0) {
      System.err.println("You are running Java version "
           + javaVersion + ".");
      System.err.println("jEdit requires Java 1.3 or later.");
      System.exit(1);
    }
    Lazy8Ledger frame = new Lazy8Ledger();

    try {
      Locale lc = new Locale(System.getProperty("user.language"),
          System.getProperty("user.region"));
      Locale.setDefault(lc);
    }
    catch (Exception cccc) {
    }
    ;
    //check if this is the first time starting this program, if so, set the
    //copyright information and show the payment info if needed.
    AcceptCopyrightDialog cpDialog;
    if (!SetupInfo.getBoolProperty(SetupInfo.ACCEPTED_COPYRIGHT)) {
      cpDialog = new AcceptCopyrightDialog(null, null);
      //        if( ! SetupInfo.getBoolProperty(SetupInfo.TRIAL_PERIOD_OVER_WARNING_SHOWN))
      //          new ShowPaymentDialog(frame,false);
      //create a new company for this new user
      new CreateNewCompany(null ,false);
      //set the date that this program is first added.
      SetupInfo.setProperty(SetupInfo.TRIAL_PERIOD_START_TIME,
          (new Long((new java.util.Date()).getTime())).toString());
      SetupInfo.store();
      Locale[] locales = Locale.getAvailableLocales();
      if (locales.length < 5)
        JOptionPane.showMessageDialog(null,
            "You do not have the international version of java.\r\n" +
            "You will only be able to use the 'english' language.\r\n" +
            "If you want to use any other language, you must reinstall the \r\n" +
            "international version of java.",
            "WARNING",
            JOptionPane.PLAIN_MESSAGE);

    }
    //check if 30 days is over
    if (!SetupInfo.getBoolProperty(SetupInfo.TRIAL_PERIOD_OVER_WARNING_SHOWN)) {
      long iDateStartedProgram = 0;
      try {
        iDateStartedProgram = (new Long(SetupInfo.getProperty(
            SetupInfo.TRIAL_PERIOD_START_TIME))).longValue();
      }
      catch (Exception e) {
      }
      long iTodaysDate = (new java.util.Date()).getTime();
      //        if ((iTodaysDate-iDateStartedProgram)> 2592000)
      //          new ShowPaymentDialog(frame,true);
    }
    /*
     *  //check if this is a totally installation
     *  if (SetupInfo.getProperty(
     *  SetupInfo.LAZY8LEDGER_VERSION).compareTo("") == 0) {
     *  String aaa;
     *  String bbbb;
     *  /its a new installation, copy over all files.
     *  }
     */
    //try to read the connect information from file.  Else send up dialog

  }

  /**
   *  Description of the Method
   *
   *@param  filename     Description of the Parameter
   *@param  description  Description of the Parameter
   *@return              Description of the Return Value
   */
  public ImageIcon createImageIcon(String filename, String description) {
    String path = filename;
    return new ImageIcon(getClass().getResource(path), description);
  }
  public static int getWindowNumber(){
    return iWindowNumber++;
  }
  private static int iWindowNumber=1;
  /**
   *  Description of the Field
   */
  public static ImageIcon imageLazy8;
  /**
   *  Description of the Field
   */
  public static Lazy8Ledger frame;
  /**
   *  Description of the Field
   */
}

