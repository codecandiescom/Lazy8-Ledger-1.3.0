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
package org.lazy8.nu.util.gen;

import java.awt.*;
import javax.swing.*;

/**
 *  Description of the Class
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class WorkingDialog extends JDialog {
  JProgressBar progressBar;
  AnimationThread thread;
  JLabel filler1,filler2;

  /**
   *  Constructor for the WorkingDialog object
   *
   *@param  frame  Description of the Parameter
   */
  public WorkingDialog(javax.swing.JFrame frame) {
    super(frame, Translator.getTranslation("Working..."), false);

    progressBar =
      new JProgressBar(JProgressBar.HORIZONTAL, 0, 100) {
        public Dimension getPreferredSize() {
          return new Dimension(300, super.getPreferredSize().height);
        }
      };

    getContentPane().setLayout(new GridLayout(3, 1));
    filler1=new JLabel(" ");
    filler2=new JLabel(" ");
    getContentPane().add(filler1);
    getContentPane().add(progressBar);
    getContentPane().add(filler2);

		thread=new AnimationThread();
    pack();
    setLocationRelativeTo(frame);
    setVisible(true);

  }

  /**
   *  Description of the Method
   *
   *@param  iNewProg  Description of the Parameter
   */
  public void SetProgress(int iNewProg) {
    progressBar.setValue(iNewProg);
    filler1.paintImmediately(0, 0, 10000, 10000);
    progressBar.paintImmediately(0, 0, 10000, 10000);
    filler2.paintImmediately(0, 0, 10000, 10000);
  }
		public void addNotify()
		{
			super.addNotify();
			thread.start();
		}

		public void removeNotify()
		{
			super.removeNotify();
			thread.stopNow=true;
		}

		class AnimationThread extends Thread
		{
      boolean stopNow;
			AnimationThread()
			{
				super("About box animation thread");
        stopNow=false;
				setPriority(Thread.MIN_PRIORITY);
			}

			public void run()
			{
				int whichMessage=0;
				final String[] theMessages={ Translator.getTranslation("Working..."),
																Translator.getTranslation("Please wait"),
																Translator.getTranslation("This may take some time")
				};
				for(;;)
				{
          if(stopNow) return;
					long start = System.currentTimeMillis();
					if(whichMessage >= theMessages.length)
						whichMessage=0;
					setTitle(theMessages[whichMessage]);
					whichMessage++;	
          if(stopNow) return;
					try
					{
						Thread.sleep(Math.max(0,3000 -
							(System.currentTimeMillis() - start)));
					}
					catch(InterruptedException ie)
					{
					}
          if(stopNow) return;
					WorkingDialog.this.repaint();
				}
			}
		}
}


