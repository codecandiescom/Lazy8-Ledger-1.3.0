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
 *
 */
package org.lazy8.nu.util.gen;

import java.io.*;
import javax.swing.filechooser.FileSystemView;
import java.util.*;
import java.net.*;
import org.gjt.sp.jedit.*;
import org.gjt.sp.util.*;
/**
 *  This class implements the standard "good programing" rule that all setup
 *  files edited by the application are put in a directory in the users home
 *  catalog called ".jEdit/Lazy8Ledger". However, all default setup files are in the
 *  applications installation jar directory in a jar file or in the ".jEdit/jar" in the
 *  users directory. So in a
 *  read only scenario, the default setup files can be gotten from that
 *  installation directory which can also be inside of the Lazy8.jar file. If a
 *  default file is found only in the installation directory, and it will be
 *  possibly saved, the file is copied over to the users home catalog under
 *  ".Lazy8Ledger" and then opened there. This class will only handle safely one
 *  directory level.
 *
 *@author     Lazy Eight Data HB, Thomas Dilts
 *@created    den 5 mars 2002
 */
public class Fileio {
  private final static String LAZY8DIR = "Lazy8Ledger";

  public static String getJarDir() {
    return "jeditresource:/" + LAZY8DIR + ".jar!"; //this might be in the installation directory!!!!
  }

  public static String getHomeDir() {
    String homeDir;
    if (jEdit.getSettingsDirectory() == null) 
      homeDir=MiscUtilities.constructPath(MiscUtilities.constructPath(
			System.getProperty("user.home"),".jedit"),LAZY8DIR);
    else
      homeDir=MiscUtilities.constructPath(
			jEdit.getSettingsDirectory(),LAZY8DIR);
    Log.log(Log.DEBUG,"Fileio","getHomeDir="+homeDir);
    File fil = new File(homeDir);
    if (!fil.isDirectory())
        fil.mkdirs();
    homeDir = fil.getAbsolutePath();
    Log.log(Log.DEBUG,"Fileio","getHomeDir2="+homeDir);
    return homeDir;
  }

  private static File getUserFile(String fileName, String fileDir) {
    File out;
    if (fileDir.length() == 0)
      out = new File(getHomeDir(), fileName);

    else {
      out = new File(getHomeDir(), fileDir);
      if (!out.isDirectory())
        out.mkdirs();
      out = new File(out, fileName);
    }
    Log.log(Log.DEBUG,"Fileio","getUserFile="+out);
    return out;
  }

  public static URL getURL(String fileName, String fileDir){
    //first look in the users directory without creating
    File foundFile = getUserFile(fileName, fileDir);
    try{
      if (foundFile.isFile()){
        Log.log(Log.DEBUG,"Fileio","getURL1="+foundFile.toURL());
        return foundFile.toURL();
      }
      String strJar=MiscUtilities.constructPath(MiscUtilities.constructPath(
        getJarDir(),fileDir),fileName);
      URL jardir=new URL(strJar);
      Log.log(Log.DEBUG,"Fileio","getURL2="+jardir);
      return jardir;
    }catch(MalformedURLException ee){
      Log.log(Log.DEBUG,"Fileio","getURL3 error="+ee);
    }
    return null;
   }
   public static InputStream getInputStream(String fileName, String fileDir)
      throws FileNotFoundException,MalformedURLException,IOException{
    //first look in the users directory without creating
    File foundFile = getUserFile(fileName, fileDir);
    if (foundFile.isFile())
      return new FileInputStream(foundFile);
    URL jardir=new URL(MiscUtilities.constructPath(MiscUtilities.constructPath(
			  getJarDir(),fileDir),fileName));
    Log.log(Log.DEBUG,"Fileio","getInputStream="+jardir);
    return jardir.openStream();
  }
   
   public static File getFile(String fileName, String fileDir, boolean isCreate, boolean isReadOnly)
    throws Exception {
    //first look in the users directory without creating
    File foundFile = getUserFile(fileName, fileDir);
    if (foundFile.isFile())
      return foundFile;
    try {
      if(!isReadOnly){
        InputStream in=getInputStream( fileName,  fileDir);
        if(in!=null){
          CopyFile(in, foundFile);
          Log.log(Log.DEBUG,"Fileio","getFile-created new file="+foundFile);
          return foundFile;
        }
      }
    }
    catch (Exception e) {
      Log.log(Log.DEBUG,"Fileio","getFile failed="+e);
      //foundFile = null;
    }
    if (foundFile != null && !foundFile.isFile() && isCreate) {
      foundFile = getUserFile(fileName, fileDir);
      foundFile.createNewFile();
    }
    Log.log(Log.DEBUG,"Fileio","getFile2="+foundFile);
    return foundFile;
  }

  /**
   *  Description of the Method
   *
   *@param  fromFile       Description of the Parameter
   *@param  toFile         Description of the Parameter
   *@exception  Exception  Description of the Exception
   */
  public static void CopyFile(InputStream in, File toFile)
    throws Exception {
    OutputStream out = new FileOutputStream(toFile);

    in = new BufferedInputStream(in);
    out = new BufferedOutputStream(out);

    byte[] buf = new byte[20000];
    int copied = 0;
    loop :
    for (; ; ) {
      int count = in.read(buf, 0, buf.length);
      if (count == -1)
        break loop;

      out.write(buf, 0, count);
    }
    in.close();
    out.close();
  }

  /**
   *  Gets the fileNames attribute of the Fileio class
   *
   *@param  directoryOfFiles  Description of the Parameter
   *@return                   The fileNames value
   */
  public static ArrayList getFileNames(String directoryOfFiles) {
    Log.log(Log.DEBUG,"Fileio","getFileNames="+directoryOfFiles);
    File dir;
    //first try to find the files in this jar file.  IF in this jar
    //file then there is a property called 
    //lazy8ledger.jarfiles.<dirname> <filenames>
    //where filenames is sepertated by spaces.
		StringTokenizer st = new StringTokenizer(
      jEdit.getProperty("lazy8ledger.jarfiles." + directoryOfFiles));
    ArrayList ResultList=new ArrayList();

		while(st.hasMoreTokens())
      ResultList.add(st.nextToken());

    dir = new File(getHomeDir(), directoryOfFiles);
    String[] files2 = dir.list();
    if (files2!=null){
      int j;
      if (files2 != null)
        for (int i = 0; i < files2.length; i++) {
          for (j = 0; j < ResultList.size(); j++)
            if (files2[i].compareTo(ResultList.get(j).toString()) == 0)
              break;
          if (j == ResultList.size())
            ResultList.add(files2[i]);
        }
    }

    return ResultList;
  }

}

