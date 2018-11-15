package com.hortonworks.streamline.streams.layout.beam;

import org.apache.beam.sdk.transforms.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Created by Satendra Sahu on 11/14/18
 */
public class ClassIterator
{

   public static Class[] getClassesInPackage(String pckgname)
   {
	  File directory = getPackageDirectory(pckgname);
	  if (!directory.exists())
	  {
		 throw new IllegalArgumentException("Could not get directory resource for package " + pckgname + ".");
	  }

	  return getClassesInPackage(pckgname, directory);
   }

   private static Class[] getClassesInPackage(String pckgname, File directory)
   {
	  List<Class> classes = new ArrayList<Class>();
	  for (String filename : directory.list())
	  {
		 if (filename.endsWith(".class"))
		 {
			String classname = buildClassname(pckgname, filename);
			try
			{
			   classes.add(Class.forName(classname));
			}
			catch (ClassNotFoundException e)
			{
			   System.err.println("Error creating class " + classname);
			}
		 }
	  }
	  return classes.toArray(new Class[classes.size()]);
   }

   private static String buildClassname(String pckgname, String filename)
   {
	  return pckgname + '.' + filename.replace(".class", "");
   }

   private static File getPackageDirectory(String pckgname)
   {
	  ClassLoader cld = Thread.currentThread().getContextClassLoader();
	  if (cld == null)
	  {
		 throw new IllegalStateException("Can't get class loader.");
	  }

	  URL resource = cld.getResource(pckgname.replace('.', '/'));
	  if (resource == null)
	  {
		 throw new RuntimeException("Package " + pckgname + " not found on classpath.");
	  }

	  return new File(resource.getFile());
   }


}

