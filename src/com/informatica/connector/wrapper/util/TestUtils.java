package com.informatica.connector.wrapper.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.informatica.cloud.api.adapter.common.INIFile;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;

public class TestUtils {
	
	
	public static List<String> getSourceRecords(IRegistrationInfo registrationInfo) throws IOException 
	{
		INIFile iniFile = getINIFile(registrationInfo);
	    List<String> recordNames = new ArrayList<String>();
	    String recordNameCsv = iniFile.getStringProperty("Record", "ReadNames");
	    if (recordNameCsv != null) {
	      recordNames = Arrays.asList(recordNameCsv.split(","));
	    }
	    return recordNames;
	}

	public static List<String> getTargetRecords(IRegistrationInfo registrationInfo) throws IOException {
		INIFile iniFile = getINIFile(registrationInfo);
	    List<String> recordNames = new ArrayList<String>();
	    String recordNameCsv = iniFile.getStringProperty("Record", "WriteNames");
	    if (recordNameCsv != null) {
	      recordNames = Arrays.asList(recordNameCsv.split(","));
	    }
	    return recordNames;
	}
	
	
	 public static Map<String, String> getConnectionAtributes(IRegistrationInfo registrationInfo) throws FileNotFoundException
	  {
		INIFile iniFile = getINIFile(registrationInfo);
	    String[] propertyNames = iniFile.getPropertyNames("Connection Parameters");
	    Map<String, String> connAttributes = new HashMap<String, String>();

	    for (int i = 0; i < propertyNames.length; i++) {
	      Object key = propertyNames[i];
	      String value = iniFile.getStringProperty("Connection Parameters", propertyNames[i]);
	      connAttributes.put(key.toString(), value);
	    }
	    return connAttributes;
	  }

	  public static INIFile getINIFile(IRegistrationInfo registrationInfo) throws FileNotFoundException
	  {
		String fileName =   "INIFiles" + File.separator + registrationInfo.getPluginUUID().toString() + ".ini";
		File fl = new File(fileName);
		if(fl.exists() && fl.canRead())
		{
			return  INIFile.getInstance("INIFiles" + File.separator + registrationInfo.getPluginUUID().toString() + ".ini");
		}		
		throw new FileNotFoundException(fileName);		
	  }
	
	public static void CopyFile(File srcFile, File destFile) {
		FileInputStream fis = null;
		FileOutputStream fos = null;
		FileChannel input = null;
		FileChannel output = null;
		try {
			fis = new FileInputStream(srcFile);
			fos = new FileOutputStream(destFile);
			input = fis.getChannel();
			output = fos.getChannel();
			long size = input.size();
			long pos = 0;
			long count = 0;
			while (pos < size) {
				count = size - pos > 1024 * 1024 ? 1024 * 1024 : size - pos;
				pos += output.transferFrom(input, pos, count);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(output!=null)
					output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fos!=null)
					fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(input!=null)
					input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fis!=null)
					fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
		
	
}
