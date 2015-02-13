package com.informatica.connector.wrapper.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class InputDataBufferImpl implements IInputDataBuffer {
	private List<String> rowList;
	private int curRowIndex = -1;
	private List<Field> fieldList;
	private String fileName = null;
	//private static final Log log = LogFactory.getLog(InputDataBufferImpl.class);

	public InputDataBufferImpl(String fileName, List<Field> fldList) {
		this.rowList = new ArrayList<String>();
		this.fieldList = fldList;
		this.fileName = fileName;

		if(fileName==null || fileName.isEmpty())		
			throw new IllegalArgumentException("Input Argument {fileName} cannot be null");

		if(fldList==null || fldList.isEmpty())		
			throw new IllegalArgumentException("Input Argument {fldList} cannot be null");
		
		try {
			fileName = "CSV" + File.separator + fileName;
			BufferedReader csvFile = new BufferedReader(
					new FileReader(fileName));
			String line = csvFile.readLine();			
			while (line != null) {
				if(!line.startsWith(fldList.get(0).getDisplayName())) //Skip header line
					this.rowList.add(line);
				line = csvFile.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			//log.error(e.toString());
			throw new IllegalArgumentException(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			//log.error(e.toString());
			throw new IllegalArgumentException(e.toString());
		}
	}

	private Object getObjectFromString(String val, JavaDataType jdt) {


		if (val == null || val.equalsIgnoreCase("null"))
			return null;
		
		val = val.trim();

		if (val.isEmpty()) {
			if (jdt == JavaDataType.JAVA_STRING)
				return val;
			else
				return null;
		}
		
		switch (jdt) {
		case JAVA_INTEGER:
			return new BigDecimal(val.toString()).intValue();
		case JAVA_BOOLEAN:
			return Boolean.valueOf(val);
		case JAVA_TIMESTAMP:
			return java.sql.Timestamp.valueOf(val);
		case JAVA_BIGDECIMAL:
			return new BigDecimal(val);
		case JAVA_DOUBLE:
			return new BigDecimal(val.toString()).doubleValue();
		case JAVA_SHORT:
			return new BigDecimal(val.toString()).shortValue();
		case JAVA_BIGINTEGER:
			return new BigDecimal(val.toString()).toBigInteger();
		case JAVA_LONG:
			return new BigDecimal(val.toString()).longValue();
		case JAVA_FLOAT:
			return new BigDecimal(val.toString()).floatValue();
		case JAVA_PRIMITIVE_BYTEARRAY:
			if (Base64.isBase64(val))
				return Base64.decodeBase64(val);
			else
				return val.getBytes();
		case JAVA_STRING:
			return val;
		default:
			throw new IllegalArgumentException("Invalid Flield Type "
					+ jdt.name());
		}
	}

	public Object[] getData() throws Exception {
		this.curRowIndex += 1;
		String row = (String) this.rowList.get(this.curRowIndex);
		String[] data = row.split(",");
		if(data.length != fieldList.size())
			throw new IllegalArgumentException("Number of columns in CSV  {"+fileName+"} does not match the number of fields in object {"+fieldList.get(0).getContainingRecord().getRecordName()+"}");
		Object[] obArr = new Object[data.length];
		for (int i = 0; i < data.length; i++) {
			try
			{
				obArr[i] = getObjectFromString(data[i],this.fieldList.get(i).getJavaDatatype());
			}catch(Throwable t)
			{
				System.out.println("Invalid Value: "+ data[i]);
				t.printStackTrace();
			}
		}
		return obArr;
	}

	public int getRowCount() {
		if (this.rowList != null) {
			return this.rowList.size();
		}
		return 0;
	}

	public boolean hasMoreRows() {
		return this.curRowIndex < this.rowList.size() - 1;
	}
}