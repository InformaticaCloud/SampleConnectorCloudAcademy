package com.informatica.connector.wrapper.util;

import java.util.ArrayList;
import java.util.List;

import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
import com.informatica.cloud.api.adapter.metadata.Field;


public class PreviewDataBufferImpl implements IOutputDataBuffer {
	private List<Field> fieldList;
	ArrayList<String[]> previewData = new ArrayList<String[]>();

	public PreviewDataBufferImpl(List<Field> list) {
		this.fieldList = list;
	}

	public void setData(Object[] data) throws DataConversionException,
			FatalRuntimeException {
		if (data != null && data.length > 0) 
		{

			if (data.length != fieldList.size())
				throw new IllegalArgumentException(
						"data array size should match fieldList size");

			String[] strdata = new String[data.length];
			for (int i = 0; i < data.length; i++) {
				if (data[i] != null) 
				{
					String fieldJDTClassName = this.fieldList.get(i).getJavaDatatype().getFullClassName();
					
					String dataClassName = data[i].getClass().getCanonicalName();
					
					try 
					{						
						if(!dataClassName.equals(fieldJDTClassName))
						{						
							if(!ConnectorUtils.classForJavaDataTypeFullClassName(fieldJDTClassName).isInstance(data[i]))
							{
								throw new DataConversionException("The data[" + dataClassName
								+ "] class and the field[" + fieldJDTClassName
								+ "] class should match for field ["+this.fieldList.get(i).getDisplayName()+"]");
							}
						}						
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
						throw new DataConversionException(""+e);
					}

					if (data[i].getClass().getName()
							.equals(byte[].class.getName())) {
						strdata[i] = "Binary Data";
					} else {
						strdata[i] = data[i].toString();
						// Truncate large string value to speedup data preview
						if (strdata[i].length() > 50)
							strdata[i] = strdata[i].substring(0, 50)
									+ "...(truncated)";
					}
				}
			}
			previewData.add(strdata);
		}
	}

	public void flush() {
	}

	String[][] getDataPreview() {
		if(!previewData.isEmpty())
		{
			return previewData.toArray(new String[0][0]);
		}else
		{
			String[][] dataRows = new String[1][fieldList.size()];
			if(fieldList.size()>0)
				dataRows[0][0] = "No data found";
			return dataRows;			
		}
	}

}