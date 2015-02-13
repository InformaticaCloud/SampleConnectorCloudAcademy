package com.informatica.connector.wrapper.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;

public class OutputDataBufferImpl implements IOutputDataBuffer {
	private FileWriter writer;
	private StringBuffer finalString;
	private List<Field> fieldList;
	
	public static final char LF = '\n';

	public static final char CR = '\r';

	public static final char QUOTE = '"';

	public static final char COMMA = ',';

	//private static final Log log = LogFactory.getLog(OutputDataBufferImpl.class);

	public OutputDataBufferImpl(String fileName, List<Field> fldList)
			throws IOException {

		if(fileName==null || fileName.isEmpty())		
			throw new IllegalArgumentException("Input Argument {fileName} cannot be null");

		if(fldList==null || fldList.isEmpty())		
			throw new IllegalArgumentException("Input Argument {fldList} cannot be null");
		
		
		this.fieldList = fldList;

		createCSVDirectory();
		fileName = "CSV" + File.separator + fileName;
		try {
			this.writer = new FileWriter(fileName);
			this.finalString = new StringBuffer();

			if (fldList != null) {
				for (int i = 0; i < fldList.size(); i++) {
					this.finalString.append(((Field) fldList.get(i))
							.getDisplayName());
					if (i < fldList.size() - 1)
						this.finalString.append(",");
				}
				this.finalString.append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void setData(Object[] data) throws DataConversionException,
			FatalRuntimeException {
		if (data != null) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < data.length; i++) {
				if (data[i] != null) {
					String javaDatatypeFullClassName = ((Field) this.fieldList.get(i))
							.getJavaDatatype().getFullClassName();
					
					String dataClassCanonicalName = data[i].getClass().getCanonicalName();
															
					try {

						if(!dataClassCanonicalName.equals(javaDatatypeFullClassName))
						{						
							if(!ConnectorUtils.classForJavaDataTypeFullClassName(javaDatatypeFullClassName).isInstance(data[i]))
							{
								throw new DataConversionException("The data[" + dataClassCanonicalName
								+ "] class and the field[" + javaDatatypeFullClassName
								+ "] class should match for field ["+(this.fieldList.get(i)).getDisplayName()+"]");
							}
						}						
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
						throw new DataConversionException(""+e);
					}

					if (data[i] instanceof byte[]) {
						sb.append(Base64.encodeBase64String((byte[]) data[i]));
					} else {
						sb.append(ConnectorUtils.getCSVFriendlyString(data[i].toString()));
					}
				} else {
					sb.append("null");
				}
				if (i < data.length - 1)
					sb.append(",");
			}
			this.finalString.append(sb.toString());
			this.finalString.append("\n");
		}
	}

	public void flush() {
		try {
			this.writer.write(this.finalString.toString());
			this.writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createCSVDirectory() {
		File csvDir = new File("CSV");
		if (csvDir.isDirectory()) {
			return;
		}
		csvDir.mkdir();
	}
	
	
}