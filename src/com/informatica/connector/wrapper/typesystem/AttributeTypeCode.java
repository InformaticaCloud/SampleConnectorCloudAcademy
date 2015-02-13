package com.informatica.connector.wrapper.typesystem;


public enum AttributeTypeCode {
	STRING("String",1,0,0,false),
	INTEGER("Integer",2,10,0,false),
	DOUBLE("Double",3,15,0,true),
	BOOLEAN("Boolean",4,10,0,false),
	DATETIME("DateTime",5,29,9,true),
	DECIMAL("Decimal",6,0,0,true),
	BINARY("Binary",7,0,0,false),
	SHORT("Short",8,10,0,false),
	LONG("Long",9,19,0,false),
	BIGINT("BigInteger",10,19,0,false),
	FLOAT("Float",11,15,0,true),
	BYTE("Byte",12,5,0,false);

    private String dataTypeName;
    private  int dataTypeId;
    private  int defaultPrecision;
    private  int defaultScale;
    private boolean hasScale;

    AttributeTypeCode(String dataTypeName, int dataTypeId, int defaultPrecision, int defaultScale, boolean hasScale) {
        this.dataTypeName = dataTypeName;
        this.dataTypeId = dataTypeId;
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        this.hasScale = hasScale;
    }
    
    public String getDataTypeName() {
        return this.dataTypeName;
    }
  
    public int getDataTypeId() {
        return this.dataTypeId;
    }

    public int getDefaultPrecision() {
        return this.defaultPrecision;
    }

    
    public int getDefaultScale() {
		return this.defaultScale;
	}

	public boolean hasScale() {
		return this.hasScale;
	}

	public static AttributeTypeCode fromValue(String value) {
        for(AttributeTypeCode c : AttributeTypeCode.values()) {
              if(c.getDataTypeName().equals(value)) {
                    return c;
              }
        }
        throw new IllegalArgumentException(value);
  }

	
}
