package taxonomyviz;

public class ColumnType {
	private ColumnTypeEnum elementaryType;
	private String dateFormat;
	private ClassificationNode classification;
	
	public static ColumnType IntegerVal = new ColumnType(ColumnTypeEnum.IntegerVal);
	public static ColumnType BooleanVal = new ColumnType(ColumnTypeEnum.BooleanVal);
	public static ColumnType DoubleVal = new ColumnType(ColumnTypeEnum.DoubleVal);
	public static ColumnType StringVal = new ColumnType(ColumnTypeEnum.StringVal);
	public static ColumnType ArrayVal = new ColumnType(ColumnTypeEnum.ArrayVal);
	public static ColumnType ObjectVal = new ColumnType(ColumnTypeEnum.ObjectVal);
	
	private ColumnType(ColumnTypeEnum elementaryType) {
		this.elementaryType = elementaryType;
	}
	
	public ColumnType(String dateFormat) {
		this.elementaryType = ColumnTypeEnum.DateVal;
		this.dateFormat = dateFormat;
	}

	public ColumnType(ClassificationNode classification) {
		this.elementaryType = ColumnTypeEnum.ClassificationVal;
		this.classification = classification;
	}

	public boolean isDate() {
		return elementaryType.equals(ColumnTypeEnum.DateVal);
	}
	
	public String getDateFormat() {
		return dateFormat;
	}

	public boolean isClassification() {
		return elementaryType.equals(ColumnTypeEnum.ClassificationVal);
	}

	public ClassificationNode getClassification() {
		return classification;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof ColumnType))
			return false;
		ColumnType ct = (ColumnType)obj;
		if (elementaryType == null || ct.elementaryType == null || 
				!elementaryType.equals(ct.elementaryType))
			return false;
		if (elementaryType.equals(ColumnTypeEnum.DateVal) && 
				(dateFormat == null || ct.dateFormat == null || 
				!dateFormat.equals(ct.dateFormat)))
			return false;
		if (elementaryType.equals(ColumnTypeEnum.ClassificationVal) && 
				(classification == null || ct.classification == null || 
				!classification.equals(ct.classification)))
			return false;
		return true;
	}

	private static enum ColumnTypeEnum {
		IntegerVal, BooleanVal, DoubleVal, StringVal, DateVal, ClassificationVal, ArrayVal, ObjectVal
	}
}
