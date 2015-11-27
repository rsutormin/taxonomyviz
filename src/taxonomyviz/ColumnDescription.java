package taxonomyviz;

public class ColumnDescription {
	public String id;
	public String name; 
	public ColumnType type;
	public String descr; 
	public String[] aliases;
	
	public ColumnDescription() {}
	
	public ColumnDescription(String id, String name, ColumnType type, String descr, String... aliases) {
		this.id = id;
		this.name = name;
		this.type = type;
		this.descr = descr;
		this.aliases = aliases;
	}
}
