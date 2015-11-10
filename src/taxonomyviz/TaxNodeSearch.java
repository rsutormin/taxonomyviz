package taxonomyviz;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import util.db.MysqlConn;

public class TaxNodeSearch {
	private static final Pattern slashDiv = Pattern.compile(Pattern.quote("/"));
	
	public static List<Integer> getPath(int taxId) throws Exception {
		String pathText = MysqlConn.get().loadSingle("select path from " + 
				MysqlDbManager.TBL_TAX_INDEX + " where taxid=?", new MysqlConn.SqlLoader<String>() {
			@Override
			public String collectRow(ResultSet rs) throws Exception {
				return rs.getString(1);
			}
		}, taxId);
		if (pathText == null)
			throw new IllegalStateException("Tax node wasn't found for id=" + taxId);
		String[] parts = slashDiv.split(pathText);
		List<Integer> ret = new ArrayList<Integer>();
		for (String part : parts)
			if (part.length() > 0)
				ret.add(Integer.valueOf(part));
		return ret;
	}
	
	public static TaxNode getNode(int taxId) throws Exception {
		return MysqlConn.get().loadSingle("select taxid,title,hidden from " + 
				MysqlDbManager.TBL_TAX_INDEX + " where taxid=?", new MysqlConn.SqlLoader<TaxNode>() {
			@Override
			public TaxNode collectRow(ResultSet rs) throws Exception {
				TaxNode ret = new TaxNode(rs.getInt(1), rs.getString(2));
				ret.hidden = rs.getInt(3);
				return ret;
			}
		}, taxId);
	}

	public static List<TaxNode> getChildren(int taxId) throws Exception {
		return MysqlConn.get().collectSql("select taxid,title,hidden from " + 
				MysqlDbManager.TBL_TAX_INDEX + " where parid=?", new MysqlConn.SqlLoader<TaxNode>() {
			@Override
			public TaxNode collectRow(ResultSet rs) throws Exception {
				TaxNode ret = new TaxNode(rs.getInt(1), rs.getString(2));
				ret.hidden = rs.getInt(3);
				return ret;
			}
		}, taxId);
	}
	
	//public static Map<String, Object> searchSubtree(List<Object> trackItems) {
	//}

	public static void main(String[] args) throws Exception {
		TaxTreeLoader.loadTaxTree();
		List<Integer> path = getPath(83333);
		System.out.println(path);
		for (int taxId : path) {
			System.out.println(new ObjectMapper().writeValueAsString(getNode(taxId)) + "  =>  " +
					getChildren(taxId).size());
		}
	}
}
