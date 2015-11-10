package taxonomyviz;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import util.db.MysqlConn;

public class FunnyTrackExample {
	private static final String substring = "Roman";
	
	public static void main(String[] args) throws Exception {
		List<TaxNode> nodes = MysqlConn.get().collectSql("select taxid,title from " + 
				MysqlDbManager.TBL_TAX_INDEX + " where title like '%" + 
				substring.toLowerCase() + "%' group by parid", 
				new MysqlConn.SqlLoader<TaxNode>() {
			@Override
			public TaxNode collectRow(ResultSet rs) throws Exception {
				return new TaxNode(rs.getInt(1), rs.getString(2));
			}
		});
		Track track = new Track();
		track.name = "Index of " + substring;
		track.user = "rsutormin";
		track.fields = Arrays.asList("position");
		List<Object> data = new ArrayList<Object>();
		track.data = data;
		for (TaxNode node : nodes) {
			int position = node.title.toLowerCase().indexOf(substring.toLowerCase());
			Map<String, Object> item = new LinkedHashMap<String, Object>();
			item.put("taxid", node.taxid);
			item.put("position", position);
			data.add(item);
		}
		new ObjectMapper().writeValue(new File("data/track_1.json"), track);
	}
}
