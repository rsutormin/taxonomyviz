package taxonomyviz;

import util.db.MysqlConn;

public class MysqlDbManager {
	public static final String TBL_TAX_INDEX = "taxonomy";
	
	public static MysqlConn conn() {
		return MysqlConn.get();
	}
	
	public static MysqlConn.Batch createTaxIndexBatch() throws Exception {
		MysqlConn conn = MysqlConn.get();
		if (!conn.checkTable(TBL_TAX_INDEX)) {
            conn.execSql("" +
                    "create table " + TBL_TAX_INDEX + " (" +
                    "  taxid integer primary key, " +
                    "  parid integer, " +
                    "  title varchar(255), " +
                    "  hidden tinyint, " +
                    "  layer integer, " +
                    "  lpos integer, " +
                    "  ind integer, " +
                    "  maxind integer, " +
                    "  path varchar(255), " +
                    "  size integer, " +
                    "  index (parid), " +
                    "  index (layer, lpos), " +
                    "  index (ind) " +
                    ")");
        }
		return conn.execBatch("insert into " + TBL_TAX_INDEX + " (taxid, parid, title, hidden, "
				+ "layer, lpos, ind, maxind, path, size) values (?,?,?,?,?,?,?,?,?,?)");
	}
}
