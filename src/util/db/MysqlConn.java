package util.db;

import java.io.*;
import java.sql.*;
import java.util.*;

public class MysqlConn {
    private String driver;
    private String host;
    private String port;
    private String db;
    private String url;
    private String user;
    private String pwd;
    private Connection conn = null;
    private boolean dbWasChacked = false;

    private static MysqlConn instance = null;

    public static final SqlLoader<Integer> INT_LOADER = new SqlLoader<Integer>() {
        public Integer collectRow(ResultSet rs) throws Exception {
            return rs.getInt(1);
        }
    };

    public static MysqlConn get() {
        if (instance != null)
            return instance;
        try {
            File config = new File("mysql.cfg").getCanonicalFile();
            if (!config.exists())
                throw new IllegalStateException("Mysql configuration file [" + 
                		config.getAbsolutePath() + "] was not found");
            Properties props = new Properties();
            InputStream is = new FileInputStream(config);
            props.load(is);
            is.close();
            String host = getConfigParam(props, "host", "localhost");
            String port = getConfigParam(props, "port", "3306");
            String db = getConfigParam(props, "db");
            String user = getConfigParam(props, "user");
            String pwd = getConfigParam(props, "pwd", "");
            instance = new MysqlConn(host, port, db, user, pwd);
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            	public void run() {
            		instance.close();
            		System.out.println("Mysql connection was closed in shutdown hook.");
            	}
            }));
            return instance;
        } catch (IOException e) {
            throw new IllegalStateException("Error loading mysql configuration properties", e);
        }
    }

    private static String getConfigParamOrNull(Properties props, String paramName) {
        return props.getProperty(paramName);
    }

    private static String getConfigParam(Properties props, String paramName) {
        String ret = getConfigParamOrNull(props, paramName);
        if (ret == null)
            throw new IllegalStateException("Parameter " + paramName + " is not set in configuration");
        return ret;
    }

    private static String getConfigParam(Properties props, String paramName, String defaultValue) {
        String ret = getConfigParamOrNull(props, paramName);
        if (ret == null)
            ret = defaultValue;
        return ret;
    }

    public MysqlConn(String host, String port, String db, String user, String pwd) {
        this("org.gjt.mm.mysql.Driver", host, port, db, user, pwd);
    }

    public MysqlConn(String driver, String host, String port, String db, String user, String pwd) {
        this.driver = driver;
        this.host = host;
        this.port = port;
        this.db = db;
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + db;
        this.user = user;
        this.pwd = pwd;
    }

    private void checkForDb() throws SQLException {
        if (dbWasChacked)
            return;
        String url = "jdbc:mysql://" + host + ":" + port + "/mysql";
        Connection conn = DriverManager.getConnection(url, user, pwd);
        try {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getCatalogs();
            boolean found = false;
            while (rs.next() && !found) {
                String db = rs.getString("TABLE_CAT");
                if (db.equalsIgnoreCase(this.db))
                    found = true;
            }
            rs.close();
            if (!found) {
                Statement stmt = conn.createStatement();
                stmt.execute("create database " + this.db);
                stmt.close();
                System.out.println("Database " + db + " was created");
            }
            dbWasChacked = true;
        } finally {
            conn.close();
        }
    }

    public Connection getConnection() {
        Exception error = null;
        for (int i = 0; i < 3; i++) {
            if (conn != null) {
                try {
                    Statement st = conn.createStatement();
                    st.executeQuery("select 1").close();
                    st.close();
                    break;
                } catch (Exception ex) {
                    // Something wrong
                    System.out.println("Problems with database connection, trying to reconnect...");
                    close();
                }
            }
            if (conn == null) {
                try {
                    error = null;
                    Class.forName(driver);
                    conn = DriverManager.getConnection(url, user, pwd);
                } catch (Exception e) {
                    error = e;
                    try { checkForDb(); } catch (Exception ignore) {ignore.printStackTrace();}
                }
            }
        }
        if (conn == null && error != null)
            throw new IllegalStateException("Error connecting to database: " + url, error);
        return conn;
    }

    public MysqlConn dropTableIfExists(String... tables) throws SQLException {
        for (String table : tables)
            execSql("DROP TABLE IF EXISTS " + table);
        return this;
    }

    public synchronized MysqlConn execSql(String sql, Object... params) throws SQLException {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++)
            st.setObject(1 + i, params[i]);
        st.execute();
        st.close();
        return this;
    }

    public synchronized MysqlConn execBatch(String sql, RowProvider paramsProvider) throws Exception {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql);
        int bufferSize = 0;
        while (true) {
            Object[] params = paramsProvider.nextRow();
            if (params == null)
                break;
            for (int i = 0; i < params.length; i++)
                st.setObject(1 + i, params[i]);
            st.addBatch();
            bufferSize++;
            if (bufferSize == 100) {
                st.executeBatch();
                bufferSize = 0;
            }
        }
        if (bufferSize > 0)
            st.executeBatch();
        st.close();
        return this;
    }

    public synchronized Batch execBatch(String sql) throws Exception {
        Connection conn = getConnection();
        final PreparedStatement st = conn.prepareStatement(sql);
        return new Batch() {
            int bufferSize = 0;
            @Override
            public void addNextRow(Object[] row) throws Exception {
                for (int i = 0; i < row.length; i++)
                    st.setObject(1 + i, row[i]);
                st.addBatch();
                bufferSize++;
                if (bufferSize >= 1000)
                    flush();
            }

            private void flush() throws Exception {
                if (bufferSize > 0)
                    st.executeBatch();
                bufferSize = 0;
            }

            @Override
            public void close() throws Exception {
                flush();
                st.close();
            }
        };
    }


    public synchronized MysqlConn execBatchInsert(String sql, RowProvider paramsProvider) throws Exception {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql);
        List<Object[]> buffer = new ArrayList<Object[]>();
        while (true) {
            Object[] row = paramsProvider.nextRow();
            if (row == null)
                break;
            buffer.add(row);
            if (buffer.size() >= 1000) {
                flushBuffer(sql, buffer);
                buffer.clear();
            }
        }
        if (buffer.size() > 0)
            flushBuffer(sql, buffer);
        st.close();
        return this;
    }

    private void flushBuffer(String sqlPrefix, List<Object[]> buffer) throws Exception {
        int placeholderCount = buffer.get(0).length;
        final StringBuilder builder0 = new StringBuilder("(");
        for ( int i = 0; i < placeholderCount; i++ ) {
            if ( i != 0 ) {
                builder0.append(",");
            }
            builder0.append("?");
        }
        String placeholders = builder0.append(")").toString();
        final StringBuilder builder = new StringBuilder(sqlPrefix + " VALUES ");
        for (int i = 0; i < buffer.size(); i++) {
            if (i != 0)
                builder.append(",");
            builder.append(placeholders);
        }
        final String query = builder.toString();
        final PreparedStatement statement = getConnection().prepareStatement(query);
        int parameterIndex = 1;
        for (Object[] row : buffer) {
            for (Object value : row)
                statement.setObject(parameterIndex++, value);
        }
        statement.execute();
        statement.close();
    }

    public Integer execSqlWithKeygen(String sql, Object... params) throws Exception {
        return execSqlWithKeygen(sql, INT_LOADER, params);
    }

    public synchronized <T> T execSqlWithKeygen(String sql, SqlLoader<T> keyLoader, Object... params) throws Exception {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++)
            st.setObject(1 + i, params[i]);
        st.executeUpdate();
        T ret = null;
        ResultSet rs = st.executeQuery("SELECT LAST_INSERT_ID()");
        if (rs.next())
            ret = keyLoader.collectRow(rs);
        rs.close();
        st.close();
        return ret;
    }

    public synchronized MysqlConn processSql(String sql, SqlProcessor sp, Object... params) throws Exception {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        st.setFetchSize(Integer.MIN_VALUE);
        for (int i = 0; i < params.length; i++)
            st.setObject(1 + i, params[i]);
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            sp.processRow(rs);
        }
        rs.close();
        st.close();
        return this;
    }

    public synchronized <T> List<T> collectSql(String sql, SqlLoader<T> sl, Object... params) throws Exception {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++)
            st.setObject(1 + i, params[i]);
        ResultSet rs = st.executeQuery();
        List<T> ret = new ArrayList<T>();
        while (rs.next()) {
            ret.add(sl.collectRow(rs));
        }
        rs.close();
        st.close();
        return ret;
    }

    public synchronized <T> T loadSingle(String sql, SqlLoader<T> sl, Object... params) throws Exception {
        Connection conn = getConnection();
        PreparedStatement st = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++)
            st.setObject(1 + i, params[i]);
        ResultSet rs = st.executeQuery();
        T ret = null;
        if (rs.next()) {
            ret = sl.collectRow(rs);
        }
        rs.close();
        st.close();
        return ret;
    }

    public synchronized void close() {
        Connection c = conn;
        conn = null;
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignore) {}
        }
    }

    public boolean checkTable(String tableName) throws Exception {
        Set<String> tables = new HashSet<String>(collectSql("show tables", new SqlLoader<String>() {
            public String collectRow(ResultSet rs) throws Exception {
                return rs.getString(1).toUpperCase();
            }
        }));
        return tables.contains(tableName.toUpperCase());
    }

    public static interface SqlProcessor {
        void processRow(ResultSet rs) throws Exception;
    }

    public interface SqlLoader<T> {
        T collectRow(ResultSet rs) throws Exception;
    }

    public interface RowProvider {
        Object[] nextRow() throws Exception;
    }

    public abstract static class RowProviderForList<T> implements RowProvider {
        private List<T> list;
        private int currentIndex = 0;

        public RowProviderForList(List<T> list) {
            this.list = list;
        }

        public abstract Object[] transformIntoRow(T item) throws Exception;

        @Override
        public Object[] nextRow() throws Exception {
            if (currentIndex >= list.size())
                return null;
            Object[] ret = transformIntoRow(list.get(currentIndex));
            currentIndex++;
            return ret;
        }
    }

    public abstract static class Batch {
        public abstract void addNextRow(Object[] row) throws Exception;
        public abstract void close() throws Exception;
    }

    public abstract static class TypedBatch<T> {
        private Batch inner;

        public TypedBatch(Batch inner) {
            this.inner = inner;
        }

        public abstract Object[] transformIntoRow(T item) throws Exception;

        public void addNextItem(T item) throws Exception {
            inner.addNextRow(transformIntoRow(item));
        }

        public void addItems(List<T> items) throws Exception {
            for (T item : items)
                inner.addNextRow(transformIntoRow(item));
        }
    }
}