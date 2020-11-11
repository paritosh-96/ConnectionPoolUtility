package clari5.dbcon;

import java.util.List;

public class ConnectionPool {

    public enum DB_TYPE{ MYSQL, ORACLE, SQLSERVER}

    public DB_TYPE dbType;
    public String machine;
    public String port;
    public String sid;
    public String user;
    public String password;
    public String driver;
    public String serverType;

    public int maxCheckoutTime = 1000;
    public int maxIdleTime = 10000;
    public int initPoolSize = 1;
    public int minPoolSize = 1;
    public int maxPoolSize = 10;
    public int maxConnectionAge = 0;
    public int maxIdleTimeExcessConnection = 0;

    public int maxStatements = 50000;
    public int maxStatementsPerConnection = 0;
    public int numHelper = 5;
    public boolean autoCommit = false;
    public String isolation = "COMMITTED";
    public String testQuery = null;

    boolean setDbType(String dbType) {
        switch (dbType.trim().toUpperCase()) {
            case "MYSQL":
                this.dbType = DB_TYPE.MYSQL;
                break;
            case "SQLSERVER":
                this.dbType = DB_TYPE.SQLSERVER;
                break;
            case "ORACLE":
                this.dbType = DB_TYPE.ORACLE;
                break;
            default:
                System.out.println("Db type not supported. Please choose in [MYSQL, SQLSERVER, ORACLE]");
                return false;
        }
        return true;
    }

    String readData(List<String> setting, int index, String inputValue) {
        if (inputValue == null || inputValue.trim().isEmpty()) {
            return setting.get(index);
        }
        setting.set(index, inputValue.trim());
        return setting.get(index);
    }

    void setPoolAttrs(String attrName, String value) {
         if (value == null || value.trim().isEmpty()) return;

         switch (attrName) {
             case "autoCommit":
                 autoCommit = value.trim().equals("true");
                 break;
             case "isolation":
                 isolation = value;
                 break;
             case "testQuery":
                 testQuery = value;
                 break;
             default:
         }

         int val = Integer.parseInt(value.trim());
         switch (attrName) {
             case "maxCheckoutTime":
                 maxCheckoutTime = val;
                 break;
             case "maxIdleTime":
                 maxIdleTime = val;
                 break;
             case "minPoolSize":
                 minPoolSize = val;
                 break;
             case "maxPoolSize":
                 maxPoolSize = val;
                 break;
             case "initPoolSize":
                 initPoolSize = val;
                 break;
             case "maxConnectionAge":
                 maxConnectionAge = val;
                 break;
             case "maxIdleTimeExcessConnection":
                 maxIdleTimeExcessConnection = val;
                 break;
             case "maxStatements":
                 maxStatements = val;
                 break;
             case "maxStatementsPerConnection":
                 maxStatementsPerConnection = val;
                 break;
             case "numHelper":
                 numHelper = val;
                 break;
             default:
                 break;
         }
    }

    String getUrl() {
        String url = null;
        switch (dbType) {
            case ORACLE:
                if (driver == null) driver = "oracle.jdbc.OracleDriver";
                if (serverType.trim().toUpperCase().equals("DEDICATED")) {
                    url = "jdbc:oracle:thin:@(DESCRIPTION=" +
                                "(ADDRESS_LIST=" +
                                    "(ADDRESS=(PROTOCOL=TCP)" +
                                              "(HOST=" + machine + ")" +
                                              "(PORT=" + port + ")" +
                                    ")" +
                                ")" +
                                "(CONNECT_DATA=" +
                                    "(SERVICE_NAME=" + sid + ")" +
                                    "(SERVER=DEDICATED)" +
                                ")" +
                            ")";
                } else {
                    url = "jdbc:oracle:thin:@//" + machine + ":" + port + "/" + sid;
                }
                break;
            case SQLSERVER:
                if (driver == null) driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                url = "jdbc:sqlserver://" + machine + ":" + port + ";database=" + user + ";sendStringParametersAsUnicode=false";
                break;
            case MYSQL:
                if (driver == null) driver = "com.mysql.jdbc.Driver";
                url = "jdbc:mysql://" + machine + ":" + port + "/" + sid + "?rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&useSSL=false";
                break;
        }
        return url;
    }
}
