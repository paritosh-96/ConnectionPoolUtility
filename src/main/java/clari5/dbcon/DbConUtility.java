package clari5.dbcon;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DbConUtility {

    private static ConnectionPool pool = new ConnectionPool();
    private static ComboPooledDataSource cpd;
    private static int MAX_SIZE = 7;

    public static void main(String[] args) throws IOException {
        DbConUtility dbConUtility = new DbConUtility();
        dbConUtility.init();
        // switch off pool logging
        Properties p = new Properties(System.getProperties());
        p.put("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
        p.put("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "OFF"); // Off or any other level
        System.setProperties(p);

        System.out.println("\n");

        long startTime = System.nanoTime();
        dbConUtility.configure();
        System.out.println("Pool Configuration time: [" + ((System.nanoTime() - startTime) / (double) 1000000) + "]ms");

        for (int i = 0; i < 10; i++) {
            long conStartTime = System.nanoTime();
            try (Connection con = cpd.getConnection()) {
                System.out.println("Connection[" + i + "] fetch time : [" + ((System.nanoTime() - conStartTime) / (double) 1000000) + "]ms");
                if (i == 9) {
                    String query = "";
                    switch (pool.dbType) {
                        case MYSQL:
                        case SQLSERVER:
                            query = "SELECT 1";
                            break;
                        case ORACLE:
                            query = "SELECT 1 FROM DUAL";
                            break;
                        default:
                            System.out.println("Unsupported db type");
                            System.exit(1);
                    }
                    long execStartTime = System.nanoTime();
                    PreparedStatement ps = con.prepareStatement(query);
                    ps.execute();
                    System.out.println("Query execution time: [" + ((System.nanoTime() - execStartTime) / (double) 1000000) + "]ms");
                }
            } catch (SQLException e) {
                System.out.println("Exception in getting connection from pool [" + e.getMessage() + "]");
            }
        }

        System.out.println("Total time taken by the utility: [" + ((System.nanoTime() - startTime) / (double) 1000000) + "]ms");

    }

    private void init() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Path cachePath = Paths.get(System.getProperty("user.dir"), "caches.txt");
        File f = new File(cachePath.toString());
        List<String> setting = new ArrayList<>();
        if (f.exists()) {
            setting = Files.readAllLines(cachePath);
        }
        int j = (MAX_SIZE - setting.size());
        for (int i = 0; i < j; i++) setting.add("");
        System.out.println("Please enter the following details to configure connection pool, You can press enter to select default value");
        System.out.print("DB TYPE - [" + setting.get(0) + "]: ");
        if (!pool.setDbType(pool.readData(setting, 0, reader.readLine()))) System.exit(1);

        System.out.print("MACHINE - [" + setting.get(1) + "]: ");
        pool.machine = pool.readData(setting, 1, reader.readLine());
        System.out.print("PORT - [" + setting.get(2) + "]: ");
        pool.port = pool.readData(setting, 2, reader.readLine());
        System.out.print("SID - [" + setting.get(3) + "]: ");
        pool.sid = pool.readData(setting, 3, reader.readLine());
        System.out.print("USER - [" + setting.get(4) + "]: ");
        pool.user = pool.readData(setting, 4, reader.readLine());
        System.out.print("PASSWORD - [" + setting.get(5) + "]: ");
        pool.password = pool.readData(setting, 5, reader.readLine());
        if (pool.dbType == ConnectionPool.DB_TYPE.ORACLE) {
            System.out.print("Server Type [ORACLE db only] (DEDICATED|SHARED) - [" + setting.get(6) + "]: ");
            pool.serverType = pool.readData(setting, 6, reader.readLine());
        }

        System.out.print("maxCheckoutTime - [1000]: ");
        pool.setPoolAttrs("maxCheckoutTime", reader.readLine());
        System.out.print("maxIdleTime - [10000]: ");
        pool.setPoolAttrs("maxIdleTime", reader.readLine());
        System.out.print("initPoolSize - [1]: ");
        pool.setPoolAttrs("initPoolSize", reader.readLine());
        System.out.print("minPoolSize - [1]: ");
        pool.setPoolAttrs("minPoolSize", reader.readLine());
        System.out.print("maxPoolSize - [10]: ");
        pool.setPoolAttrs("maxPoolSize", reader.readLine());
        System.out.print("maxConnectionAge - [0]: ");
        pool.setPoolAttrs("maxConnectionAge", reader.readLine());
        System.out.print("maxIdleTimeExcessConnection - [0]: ");
        pool.setPoolAttrs("maxIdleTimeExcessConnection", reader.readLine());
        System.out.print("maxStatements - [50000]: ");
        pool.setPoolAttrs("maxStatements", reader.readLine());
        System.out.print("maxStatementsPerConnection - [0]: ");
        pool.setPoolAttrs("maxStatementsPerConnection", reader.readLine());
        System.out.print("numHelper - [5]: ");
        pool.setPoolAttrs("numHelper", reader.readLine());

        System.out.print("autoCommit - [false]: ");
        pool.setPoolAttrs("autoCommit", reader.readLine());
        System.out.print("isolation - [COMMITTED]: ");
        pool.setPoolAttrs("isolation", reader.readLine());
        System.out.print("testQuery [for checkout test]: ");
        pool.setPoolAttrs("testQuery", reader.readLine());

        try (FileWriter fileWriter = new FileWriter(cachePath.toString())) {
            for (String line : setting) {
                fileWriter.write(line + System.lineSeparator());
            }
        }
    }

    private void configure() {
        cpd = new ComboPooledDataSource();

        // Set pool sizes (init/min/max)
        cpd.setInitialPoolSize(pool.initPoolSize);
        cpd.setMinPoolSize(pool.minPoolSize);

        cpd.setMaxPoolSize(pool.maxPoolSize);

        // Set connection configs
        cpd.setMaxConnectionAge(pool.maxConnectionAge);
        cpd.setMaxIdleTime(pool.maxIdleTime);  // 30 minutes
        cpd.setMaxIdleTimeExcessConnections(pool.maxIdleTimeExcessConnection);

        // Set statement configs
        cpd.setMaxStatements(pool.maxStatements);
        cpd.setMaxStatementsPerConnection(pool.maxStatementsPerConnection);

        // Set other properties, along with initializing autocommit;
        cpd.setAutoCommitOnClose(pool.autoCommit);
        cpd.setCheckoutTimeout(pool.maxCheckoutTime);
        cpd.setNumHelperThreads(pool.numHelper);

        cpd.setAcquireIncrement(1);
        cpd.setTestConnectionOnCheckin(false);

        String preferredTestQuery = pool.testQuery;
        if (preferredTestQuery != null) {
            cpd.setPreferredTestQuery(preferredTestQuery);
            cpd.setTestConnectionOnCheckout(true);
        } else {
            cpd.setTestConnectionOnCheckout(false);
        }

        // Set user, connection specific location
        cpd.setUser(pool.user);
        cpd.setJdbcUrl(pool.getUrl());
        cpd.setPassword(pool.password);
    }
}
