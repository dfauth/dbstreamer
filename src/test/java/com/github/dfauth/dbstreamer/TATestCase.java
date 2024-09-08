package com.github.dfauth.dbstreamer;

import lombok.extern.slf4j.Slf4j;
import org.hsqldb.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.dfauth.dbstreamer.Predicates.caseInsensitiveStringComparisonOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
public class TATestCase {

    private static final String LOCALHOST = "localhost";
    private final Server sourcedB = new Server();
    private final Server targetdB = new Server();

    /**

     spring.datasource.url=jdbc:hsqldb:file:data/testdb;ifexists=true
     spring.datasource.username=sa
     spring.datasource.password=

     ## MySQL
     #spring.datasource.url=jdbc:mysql://127.0.0.1:3306/fred
     #spring.datasource.username=damian
     #spring.datasource.password=W@tcher1


     */

    @BeforeTest
    private void setUp() {
//        targetdB.setAddress(LOCALHOST);
//        targetdB.setPort(9003);
//        targetdB.setDatabaseName(0, "simplehr1");
//        targetdB.setDatabasePath(0, "./data/simplehr1");
//        targetdB.start();
//        sourcedB.setAddress(LOCALHOST);
//        sourcedB.setPort(3306);
//        sourcedB.setDatabaseName(0, "fred");
//        sourcedB.start();
    }

    @AfterTest
    private void tearDown() {
//        sourcedB.stop();
//        targetdB.stop();
    }

    @Test
    public void testIt() {

        DataSource target = DataSourceConfig.builder().
                withDriver("org.hsqldb.jdbcDriver").
                withUrl("jdbc:hsqldb:file:../ta/data/testdb;ifexists=true").
                withUsername("sa").
                withPassword("").
                build();

        DataSource source = DataSourceConfig.builder().
                withDriver("com.mysql.jdbc.Driver").
                withUrl("jdbc:mysql://localhost:3306/fred").
                withUsername("damian").
                withPassword("W@tcher1").
                build();


        DbStreamer dbStreamer = new DbStreamer(source, target);
        dbStreamer.includeTables(t -> "PRICE".equalsIgnoreCase(t));

        List<TableDefinition> tables = dbStreamer.sniff().stream().collect(Collectors.toList());

        // check each table
        tables.stream().forEach(t -> {
            int rowsTarget = dbStreamer.getTargetdB().countRows(t.getName());
            int rowsSource = dbStreamer.getSourcedB().countRows(t.getName());
            assertTrue(rowsSource != 0, "Oops table "+t.getName()+" has zero rows in source db");
            assertTrue(rowsTarget == 0, "Oops table "+t.getName()+" has non-zero number of rows in target db");
        });

        List<String> tmp = new ArrayList<>();
        dbStreamer.withColumnUpdate(caseInsensitiveStringComparisonOf("password"), cu -> cu).stream(td -> {
            // capture the available tables
            tmp.add(td.getName());
        });

        // check each table
        tmp.stream().forEach(t -> {
            int rowsTarget = dbStreamer.getTargetdB().countRows(t);
            int rowsSource = dbStreamer.getSourcedB().countRows(t);
            assertEquals(rowsSource, rowsTarget, "Oops for table "+t+" source ("+rowsSource+") and target ("+rowsTarget+") dbs have differing number of rows");
        });

    }
}

