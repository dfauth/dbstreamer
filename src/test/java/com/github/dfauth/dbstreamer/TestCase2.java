package com.github.dfauth.dbstreamer;

import org.hsqldb.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import static com.github.dfauth.dbstreamer.Predicates.caseInsensitiveStringComparisonOf;

public class TestCase2 {

    private static final Logger logger = LoggerFactory.getLogger(TestCase2.class);
    private static final String LOCALHOST = "localhost";
    private final Server sourcedB = new Server();
    private final Server targetdB = new Server();

    @BeforeTest
    private void setUp() {
        targetdB.setAddress(LOCALHOST);
        targetdB.setPort(9003);
        targetdB.setDatabaseName(0, "simplehr1");
        targetdB.setDatabasePath(0, "./data/simplehr1");
        targetdB.start();
        sourcedB.setAddress(LOCALHOST);
        sourcedB.setPort(9001);
        sourcedB.setDatabaseName(0, "simplehr");
        sourcedB.setDatabasePath(0, "./data/simplehr");
        sourcedB.start();
    }

    @AfterTest
    private void tearDown() {
        sourcedB.stop();
        targetdB.stop();
    }

    @Test
    public void testIt() {

        DataSource target = DataSourceConfig.builder().
                withDriver("org.hsqldb.jdbcDriver").
                withUrl("jdbc:hsqldb:hsql://localhost:9003/simplehr1").
                withUsername("SA").
                withPassword("").
                build();

        DataSource source = DataSourceConfig.builder().
                withDriver("org.hsqldb.jdbcDriver").
                withUrl("jdbc:hsqldb:hsql://localhost:9001/simplehr").
                withUsername("SA").
                withPassword("").
                build();

        new DbStreamer(source, target).withColumnUpdate(caseInsensitiveStringComparisonOf("password"), cu -> cu).stream();

    }
}

