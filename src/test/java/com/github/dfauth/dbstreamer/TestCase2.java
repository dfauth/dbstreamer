package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import javax.sql.DataSource;

public class TestCase2 {

    private static final Logger logger = LoggerFactory.getLogger(TestCase2.class);

    @Test
    public void testIt() {

        try {
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

            new SchemaSucker(source, target).suck();

            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }
}

