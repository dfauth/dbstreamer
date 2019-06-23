package com.github.dfauth.dbstreamer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);
    
    private static Options options = new Options();

    static {
        options.addOption("targetJdbcUrl", true, "target JDBC URL");
        options.addOption("targetJdbcUsername", true, "target JDBC username");
        options.addOption("targetJdbcPassword", true, "target JDBC password");
        options.addOption("targetJdbcDriver", true, "target JDBC driver");

        options.addOption("sourceJdbcUrl", true, "source JDBC URL");
        options.addOption("sourceJdbcUsername", true, "source JDBC username");
        options.addOption("sourceJdbcPassword", true, "source JDBC password");
        options.addOption("sourceJdbcDriver", true, "source JDBC driver");
    }

    public static void main(String[] args) {
        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            DataSourceConfig.Builder source = new DataSourceConfig().builder();
            DataSourceConfig.Builder target = new DataSourceConfig().builder();
            
            source.withDriver(line.getOptionValue("sourceJdbcDriver"));
            source.withUrl(line.getOptionValue("sourceJdbcUrl"));
            source.withUsername(line.getOptionValue("sourceJdbcUsername"));
            source.withPassword(line.getOptionValue("sourceJdbcPassword"));
            
            target.withDriver(line.getOptionValue("targetJdbcDriver"));
            target.withUrl(line.getOptionValue("targetJdbcUrl"));
            target.withUsername(line.getOptionValue("targetJdbcUsername"));
            target.withPassword(line.getOptionValue("targetJdbcPassword"));
            
            new SchemaSucker(source.build(), target.build()).suck();

            Thread.sleep(10 * 1000);
            
        } catch (org.apache.commons.cli.ParseException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
