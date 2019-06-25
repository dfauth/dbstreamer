package com.github.dfauth.dbstreamer;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;

public class DataSourceConfig {

    private String driverName;
    private String jdbcUrl;
    private String username;
    private String password;

    public static DataSourceConfig.Builder builder() {
        return new Builder();
    }

    public void setDriver(String s) {
        this.driverName = s;
    }

    public void setUrl(String s) {
        this.jdbcUrl = s;
    }

    public void setUsername(String s) {
        this.username = s;
    }

    public void setPassword(String s) {
        this.password  = s;
    }

    public DataSource getDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driverName);
        ds.setUrl(jdbcUrl);
        ds.setUsername(username);
        ds.setPassword(password);


        ds.setMinIdle(5);
        ds.setMaxIdle(10);
        ds.setMaxOpenPreparedStatements(100);
        return ds;
    }

    public static class Builder {

        private DataSourceConfig dsc = new DataSourceConfig();

        public Builder withDriver(String driver) {
            dsc.setDriver(driver);
            return this;
        }

        public Builder withUrl(String url) {
            dsc.setUrl(url);
            return this;
        }

        public Builder withUsername(String username) {
            dsc.setUsername(username);
            return this;
        }

        public Builder withPassword(String password) {
            dsc.setPassword(password);
            return this;
        }

        public DataSource build() {
            return dsc.getDataSource();
        }
    }
}
