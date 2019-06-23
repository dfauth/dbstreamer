package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public interface DataType<R> {

    Consumer<R> getWriteConsumer(PreparedStatement pstmt, int i);

    Function<ResultSet, R> getReadFunction(int ord);

    int sqlType();

    abstract class Factory<R> {

        private static final Logger logger = LoggerFactory.getLogger(Factory.class);

        public static final Factory VARCHAR_FACTORY = new Factory(String.class, "character varying", "character") {
            @Override
            public DataType<String> create() {
                return new DataTypeImpl<String>(Types.VARCHAR) {
                    @Override
                    public Consumer<String> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setString(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, String> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getString(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory INTEGER_FACTORY = new Factory(Integer.class, "integer") {
            @Override
            public DataType<Integer> create() {
                return new DataTypeImpl<Integer>(Types.INTEGER) {
                    @Override
                    public Consumer<Integer> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setInt(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, Integer> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getInt(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory BIGDECIMAL_FACTORY = new Factory(BigDecimal.class, "bigint") {
            @Override
            public DataType<BigDecimal> create() {
                return new DataTypeImpl<BigDecimal>(Types.BIGINT) {
                    @Override
                    public Consumer<BigDecimal> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setBigDecimal(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, BigDecimal> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getBigDecimal(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory BLOB_FACTORY = new Factory(Blob.class, "BINARY LARGE OBJECT") {
            @Override
            public DataType<Blob> create() {
                return new DataTypeImpl<Blob>(Types.BLOB) {
                    @Override
                    public Consumer<Blob> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setBlob(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, Blob> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getBlob(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory DATE_FACTORY = new Factory(Date.class, "date") {
            @Override
            public DataType<Date> create() {
                return new DataTypeImpl<Date>(Types.DATE) {
                    @Override
                    public Consumer<Date> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setDate(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, Date> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getDate(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory DOUBLE_FACTORY = new Factory(Double.class, "double precision") {
            @Override
            public DataType<Double> create() {
                return new DataTypeImpl<Double>(Types.DOUBLE) {
                    @Override
                    public Consumer<Double> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setDouble(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, Double> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getDouble(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory TIMESTAMP_FACTORY = new Factory(Timestamp.class, "timestamp") {
            @Override
            public DataType<Timestamp> create() {
                return new DataTypeImpl<Timestamp>(Types.TIMESTAMP) {
                    @Override
                    public Consumer<Timestamp> getWriteConsumer(PreparedStatement pstmt, int i) {
                        return v -> {
                            try {
                                pstmt.setTimestamp(i, v);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }

                    @Override
                    public Function<ResultSet, Timestamp> getReadFunction(int ord) {
                        return rs -> {
                            try {
                                return rs.getTimestamp(ord);
                            } catch (SQLException e) {
                                logger.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        };
                    }
                };
            }
        };

        public static final Factory[] values = new Factory[]{VARCHAR_FACTORY,
                INTEGER_FACTORY,
                BIGDECIMAL_FACTORY,
                DATE_FACTORY,
                BLOB_FACTORY,
        DOUBLE_FACTORY,
        TIMESTAMP_FACTORY};

        private final Set<String> aliases;
        private final Class<R> clazz;

        private Factory(Class<R> clazz, String... aliases) {
            this.aliases = Stream.of(aliases).map(a -> a.toUpperCase()).collect(Collectors.toSet());
            this.clazz = clazz;
        }

        public static Factory findBySqlType(String sqlDataType) {
            return Stream.of(values).filter(v -> v.aliases.contains(sqlDataType.toUpperCase())).findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown or unsupported sql type: "+sqlDataType));
        }

        public abstract DataType<R> create();
    }
}
