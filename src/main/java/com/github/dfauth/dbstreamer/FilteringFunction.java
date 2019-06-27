package com.github.dfauth.dbstreamer;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;


public class FilteringFunction<T,U> implements Function<T, UnaryOperator<U>> {

    private Predicate<T> filter = t -> true;
    private UnaryOperator<U> f = UnaryOperator.identity();

    public static <T,U> FilteringFunction<T,U> of(Predicate<T> filter, UnaryOperator<U> f) {
        return new FilteringFunction<>(filter, f);
    }

    public FilteringFunction(Predicate<T> filter, UnaryOperator<U> f) {
        this.filter = filter;
        this.f = f;
    }

    @Override
    public UnaryOperator<U> apply(T t) {
        if(this.filter.test(t)) {
            return f;
        } else {
            return UnaryOperator.identity();
        }
    }
}
