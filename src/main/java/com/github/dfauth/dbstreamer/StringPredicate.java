package com.github.dfauth.dbstreamer;

import java.util.function.Predicate;


public class StringPredicate implements Predicate<String> {


    private String ref;

    public StringPredicate(String ref) {
        this.ref = ref;
    }

    @Override
    public boolean test(String s) {
        return ref.equalsIgnoreCase(s);
    }
}
