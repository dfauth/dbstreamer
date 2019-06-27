package com.github.dfauth.dbstreamer;

import java.util.function.Predicate;


public class Predicates {

    public static final Predicate<String> caseInsensitiveStringComparisonOf(String ref) {
            return s -> ref.equalsIgnoreCase(s);
    }

    public static final Predicate<String> caseSensitiveStringComparisonOf(String ref) {
            return s -> ref.equals(s);
    }

}
