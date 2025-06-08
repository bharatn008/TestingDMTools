package com.example.util;

public class QueryTranslator {
    public static String translate(String input) {
        return input.replace("select", "SELECT");
    }
}