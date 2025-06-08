package com.example.exception;

public class InvalidSchemaException extends RuntimeException {
    public InvalidSchemaException(String message) { super(message); }
}