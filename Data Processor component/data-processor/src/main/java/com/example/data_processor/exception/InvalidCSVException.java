package com.example.data_processor.exception;

public class InvalidCSVException extends RuntimeException {
    public InvalidCSVException(String message) {
        super(message);
    }
}
