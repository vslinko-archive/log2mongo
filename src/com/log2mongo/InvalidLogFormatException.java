package com.log2mongo;

public class InvalidLogFormatException extends Exception {
    public InvalidLogFormatException() {
    }

    public InvalidLogFormatException(String s) {
        super(s);
    }

    public InvalidLogFormatException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public InvalidLogFormatException(Throwable throwable) {
        super(throwable);
    }
}
