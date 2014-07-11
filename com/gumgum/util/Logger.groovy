package com.gumgum.util

class Logger {

    def clazz

    /**
     * Prints a message to stdout with a date string in front of it.
     */
    def log(level="INFO", msg) {
        def dateFormat = "MMM dd yyyy HH:mm:ss a"
        def dateFormatted = new Date().format(dateFormat)
        println "${dateFormatted} ${level}  (${shorten(clazz.name)}) - ${msg}"
    }

    def error(msg) {
        log("ERROR", msg)
    }

    def warn(msg) {
        log("WARN", msg)
    }

    def info(msg) {
        log("INFO", msg)
    }

    def debug(msg) {
        log("DEBUG", msg)
    }

    def shorten(className) {
        def parts = className.split('\\.')
        if (parts.size() > 2) {
            return parts[0..-2].collect {it[0]}.join('.') + "." + parts[-1]
        }
        return className
    }

    /**
     * Logs an error message and prints a stack trace when the given code block throws an exception.
     * Useful to surround scripts to make sure any otherwise uncaught exception is logged.
     */
    def logIfThrows(closure) {
        try {
            closure()
        } catch (Throwable t) {
            error(t.getMessage())
            t.printStackTrace()
        }
    }
}

