package com.uiuc.systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Path;

public class LeaderLoggerFactory {

    private static final String BASE_DIR = "rainstorm_logs";

    public static Logger createLeaderLogger() {
        try {
            Path logDir = Paths.get(BASE_DIR);
            Files.createDirectories(logDir);
            long ts = System.currentTimeMillis();
            String filename = logDir.resolve("rainstorm_leader_" + ts + ".log").toString();
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(context);
            encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n");
            encoder.start();

            FileAppender<ILoggingEvent> appender = new FileAppender<>();
            appender.setContext(context);
            appender.setName("LEADER_FILE_" + ts);
            appender.setFile(filename);
            appender.setAppend(true);
            appender.setEncoder(encoder);
            appender.start();
            Logger slf4jLogger = LoggerFactory.getLogger("RainStormLeader-" + ts);
            ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) slf4jLogger;

            logbackLogger.addAppender(appender);
            logbackLogger.setAdditive(false);
            logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO);
            slf4jLogger.info("Leader logger started: " + filename);
            return slf4jLogger;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize LeaderLogger", e);
        }
    }
}
