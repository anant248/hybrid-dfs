package com.uiuc.systems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderLoggerHelper {
    private static final Logger log = LoggerFactory.getLogger("RainstormLeader");

    public static void runStart(String cmd) {
        log.info("EVENT=RUN_START cmd=\"{}\"", cmd);
    }

    public static void runEnd(String status) {
        log.info("EVENT=RUN_END status={}", status);
    }

    public static void config(int stages, int tPerStage, boolean eo, boolean auto, int rate, int lw, int hw) {
        log.info("EVENT=CONFIG nStages={} nTasksPerStage={} exactlyOnce={} autoscale={} inputRate={} LW={} HW={}",stages, tPerStage, eo, auto, rate, lw, hw);
    }

    public static void taskStart(int stage, int taskId, String vmHost) {
        log.info("EVENT=TASK_START stage={} taskId={} vm={}", stage, taskId, vmHost);
    }

    public static void taskEnd(int stage, int taskId, String vmHost, String reason) {
        log.info("EVENT=TASK_END stage={} taskId={} vm={} reason={}", stage, taskId, vmHost, reason);
    }

    public static void taskFail(int stage, int taskId, String vmHost) {
        log.info("EVENT=TASK_FAIL stage={} taskId={} vm={} error={}", stage, taskId, vmHost);
    }

    public static void autoscaleDecision(int stage, int oldTasks, int newTasks, double rate) {
        log.info("EVENT=AUTOSCALE stage={} oldTasks={} newTasks={} inputRate={}",
                stage, oldTasks, newTasks, rate);
    }
}