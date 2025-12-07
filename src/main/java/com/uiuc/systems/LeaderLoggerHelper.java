package com.uiuc.systems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderLoggerHelper {
    private static final Logger log = LoggerFactory.getLogger("RainstormLeader");

    public static void runStart() {
        log.info("EVENT=RUN_START");
    }

    public static void runEnd() {
        log.info("EVENT=RUN_END");
    }

    public static void config(int stages, int tPerStage, boolean eo, boolean auto, int rate, int lw, int hw) {
        log.info("EVENT=CONFIG nStages={} nTasksPerStage={} exactlyOnce={} autoscale={} inputRate={} LW={} HW={}",stages, tPerStage, eo, auto, rate, lw, hw);
    }

    public static void taskStart(int stage, int taskId, String host) {
        log.info("EVENT=TASK_START stage={} taskId={} vm={}", stage, taskId, host);
    }

    public static void taskEnd(int stage, int taskId, String host, String reason) {
        log.info("EVENT=TASK_END stage={} taskId={} vm={} reason={}", stage, taskId, host, reason);
    }

    public static void taskFail(int stage, int taskId, String host) {
        log.info("EVENT=TASK_FAIL stage={} taskId={} vm={} error={}", stage, taskId, host);
    }

    public static void autoscaleDecision(int stage, int oldTasks, int newTasks, double rate) {
        log.info("EVENT=AUTOSCALE stage={} oldTasks={} newTasks={} inputRate={}",
                stage, oldTasks, newTasks, rate);
    }

    public static void taskRestart(int stage, int taskId, String host) {
        log.info("EVENT=TASK_RESTART stage={} taskId={} vm={}", stage, taskId, host);
    }

    public static void taskScaleDown(int stage, int taskId, String host){
        log.info("EVENT=SCALE_DOWN stage={} taskId={} host={}",stage,taskId,host);
    }

    public static void taskScaleUp(int stage, int taskId, String host){
        log.info("EVENT=SCALE_DOWN stage={} taskId={} host={}",stage,taskId,host);
    }
}