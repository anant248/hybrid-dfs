package com.uiuc.systems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uiuc.systems.WorkerTask.PendingTuple;

public class WorkerLoggerHelper {
    private static final Logger log = LoggerFactory.getLogger("WorkerTask");

    public static void taskStart(int stage, int taskId, String host, String operator) {
        log.info("EVENT=TASK_START stage={} taskId={} vm={} operator={}", stage, taskId, host, operator);
    }

    public static void taskEnd(int taskId, String reason) {
        log.info("EVENT=TASK_END taskId={} reason={}", taskId, reason);
    }

    public static void taskRestart(int stage, int taskId, String host) {
        log.info("EVENT=TASK_RESTART stage={} taskId={} vm={}", stage, taskId, host);
    }

    public static void rejectedDuplicateTuples(int stage, int taskId, String tuple, int tupleId) {
        log.info("EVENT=REJECTED_DUPLICATE_TUPLES stage={} taskId={} tupleId={}, tuple={}", stage, taskId, tupleId, tuple);
    }

    public static void processedTuples(int stage, int taskId, String host, String tuple, int tupleId) {
        log.info("EVENT=PROCESSED_TUPLES stage={} taskId={} vm={} tupleId={} tuple={}", stage, taskId, host, tupleId, tuple);
    }

    public static void taskFailNotification(int taskId, String leaderIp, int leaderPort, DownstreamTarget target) {
        log.info("EVENT=TASK_FAIL_DETECTED Task {} notified leader {}:{} of failure of downstream task {} on host {} and port {}", taskId, leaderIp, leaderPort, target.getTaskId(), target.getHost(), target.getPort());
    }

    public static void retryTupleSend(int taskId, PendingTuple tuple) {
        log.info("EVENT=RETRY_TUPLE_SEND Task {} retrying tuple {} (attempt #{})", taskId, tuple.tupleId, tuple.retryCount + 1);
    }
}