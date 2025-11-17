package com.uiuc.systems;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MemberShipScheduler {
    /* This class acts as a scheduler and sends the messages(for both gossip and PingAck)
       every interval time units through the UDP Client.
    */
    private ScheduledExecutorService scheduler;

    public void start(Runnable task, long interval) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(task, 0, interval, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
    }
}
