package utils;

import java.io.FileWriter;
import java.io.IOException;
// import java.util.Random;

/**
 * MachineLog
 *
 * Usage:
 *   java util.MachineLog <filename> <numLines>
 *
 * Example:
 *   java util.MachineLog vm1_test.log 300000
 *   -> generates ~300,000 log entries in /vm1_test.log
 */
public class MachineLog {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java src/main/java/utils/MachineLog.java <filename> <numLines>");
            System.exit(1);
        }

        String fileName = args[0];
        int numLines = Integer.parseInt(args[1]);

        // Write into / directory inside your project
        String logDir = "";
        String filePath = logDir + fileName;

        try {
            // Ensure directory exists
            java.io.File dir = new java.io.File(logDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            try (FileWriter writer = new FileWriter(filePath, false)) {
                // Random rand = new Random();

                writer.write("=== Start of log ===\n");

                for (int i = 0; i < numLines; i++) {
                    // int level = rand.nextInt(3);
                    int level = i % 100;
                    String line;
                    switch (level) {
                        case 0:
                            line = "INFO: Event " + i + " at machine ";
                            break;
                        case 1:
                            line = "WARNING: Potential issue at event " + i + " on machine ";
                            break;
                        default:
                            line = "ERROR: Failure at event " + i + " on machine ";
                            break;
                    }
                    writer.write(line + "\n");
                }

                writer.write("=== End of log ===\n");
                System.out.println("Generated " + numLines + " log entries in " + filePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}