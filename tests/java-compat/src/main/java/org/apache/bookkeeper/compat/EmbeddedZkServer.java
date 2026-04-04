package org.apache.bookkeeper.compat;

import org.apache.curator.test.TestingServer;
import java.io.File;

/**
 * Starts an embedded ZooKeeper server for testing.
 * Prints the port number to stdout, then waits for process termination.
 */
public class EmbeddedZkServer {
    public static void main(String[] args) throws Exception {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "bk-test-zk-" + System.currentTimeMillis());
        tempDir.mkdirs();

        TestingServer zk = new TestingServer(-1, tempDir, true); // -1 = random port
        int port = zk.getPort();

        // Print port so the parent process can read it
        System.out.println("ZK_PORT:" + port);
        System.out.flush();

        // Wait until killed
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zk.close();
                deleteDir(tempDir);
            } catch (Exception e) {
                // ignore
            }
        }));

        // Block forever
        Thread.currentThread().join();
    }

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDir(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
