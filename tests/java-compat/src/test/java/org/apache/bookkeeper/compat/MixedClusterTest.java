package org.apache.bookkeeper.compat;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Mixed-cluster integration tests: Java bookie + Rust bookie in the same cluster.
 *
 * <p>These tests verify that Java and Rust bookies can coexist in a single BookKeeper
 * cluster, sharing ZooKeeper metadata and serving reads/writes interchangeably.
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>ZooKeeper running (address passed via -Dzk.connect)</li>
 *   <li>BookKeeper cluster initialized</li>
 *   <li>At least one Java bookie AND one Rust bookie registered</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MixedClusterTest {

    private static final byte[] PASSWORD = "mixed-cluster-test".getBytes(StandardCharsets.UTF_8);
    private static String zkConnect;
    private BookKeeper bk;

    @BeforeAll
    static void readConfig() {
        zkConnect = System.getProperty("zk.connect", "localhost:2181");
        System.out.println("[MixedClusterTest] Using ZooKeeper at: " + zkConnect);
    }

    @BeforeEach
    void createClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk+hierarchical://" + zkConnect + "/ledgers");
        bk = new BookKeeper(conf);
    }

    @AfterEach
    void closeClient() throws Exception {
        if (bk != null) {
            bk.close();
            bk = null;
        }
    }

    /**
     * Test 1: Write to a 2-bookie ensemble spanning both Java and Rust bookies.
     *
     * <p>Creates a ledger with ensemble=2, writeQuorum=2, ackQuorum=2 so entries
     * are striped across BOTH bookies. Reads them back to verify cross-bookie
     * data integrity.
     */
    @Test
    void testMixedEnsembleWriteRead() throws Exception {
        final int numEntries = 50;

        // Create ledger spanning both bookies (E=2, W=2, A=2)
        LedgerHandle lh = bk.createLedger(2, 2, 2, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();
        System.out.println("Created ledger " + ledgerId + " with ensemble size 2");

        // Verify ensemble has 2 bookies
        assertEquals(2, lh.getLedgerMetadata().getEnsembleAt(0).size(),
                "Ensemble should have 2 bookies");
        System.out.println("Ensemble: " + lh.getLedgerMetadata().getEnsembleAt(0));

        // Write entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("mixed-entry-" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(numEntries - 1, lh.getLastAddConfirmed());
        lh.close();

        // Read back from the cluster
        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed());

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String expected = "mixed-entry-" + count;
            assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8),
                    "Entry " + count + " mismatch in mixed cluster");
            count++;
        }
        assertEquals(numEntries, count);
        readLh.close();
        System.out.println("Mixed ensemble write/read: " + numEntries + " entries OK");
    }

    /**
     * Test 2: Multiple ledgers with mixed ensemble — verify isolation.
     *
     * <p>Creates 5 ledgers across both bookies and verifies no cross-contamination.
     */
    @Test
    void testMultipleLedgersMixedCluster() throws Exception {
        final int numLedgers = 5;
        final int entriesPerLedger = 20;
        long[] ledgerIds = new long[numLedgers];

        for (int l = 0; l < numLedgers; l++) {
            LedgerHandle lh = bk.createLedger(2, 2, 2, BookKeeper.DigestType.CRC32C, PASSWORD);
            ledgerIds[l] = lh.getId();
            for (int i = 0; i < entriesPerLedger; i++) {
                lh.addEntry(("L" + l + "-E" + i).getBytes(StandardCharsets.UTF_8));
            }
            lh.close();
        }

        // Read each ledger back
        for (int l = 0; l < numLedgers; l++) {
            LedgerHandle readLh = bk.openLedger(ledgerIds[l], BookKeeper.DigestType.CRC32C, PASSWORD);
            assertEquals(entriesPerLedger - 1, readLh.getLastAddConfirmed());

            Enumeration<LedgerEntry> entries = readLh.readEntries(0, entriesPerLedger - 1);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                assertEquals("L" + l + "-E" + count,
                        new String(entry.getEntry(), StandardCharsets.UTF_8));
                count++;
            }
            assertEquals(entriesPerLedger, count);
            readLh.close();
        }
        System.out.println("Multiple ledgers mixed cluster: " + numLedgers + " ledgers OK");
    }

    /**
     * Test 3: Recovery in mixed cluster — write without closing, then recover.
     *
     * <p>Writes entries to a 2-bookie ensemble without closing. A new client
     * opens with recovery, which fences both bookies and reads all entries.
     */
    @Test
    void testRecoveryMixedCluster() throws Exception {
        final int numEntries = 20;

        LedgerHandle writer = bk.createLedger(2, 2, 2, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = writer.getId();
        System.out.println("Recovery test: ledger " + ledgerId);

        for (int i = 0; i < numEntries; i++) {
            writer.addEntry(("recovery-" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(numEntries - 1, writer.getLastAddConfirmed());
        // Do NOT close — simulate crash

        // New client opens with recovery
        ClientConfiguration conf2 = new ClientConfiguration();
        conf2.setMetadataServiceUri("zk+hierarchical://" + zkConnect + "/ledgers");
        try (BookKeeper bk2 = new BookKeeper(conf2)) {
            LedgerHandle recovered = bk2.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
            long lac = recovered.getLastAddConfirmed();
            assertTrue(lac >= numEntries - 1,
                    "Recovered LAC should be >= " + (numEntries - 1) + ", got " + lac);

            Enumeration<LedgerEntry> entries = recovered.readEntries(0, lac);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                assertEquals("recovery-" + count,
                        new String(entry.getEntry(), StandardCharsets.UTF_8));
                count++;
            }
            assertTrue(count >= numEntries);
            recovered.close();
        }
        try { writer.close(); } catch (Exception ignored) {}
        System.out.println("Recovery in mixed cluster: " + numEntries + " entries recovered OK");
    }

    /**
     * Test 4: All digest types in mixed cluster.
     *
     * <p>Tests CRC32, CRC32C, MAC, and DUMMY digests with 2-bookie ensembles.
     */
    @Test
    void testAllDigestTypesMixedCluster() throws Exception {
        BookKeeper.DigestType[] types = {
            BookKeeper.DigestType.CRC32,
            BookKeeper.DigestType.CRC32C,
            BookKeeper.DigestType.MAC,
            BookKeeper.DigestType.DUMMY,
        };

        for (BookKeeper.DigestType dt : types) {
            LedgerHandle lh = bk.createLedger(2, 2, 2, dt, PASSWORD);
            long ledgerId = lh.getId();

            for (int i = 0; i < 10; i++) {
                lh.addEntry((dt.name() + "-" + i).getBytes(StandardCharsets.UTF_8));
            }
            lh.close();

            LedgerHandle readLh = bk.openLedger(ledgerId, dt, PASSWORD);
            Enumeration<LedgerEntry> entries = readLh.readEntries(0, 9);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                assertEquals(dt.name() + "-" + count,
                        new String(entry.getEntry(), StandardCharsets.UTF_8));
                count++;
            }
            assertEquals(10, count, "Digest type " + dt + " entry count mismatch");
            readLh.close();
        }
        System.out.println("All digest types in mixed cluster: OK");
    }

    /**
     * Test 5: Large entries in mixed cluster.
     *
     * <p>Writes 64KB entries across both bookies to test payload handling.
     */
    @Test
    void testLargeEntriesMixedCluster() throws Exception {
        LedgerHandle lh = bk.createLedger(2, 2, 2, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();

        byte[] largeEntry = new byte[64 * 1024];
        java.util.Arrays.fill(largeEntry, (byte) 0xAB);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(largeEntry);
        }
        lh.close();

        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(9, readLh.getLastAddConfirmed());

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, 9);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertArrayEquals(largeEntry, entry.getEntry(),
                    "Large entry " + count + " mismatch");
            count++;
        }
        assertEquals(10, count);
        readLh.close();
        System.out.println("Large entries in mixed cluster: 10 x 64KB OK");
    }
}
