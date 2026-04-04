package org.apache.bookkeeper.compat;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;

import org.apache.bookkeeper.client.LedgerEntry;

import org.apache.bookkeeper.client.BKException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Java client compatibility tests for the Rust bookie implementation.
 *
 * <p>These tests verify wire-level compatibility between the official Apache BookKeeper
 * Java client and the Rust bookie server across multiple protocol variants and use cases.
 *
 * <p>Prerequisites before running:
 * <ul>
 *   <li>ZooKeeper running (address passed via -Dzk.connect=host:port, default localhost:2181)</li>
 *   <li>BookKeeper cluster initialized: {@code bk initnewcluster --zk localhost:2181}</li>
 *   <li>Rust bookie started and registered with ZooKeeper</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RustBookieCompatTest {

    private static final byte[] PASSWORD = "test-password".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY_PASSWORD = new byte[0];

    private static String zkConnect;

    private BookKeeper bk;

    @BeforeAll
    static void readConfig() {
        zkConnect = System.getProperty("zk.connect", "localhost:2181");
        System.out.println("[RustBookieCompatTest] Using ZooKeeper at: " + zkConnect);
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
     * Test 1: V3 protocol — create ledger, write 10 entries, read them back, close.
     *
     * <p>Uses CRC32C digest (default) over the V3 protobuf wire protocol.
     */
    @Test
    void testV3CreateWriteReadClose() throws Exception {
        final int numEntries = 10;

        // Create ledger with CRC32C digest
        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();

        // Write 10 entries
        for (int i = 0; i < numEntries; i++) {
            byte[] data = ("v3-entry-" + i).getBytes(StandardCharsets.UTF_8);
            lh.addEntry(data);
        }

        lh.close();

        // Open and read back
        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed(),
                "LAC should be " + (numEntries - 1));

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            byte[] data = entry.getEntry();
            String expected = "v3-entry-" + count;
            assertEquals(expected, new String(data, StandardCharsets.UTF_8),
                    "Entry " + count + " content mismatch");
            count++;
        }
        assertEquals(numEntries, count, "Should have read exactly " + numEntries + " entries");

        readLh.close();
    }

    /**
     * Test 2: V2 wire protocol — create ledger with useV2WireProtocol=true, write 5 entries, read back.
     *
     * <p>Forces the Java client to use the older V2 binary framing instead of V3 protobuf.
     */
    @Test
    void testV2WireProtocol() throws Exception {
        final int numEntries = 5;

        ClientConfiguration v2Conf = new ClientConfiguration();
        v2Conf.setMetadataServiceUri("zk+hierarchical://" + zkConnect + "/ledgers");
        v2Conf.setUseV2WireProtocol(true);

        try (BookKeeper v2Bk = new BookKeeper(v2Conf)) {
            LedgerHandle lh = v2Bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
            long ledgerId = lh.getId();

            for (int i = 0; i < numEntries; i++) {
                byte[] data = ("v2-entry-" + i).getBytes(StandardCharsets.UTF_8);
                lh.addEntry(data);
            }
            lh.close();

            LedgerHandle readLh = v2Bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
            assertEquals(numEntries - 1, readLh.getLastAddConfirmed());

            Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                String expected = "v2-entry-" + count;
                assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8));
                count++;
            }
            assertEquals(numEntries, count);
            readLh.close();
        }
    }

    /**
     * Test 3: Multiple digest types — verifies CRC32, CRC32C, and MAC digests all work.
     *
     * <p>Writes 5 entries with each digest type and reads them back to confirm correctness.
     */
    @Test
    void testMultipleDigestTypes() throws Exception {
        final int numEntries = 5;

        BookKeeper.DigestType[] digestTypes = {
            BookKeeper.DigestType.CRC32,
            BookKeeper.DigestType.CRC32C,
            BookKeeper.DigestType.MAC
        };

        for (BookKeeper.DigestType digestType : digestTypes) {
            LedgerHandle lh = bk.createLedger(1, 1, 1, digestType, PASSWORD);
            long ledgerId = lh.getId();

            for (int i = 0; i < numEntries; i++) {
                byte[] data = (digestType.name() + "-entry-" + i).getBytes(StandardCharsets.UTF_8);
                lh.addEntry(data);
            }
            lh.close();

            LedgerHandle readLh = bk.openLedger(ledgerId, digestType, PASSWORD);
            assertEquals(numEntries - 1, readLh.getLastAddConfirmed(),
                    "LAC mismatch for digest type: " + digestType);

            Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                String expected = digestType.name() + "-entry-" + count;
                assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8),
                        "Entry mismatch for digest type: " + digestType + ", entry: " + count);
                count++;
            }
            assertEquals(numEntries, count, "Entry count mismatch for digest type: " + digestType);
            readLh.close();
        }
    }

    /**
     * Test 4: Fencing — a second openLedger (with recovery) fences the original writer.
     *
     * <p>Verifies that after fencing, the original writer receives a fencing exception
     * and cannot write further entries.
     */
    @Test
    void testFencing() throws Exception {
        // Write a few entries without closing
        LedgerHandle writer = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = writer.getId();

        writer.addEntry("pre-fence-entry-0".getBytes(StandardCharsets.UTF_8));
        writer.addEntry("pre-fence-entry-1".getBytes(StandardCharsets.UTF_8));

        // Open with recovery — this fences the ledger
        LedgerHandle fencer = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);

        // Original writer should now fail on add
        assertThrows(Exception.class, () -> {
            writer.addEntry("post-fence-entry".getBytes(StandardCharsets.UTF_8));
        }, "Write after fencing should throw an exception");

        // Fenced reader should be able to read the pre-fence entries
        long lac = fencer.getLastAddConfirmed();
        assertTrue(lac >= 1, "LAC should be at least 1 after fencing; got " + lac);

        Enumeration<LedgerEntry> entries = fencer.readEntries(0, lac);
        int count = 0;
        while (entries.hasMoreElements()) {
            entries.nextElement();
            count++;
        }
        assertEquals(lac + 1, count, "Should read LAC+1 entries");

        fencer.close();
        // writer.close() may throw; ignore
        try { writer.close(); } catch (Exception ignored) {}
    }

    /**
     * Test 5: Large entries — writes and reads back a 64 KB and a 256 KB entry.
     *
     * <p>Verifies that the Rust bookie correctly handles large payloads without
     * data corruption or truncation.
     */
    @Test
    void testLargeEntries() throws Exception {
        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();

        // 64 KB entry
        byte[] entry64k = new byte[64 * 1024];
        Arrays.fill(entry64k, (byte) 0xAB);
        lh.addEntry(entry64k);

        // 256 KB entry
        byte[] entry256k = new byte[256 * 1024];
        Arrays.fill(entry256k, (byte) 0xCD);
        lh.addEntry(entry256k);

        lh.close();

        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(1, readLh.getLastAddConfirmed(), "LAC should be 1 (two entries: 0 and 1)");

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, 1);

        LedgerEntry e0 = entries.nextElement();
        assertArrayEquals(entry64k, e0.getEntry(), "64KB entry content mismatch");

        LedgerEntry e1 = entries.nextElement();
        assertArrayEquals(entry256k, e1.getEntry(), "256KB entry content mismatch");

        readLh.close();
    }

    /**
     * Test 6: Empty password — creates a ledger with a zero-length password.
     *
     * <p>The BookKeeper protocol uses the password (via HMAC) as a ledger access key.
     * An empty password should be valid and fully functional.
     */
    @Test
    void testEmptyPassword() throws Exception {
        final int numEntries = 3;

        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, EMPTY_PASSWORD);
        long ledgerId = lh.getId();

        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("empty-pwd-" + i).getBytes(StandardCharsets.UTF_8));
        }
        lh.close();

        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, EMPTY_PASSWORD);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed());

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String expected = "empty-pwd-" + count;
            assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8));
            count++;
        }
        assertEquals(numEntries, count);
        readLh.close();
    }

    /**
     * Test 7: Many entries — writes 1000 entries sequentially and reads them all back.
     *
     * <p>Stress-tests the bookie's storage layer and verifies there is no data loss
     * or ordering issue under a realistic write-heavy workload.
     */
    @Test
    void testManyEntries() throws Exception {
        final int numEntries = 1000;

        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();

        for (int i = 0; i < numEntries; i++) {
            byte[] data = ("entry-" + i).getBytes(StandardCharsets.UTF_8);
            lh.addEntry(data);
        }
        lh.close();

        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed(),
                "LAC should be " + (numEntries - 1));

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String expected = "entry-" + count;
            assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8),
                    "Entry " + count + " content mismatch");
            count++;
        }
        assertEquals(numEntries, count, "Should have read all " + numEntries + " entries");

        readLh.close();
    }

    // -----------------------------------------------------------------------
    // New compatibility tests
    // -----------------------------------------------------------------------

    /**
     * Test 8: Ledger recovery — write entries without closing, then open with recovery.
     *
     * <p>Simulates a writer crash by not calling close(). The recovery open should
     * fence the ledger, discover all written entries, and seal the ledger.
     */
    @Test
    void testLedgerRecovery() throws Exception {
        final int numEntries = 20;

        // Create and write without closing (simulate crash)
        LedgerHandle writer = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = writer.getId();

        for (int i = 0; i < numEntries; i++) {
            writer.addEntry(("recovery-" + i).getBytes(StandardCharsets.UTF_8));
        }
        long writerLac = writer.getLastAddConfirmed();
        assertEquals(numEntries - 1, writerLac, "Writer LAC should be " + (numEntries - 1));
        // Do NOT call writer.close() — simulating crash

        // New client opens with recovery
        ClientConfiguration conf2 = new ClientConfiguration();
        conf2.setMetadataServiceUri("zk+hierarchical://" + zkConnect + "/ledgers");
        try (BookKeeper bk2 = new BookKeeper(conf2)) {
            LedgerHandle recovered = bk2.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
            long recoveredLac = recovered.getLastAddConfirmed();
            assertTrue(recoveredLac >= numEntries - 1,
                    "Recovered LAC should be >= " + (numEntries - 1) + ", got " + recoveredLac);

            // Read all entries
            Enumeration<LedgerEntry> entries = recovered.readEntries(0, recoveredLac);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                String expected = "recovery-" + count;
                assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8),
                        "Recovery entry " + count + " mismatch");
                count++;
            }
            assertTrue(count >= numEntries,
                    "Should recover at least " + numEntries + " entries, got " + count);
            recovered.close();
        }

        try { writer.close(); } catch (Exception ignored) {}
    }

    /**
     * Test 9: Ledger deletion — create, write, close, delete, verify it's gone.
     *
     * <p>After deletion, attempting to open the ledger should fail with
     * NoSuchLedgerExistsOnMetadataServerException.
     */
    @Test
    void testLedgerDeletion() throws Exception {
        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();

        lh.addEntry("delete-me".getBytes(StandardCharsets.UTF_8));
        lh.close();

        // Verify ledger exists
        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(0, readLh.getLastAddConfirmed());
        readLh.close();

        // Delete
        bk.deleteLedger(ledgerId);

        // Verify ledger is gone
        assertThrows(BKException.BKNoSuchLedgerExistsOnMetadataServerException.class, () -> {
            bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        }, "Opening a deleted ledger should throw");
    }

    /**
     * Test 10: readLastConfirmed — verify LAC is correctly reported by the bookie.
     *
     * <p>Creates a ledger, writes entries one at a time, and after each write
     * verifies the LAC reflects the last confirmed entry.
     */
    @Test
    void testReadLastConfirmed() throws Exception {
        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
        long ledgerId = lh.getId();

        // LAC starts at -1 (no entries written)
        assertEquals(-1, lh.getLastAddConfirmed(), "Initial LAC should be -1");

        // Write entries and check LAC progresses
        for (int i = 0; i < 5; i++) {
            lh.addEntry(("lac-" + i).getBytes(StandardCharsets.UTF_8));
            assertEquals(i, lh.getLastAddConfirmed(),
                    "LAC should be " + i + " after writing entry " + i);
        }

        lh.close();

        // Re-open and verify LAC matches
        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(4, readLh.getLastAddConfirmed(), "LAC should be 4 after re-open");
        readLh.close();
    }

    /**
     * Test 11: Wrong password rejection — opening with the wrong password should fail.
     *
     * <p>Verifies the bookie correctly validates the master key derived from the password.
     */
    @Test
    void testWrongPasswordRejected() throws Exception {
        byte[] correctPassword = "correct-horse-battery-staple".getBytes(StandardCharsets.UTF_8);
        byte[] wrongPassword = "wrong-password".getBytes(StandardCharsets.UTF_8);

        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, correctPassword);
        long ledgerId = lh.getId();
        lh.addEntry("secret-data".getBytes(StandardCharsets.UTF_8));
        lh.close();

        // Opening with wrong password should fail
        assertThrows(BKException.class, () -> {
            bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, wrongPassword);
        }, "Opening with wrong password should throw BKException");
    }

    /**
     * Test 12: Multiple concurrent ledgers — create several ledgers and write to them
     * in interleaved fashion, then read each one back.
     *
     * <p>Verifies there is no cross-contamination between ledgers on the same bookie.
     */
    @Test
    void testMultipleConcurrentLedgers() throws Exception {
        final int numLedgers = 5;
        final int entriesPerLedger = 10;
        long[] ledgerIds = new long[numLedgers];
        LedgerHandle[] handles = new LedgerHandle[numLedgers];

        // Create all ledgers
        for (int l = 0; l < numLedgers; l++) {
            handles[l] = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
            ledgerIds[l] = handles[l].getId();
        }

        // Write entries in round-robin across ledgers
        for (int i = 0; i < entriesPerLedger; i++) {
            for (int l = 0; l < numLedgers; l++) {
                byte[] data = ("L" + l + "-E" + i).getBytes(StandardCharsets.UTF_8);
                handles[l].addEntry(data);
            }
        }

        // Close all
        for (int l = 0; l < numLedgers; l++) {
            handles[l].close();
        }

        // Read each ledger back and verify isolation
        for (int l = 0; l < numLedgers; l++) {
            LedgerHandle readLh = bk.openLedger(ledgerIds[l], BookKeeper.DigestType.CRC32C, PASSWORD);
            assertEquals(entriesPerLedger - 1, readLh.getLastAddConfirmed(),
                    "Ledger " + l + " LAC mismatch");

            Enumeration<LedgerEntry> entries = readLh.readEntries(0, entriesPerLedger - 1);
            int count = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                String expected = "L" + l + "-E" + count;
                assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8),
                        "Ledger " + l + " entry " + count + " mismatch");
                count++;
            }
            assertEquals(entriesPerLedger, count);
            readLh.close();
        }
    }

    /**
     * Test 13: V2 write → V3 read cross-protocol — write using V2, read using V3 (default).
     *
     * <p>Verifies data written via V2 binary protocol is correctly readable via V3 protobuf.
     * This tests the bookie's storage layer consistency across protocol versions.
     */
    @Test
    void testV2WriteV3Read() throws Exception {
        final int numEntries = 5;

        // Write using V2 protocol
        ClientConfiguration v2Conf = new ClientConfiguration();
        v2Conf.setMetadataServiceUri("zk+hierarchical://" + zkConnect + "/ledgers");
        v2Conf.setUseV2WireProtocol(true);

        long ledgerId;
        try (BookKeeper v2Bk = new BookKeeper(v2Conf)) {
            LedgerHandle lh = v2Bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, PASSWORD);
            ledgerId = lh.getId();

            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(("cross-" + i).getBytes(StandardCharsets.UTF_8));
            }
            lh.close();
        }

        // Read using V3 protocol (default)
        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWORD);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed());

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String expected = "cross-" + count;
            assertEquals(expected, new String(entry.getEntry(), StandardCharsets.UTF_8),
                    "Cross-protocol entry " + count + " mismatch");
            count++;
        }
        assertEquals(numEntries, count);
        readLh.close();
    }

    /**
     * Test 14: DUMMY digest — verifies ledger creation and I/O with no checksum.
     *
     * <p>DUMMY digest skips checksum computation, useful for performance benchmarks.
     * Tests that the Rust bookie handles this correctly.
     */
    @Test
    void testDummyDigest() throws Exception {
        final int numEntries = 5;

        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.DUMMY, PASSWORD);
        long ledgerId = lh.getId();

        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("dummy-" + i).getBytes(StandardCharsets.UTF_8));
        }
        lh.close();

        LedgerHandle readLh = bk.openLedger(ledgerId, BookKeeper.DigestType.DUMMY, PASSWORD);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed());

        Enumeration<LedgerEntry> entries = readLh.readEntries(0, numEntries - 1);
        int count = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("dummy-" + count, new String(entry.getEntry(), StandardCharsets.UTF_8));
            count++;
        }
        assertEquals(numEntries, count);
        readLh.close();
    }
}
