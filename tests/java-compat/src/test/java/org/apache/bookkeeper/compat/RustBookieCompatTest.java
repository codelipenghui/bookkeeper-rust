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
}
