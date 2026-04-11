// BkUpgradeHelper — sidecar CLI used by the Rust upgrade test.
//
// Runs inside a container on the test's Docker network, so it can reach the
// bookies by their in-network DNS names. The Rust test shells out via
// `docker run` and parses the stdout.
//
// Usage:
//   java BkUpgradeHelper write --zk <addr> --label <N> --entries <N> --era <s>
//       → prints "ledger_id=<id>" on stdout on success, exit 0
//   java BkUpgradeHelper read  --zk <addr> --ledger-id <id> --label <N>
//                              --entries <N> --era <s>
//       → prints "verified=<count>" on stdout on success, exit 0
//
// Deterministic payload (must match Rust's `payload_for`):
//   "ledger_label=<label> entry=<entry_id> era=<era>"

import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

public class BkUpgradeHelper {
    private static final byte[] PASSWORD = "upgrade-test".getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: BkUpgradeHelper <write|read> [--zk <addr>] ...");
            System.exit(2);
        }
        Map<String, String> kv = parseArgs(args, 1);
        String cmd = args[0];
        switch (cmd) {
            case "write":
                doWrite(kv);
                break;
            case "read":
                doRead(kv);
                break;
            default:
                System.err.println("Unknown subcommand: " + cmd);
                System.exit(2);
        }
    }

    private static Map<String, String> parseArgs(String[] args, int start) {
        Map<String, String> kv = new HashMap<>();
        for (int i = start; i < args.length - 1; i += 2) {
            String k = args[i];
            String v = args[i + 1];
            if (!k.startsWith("--")) {
                System.err.println("Bad flag: " + k);
                System.exit(2);
            }
            kv.put(k.substring(2), v);
        }
        return kv;
    }

    private static byte[] payloadFor(long label, long entryId, String era) {
        String s = "ledger_label=" + label + " entry=" + entryId + " era=" + era;
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static BookKeeper openClient(Map<String, String> kv) throws Exception {
        String zk = require(kv, "zk");
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk://" + zk + "/ledgers");
        conf.setAddEntryTimeout(30);
        conf.setReadEntryTimeout(30);
        return new BookKeeper(conf);
    }

    private static String require(Map<String, String> kv, String key) {
        String v = kv.get(key);
        if (v == null) {
            System.err.println("Missing --" + key);
            System.exit(2);
        }
        return v;
    }

    private static void doWrite(Map<String, String> kv) throws Exception {
        long label = Long.parseLong(require(kv, "label"));
        long n = Long.parseLong(require(kv, "entries"));
        String era = require(kv, "era");

        BookKeeper bk = openClient(kv);
        try {
            LedgerHandle lh = bk.createLedger(2, 2, 2,
                    DigestType.CRC32C, PASSWORD);
            long ledgerId = lh.getId();
            System.err.println("Created ledger " + ledgerId + " (label=" + label + ")");
            for (long i = 0; i < n; i++) {
                lh.addEntry(payloadFor(label, i, era));
            }
            lh.close();
            System.err.println("Wrote " + n + " entries to ledger " + ledgerId + " and closed");
            // Machine-readable stdout:
            System.out.println("ledger_id=" + ledgerId);
        } finally {
            bk.close();
        }
    }

    private static void doRead(Map<String, String> kv) throws Exception {
        long ledgerId = Long.parseLong(require(kv, "ledger-id"));
        long label = Long.parseLong(require(kv, "label"));
        long n = Long.parseLong(require(kv, "entries"));
        String era = require(kv, "era");

        BookKeeper bk = openClient(kv);
        try {
            LedgerHandle lh = bk.openLedger(ledgerId, DigestType.CRC32C, PASSWORD);
            long count = 0;
            if (n > 0) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, n - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    byte[] actual = e.getEntry();
                    byte[] expected = payloadFor(label, e.getEntryId(), era);
                    if (!java.util.Arrays.equals(actual, expected)) {
                        System.err.println("MISMATCH at ledger=" + ledgerId
                                + " entry=" + e.getEntryId()
                                + "\n  expected: " + new String(expected, StandardCharsets.UTF_8)
                                + "\n  actual:   " + new String(actual, StandardCharsets.UTF_8));
                        System.exit(1);
                    }
                    count++;
                }
            }
            lh.close();
            System.err.println("Read-verified " + count + " entries from ledger " + ledgerId);
            System.out.println("verified=" + count);
        } finally {
            bk.close();
        }
    }
}
