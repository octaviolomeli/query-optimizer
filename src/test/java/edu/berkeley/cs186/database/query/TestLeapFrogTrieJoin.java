package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.join.LFTJOperator;
import edu.berkeley.cs186.database.query.join.LFTJOperator;
import edu.berkeley.cs186.database.table.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TestLeapFrogTrieJoin {
    private Database d;
    private long numIOs;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("leapfrogTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(5); // B=5
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) p.unpin();
        d.close();
    }

    // 4 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            4000 * TimeoutScaling.factor)));

    private void startCountIOs() {
        d.getBufferManager().evictAll();
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void checkIOs(String message, long minIOs, long maxIOs) {
        if (message == null) message = "";
        else message = "(" + message + ")";
        long newIOs = d.getBufferManager().getNumIOs();
        long IOs = newIOs - numIOs;
        assertTrue(IOs + " I/Os not between " + minIOs + " and " + maxIOs + message,
                minIOs <= IOs && IOs <= maxIOs);
        numIOs = newIOs;
    }

    private void checkIOs(long numIOs) {
        checkIOs(null, numIOs, numIOs);
    }

    private void setSourceOperators(TestSourceOperator leftSourceOperator,
                                    TestSourceOperator rightSourceOperator, Transaction transaction) {
        setSourceOperators(
                new MaterializeOperator(leftSourceOperator, transaction.getTransactionContext()),
                new MaterializeOperator(rightSourceOperator, transaction.getTransactionContext())
        );
    }

    private void pinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        Page page = d.getBufferManager().fetchPage(new DummyLockContext(), pnum);
        this.pinnedPages.put(pnum, page);
    }

    private void setSourceOperators(QueryOperator leftSourceOperator,
                                    QueryOperator rightSourceOperator) {
        assert (this.leftSourceOperator == null && this.rightSourceOperator == null);

        this.leftSourceOperator = leftSourceOperator;
        this.rightSourceOperator = rightSourceOperator;

        // hard-coded mess, but works as long as the first two tables created are the source operators
        pinPage(1, 0); // _metadata.tables header page
        pinPage(2, 0); // _metadata.indices header page
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    @Test
    public void testSimpleLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createIncreasingSourceWithAllTypes(100),
                    TestUtils.createIncreasingSourceWithAllTypes(100),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();

            int numRecords = 0;
            Record record1 = TestUtils.createRecordWithAllTypesWithValue(1);
            Record record2 = TestUtils.createRecordWithAllTypesWithValue(1);
            Record expected = record1.concat(record2);

            while (outputIterator.hasNext() && numRecords < 100) {
                assertEquals("mismatch at record " + numRecords, expected, outputIterator.next());
                numRecords++;
                record1 = TestUtils.createRecordWithAllTypesWithValue(numRecords + 1);
                record2 = TestUtils.createRecordWithAllTypesWithValue(numRecords + 1);
                expected = record1.concat(record2);
            }

            assertFalse("too many records", outputIterator.hasNext());
            outputIterator.hasNext();
            assertEquals("too few records", 100, numRecords);
        }
    }

    @Test
    public void testEmptyWithEmptyLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            ArrayList<DataBox> source1 = new ArrayList<>();
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));

            setSourceOperators(
                    TestUtils.createEmptySourceWith3Fields(source1),
                    TestUtils.createEmptySourceWith3Fields(source1),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "field1", "field1",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    public void testNonEmptyWithEmptyLeapFrogTrieJoin() {
        // Joins a non-empty table with an empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            ArrayList<DataBox> source1 = new ArrayList<>();
            ArrayList<DataBox> source2 = new ArrayList<>();
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source2.add(new IntDataBox(1));
            source2.add(new IntDataBox(1));
            source2.add(new IntDataBox(1));

            setSourceOperators(
                    TestUtils.createSourceWithSame3Values(source2, 10),
                    TestUtils.createEmptySourceWith3Fields(source1),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "field1", "field1",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    public void testEmptyWithNonEmptyLeapFrogTrieJoin() {
        // Joins an empty table with a non-empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            ArrayList<DataBox> source1 = new ArrayList<>();
            ArrayList<DataBox> source2 = new ArrayList<>();
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source2.add(new IntDataBox(1));
            source2.add(new IntDataBox(1));
            source2.add(new IntDataBox(1));

            setSourceOperators(
                    TestUtils.createEmptySourceWith3Fields(source1),
                    TestUtils.createSourceWithSame3Values(source2, 10),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "field1", "field1",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    public void testLeapFrogUnsortedInputs()  {
        d.setWorkMem(3); // B=3
        try(Transaction transaction = d.beginTransaction()) {
            // Create random order of [1, ..., 800]
            List<Integer> numbers = new ArrayList<>();
            for (int i = 1; i <= 800; i++) {
                numbers.add(i);
            }
            Collections.shuffle(numbers);

            setSourceOperators(
                    TestUtils.createSourceWithInts(numbers),
                    TestUtils.createSourceWithInts(numbers),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(leftSourceOperator, rightSourceOperator, "int",
                    "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();

            int numRecords = 0;
            Record record1 = new Record(1);
            Record record2 = new Record(1);
            Record expected = record1.concat(record2);

            while (outputIterator.hasNext() && numRecords < 800) {
                assertEquals("mismatch at record " + numRecords, expected, outputIterator.next());
                numRecords++;
                record1 = new Record(numRecords + 1);
                record2 = new Record(numRecords + 1);
                expected = record1.concat(record2);
            }
            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 800, numRecords);
        }
    }

    @Test
    public void testNoMatchesLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            ArrayList<DataBox> source1 = new ArrayList<>();
            ArrayList<DataBox> source2 = new ArrayList<>();
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source1.add(new IntDataBox(1));
            source2.add(new IntDataBox(3));
            source2.add(new IntDataBox(3));
            source2.add(new IntDataBox(3));

            setSourceOperators(
                    // First source has 3 fields of value 1 for each record
                    TestUtils.createSourceWithSame3Values(source1, 25),
                    // Second source has 3 fields of value 3 for each record
                    TestUtils.createSourceWithSame3Values(source2, 25),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "field1", "field1",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }
}