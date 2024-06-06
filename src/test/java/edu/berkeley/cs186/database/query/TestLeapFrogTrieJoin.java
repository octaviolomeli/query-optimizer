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
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("leapfrogtrieTest");
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
    public void testAllMatchesLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createIncreasingSourceWith3IntFields(25),
                    TestUtils.createIncreasingSourceWith3IntFields(25),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "field1", "field1",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();

            int numRecords = 0;

            Record expectedRecord = new Record(0, 0, 0).concat(new Record(0, 0, 0));

            while (numRecords < 25 && outputIterator.hasNext()) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
                expectedRecord = new Record(numRecords, numRecords, numRecords)
                        .concat(new Record(numRecords, numRecords, numRecords));
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 25, numRecords);
        }
    }

    @Test
    public void testSomeMatchesLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createIncreasingSourceWith3IntFields(20, 0),
                    TestUtils.createIncreasingSourceWith3IntFields(20, 10),
                    transaction
            );

            JoinOperator joinOperator = new LFTJOperator(
                    leftSourceOperator, rightSourceOperator, "field1", "field1",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            int numRecords = 0;

            Record expectedRecord = new Record(10, 10, 10).concat(new Record(10, 10, 10));

            while (numRecords < 10 && outputIterator.hasNext()) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
                expectedRecord = new Record(numRecords + 10, numRecords + 10, numRecords + 10)
                        .concat(new Record(numRecords + 10, numRecords + 10, numRecords + 10));
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 10, numRecords);
        }
    }

    @Test
    public void testEmptyWithEmptyLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createEmptySourceWith3IntFields(),
                    TestUtils.createEmptySourceWith3IntFields(),
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
            setSourceOperators(
                    TestUtils.createIncreasingSourceWith3IntFields(10),
                    TestUtils.createEmptySourceWith3IntFields(),
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
            setSourceOperators(
                    TestUtils.createEmptySourceWith3IntFields(),
                    TestUtils.createIncreasingSourceWith3IntFields(10),
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
    public void testNoMatchesLeapFrogTrieJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createIncreasingSourceWith3IntFields(10, 0),
                    TestUtils.createIncreasingSourceWith3IntFields(10, 25),
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
}