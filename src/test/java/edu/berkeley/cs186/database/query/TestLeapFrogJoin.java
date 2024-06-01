package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj3Part1Tests;
import edu.berkeley.cs186.database.categories.Proj3Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.join.LeapfrogOperator;
import edu.berkeley.cs186.database.table.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class TestLeapFrogJoin {
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
    @Category(PublicTests.class)
    public void testSimpleLeapFrogJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createIncreasingSourceWithAllTypes(100),
                    TestUtils.createIncreasingSourceWithAllTypes(100),
                    transaction
            );

            JoinOperator joinOperator = new LeapfrogOperator(
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
    @Category(PublicTests.class)
    // The point of using a source with increasing gaps is to test out the iterators' seek function
    public void testIncreasingGapSourceLeapFrogJoin() {
        d.setWorkMem(5); // B=5

        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createIncreasingJumpSourceWithInts(50, 3),
                    TestUtils.createIncreasingJumpSourceWithInts(25, 9),
                    transaction
            );
            List<Integer> numbersInSource1 = new ArrayList<>();
            List<Integer> numbersInSource2 = new ArrayList<>();
            List<Integer> expectedValues = new ArrayList<>();

            for (int i = 1; i <= 50 * 3; i+= 3) numbersInSource1.add(i);
            for (int i = 1; i <= 25 * 9; i+= 9) numbersInSource2.add(i);

            for (int i = 0; i < numbersInSource1.size(); i++) {
                for (int j = 0; j < numbersInSource2.size(); j++) {
                    if (numbersInSource1.get(i).equals(numbersInSource2.get(j)) ) {
                        expectedValues.add(numbersInSource1.get(i));
                    }
                }
            }

            JoinOperator joinOperator = new LeapfrogOperator(
                    leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();

            int numRecords = 0;
            Record record1;
            Record record2;
            Record expected;

            while (outputIterator.hasNext() && numRecords < expectedValues.size()) {
                record1 = new Record(expectedValues.get(numRecords));
                record2 = new Record(expectedValues.get(numRecords));
                expected = record1.concat(record2);
                assertEquals("mismatch at record " + numRecords, expected, outputIterator.next());
                numRecords++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            outputIterator.hasNext();
            assertEquals("too few records", expectedValues.size(), numRecords);
        }
    }

    @Test
    public void testEmptyWithEmptyLeapFrogJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    transaction
            );
            startCountIOs();

            JoinOperator joinOperator = new LeapfrogOperator(
                    leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    public void testNonEmptyWithEmptyLeapFrogJoin() {
        // Joins a non-empty table with an empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithAllTypes(100),
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    transaction
            );
            startCountIOs();
            JoinOperator joinOperator = new LeapfrogOperator(leftSourceOperator, rightSourceOperator,
                    "int", "int", transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    public void testEmptyWithNonEmptyLeapFrogJoin() {
        // Joins an empty table with a non-empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    TestUtils.createSourceWithAllTypes(100),
                    transaction
            );
            startCountIOs();
            JoinOperator joinOperator = new LeapfrogOperator(leftSourceOperator, rightSourceOperator,
                    "int", "int", transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    @Category(PublicTests.class)
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

            JoinOperator joinOperator = new LeapfrogOperator(leftSourceOperator, rightSourceOperator, "int",
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