package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
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

public class TestIndexNestedLoopJoin {
    private Database d;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private final Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("inljTest");
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
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    @Test
    public void testAllMatchesINLJ() {
        // Simulates joining two tables, each containing 100 identical records,
        // joined on the column "int". Since all records are identical we expect
        // expect exactly 100 x 100 = 10000 records to be yielded.
        // Both tables consist of a single page.
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");
            List<Integer> ids = new ArrayList<>();
            for (int i = 0; i < 100; i++) ids.add(i);
            Collections.shuffle(ids);
            for (int i = 0; i < 100; i++) {
                transaction1.insert("table1", i, "Jane", "Doe");
                transaction1.insert("table2", ids.get(i), "Jane", "Doe");
            }

            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();

            int numRecords = 0;
            Record expectedRecord = new Record(0, "Jane", "Doe", 0, "Jane", "Doe");

            while (iter.hasNext() && numRecords < 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, iter.next());
                numRecords++;
                expectedRecord = new Record(numRecords, "Jane", "Doe", numRecords, "Jane", "Doe");
            }

            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 100, numRecords);
        }
    }

    @Test
    public void testAllMatchesSameKeyINLJ() {
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("a", Type.intType())
                    .add("b", Type.intType());
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");
            // Table 1
            transaction1.insert("table1", 1, 2, 3);
            transaction1.insert("table1", 1, 5, 5);
            transaction1.insert("table1", 1, 4, 4);
            transaction1.insert("table1", 1, 9, 9);
            // Table 2
            transaction1.insert("table2", 1, 2, 3);
            transaction1.insert("table2", 1, 2, 2);
            transaction1.insert("table2", 1, 1, 1);

            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            List<Record> expectedValues = new ArrayList<>();
            expectedValues.add((new Record(1, 2, 3)).concat(new Record(1, 1, 1)));
            expectedValues.add((new Record(1, 2, 3)).concat(new Record(1, 2, 2)));
            expectedValues.add((new Record(1, 2, 3)).concat(new Record(1, 2, 3)));

            expectedValues.add((new Record(1, 5, 5)).concat(new Record(1, 1, 1)));
            expectedValues.add((new Record(1, 5, 5)).concat(new Record(1, 2, 2)));
            expectedValues.add((new Record(1, 5, 5)).concat(new Record(1, 2, 3)));

            expectedValues.add((new Record(1, 4, 4)).concat(new Record(1, 1, 1)));
            expectedValues.add((new Record(1, 4, 4)).concat(new Record(1, 2, 2)));
            expectedValues.add((new Record(1, 4, 4)).concat(new Record(1, 2, 3)));

            expectedValues.add((new Record(1, 9, 9)).concat(new Record(1, 1, 1)));
            expectedValues.add((new Record(1, 9, 9)).concat(new Record(1, 2, 2)));
            expectedValues.add((new Record(1, 9, 9)).concat(new Record(1, 2, 3)));

            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();

            int numRecords = 0;

            while (iter.hasNext() && numRecords < expectedValues.size()) {
                assertEquals("mismatch at record " + numRecords, expectedValues.get(numRecords), iter.next());
                numRecords++;
            }

            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", expectedValues.size(), numRecords);
        }
    }

    @Test
    // The point of using a source with increasing gaps is to test out the iterators' seek function
    public void testSomeMatchesINLJ() {
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");

            List<Integer> numbersInSource1 = new ArrayList<>();
            List<Integer> numbersInSource2 = new ArrayList<>();

            for (int i = 1; i <= 50 * 3; i+= 3) numbersInSource1.add(i);
            for (int i = 1; i <= 25 * 9; i+= 9) numbersInSource2.add(i);

            Collections.shuffle(numbersInSource2);
            for (Integer integer : numbersInSource1) {
                transaction1.insert("table1", integer, "Jane", "Doe");
            }

            for (Integer integer : numbersInSource2) {
                transaction1.insert("table2", integer, "Jane", "Doe");
            }

            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            List<Integer> numbersInSource1 = new ArrayList<>();
            List<Integer> numbersInSource2 = new ArrayList<>();
            List<Integer> expectedValues = new ArrayList<>();

            for (int i = 1; i <= 50 * 3; i+= 3) numbersInSource1.add(i);
            for (int i = 1; i <= 25 * 9; i+= 9) numbersInSource2.add(i);

            for (Integer integer : numbersInSource1) {
                for (Integer value : numbersInSource2) {
                    if (integer.equals(value)) {
                        expectedValues.add(integer);
                    }
                }
            }

            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();

            int numRecords = 0;
            Record record1;
            Record record2;
            Record expected;

            while (iter.hasNext() && numRecords < expectedValues.size()) {
                record1 = new Record(expectedValues.get(numRecords), "Jane", "Doe");
                record2 = new Record(expectedValues.get(numRecords), "Jane", "Doe");
                expected = record1.concat(record2);
                assertEquals("mismatch at record " + numRecords, expected, iter.next());
                numRecords++;
            }

            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", expectedValues.size(), numRecords);
        }
    }

    @Test
    public void testSomeMatchesDuplicateKeysINLJ() {
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("a", Type.intType())
                    .add("b", Type.intType());
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");
            // Table 1
            transaction1.insert("table1", 1, 2, 3);
            transaction1.insert("table1", 1, 5, 5);
            transaction1.insert("table1", 2, 4, 4);
            transaction1.insert("table1", 7, 7, 7);
            transaction1.insert("table1", 3, 9, 9);
            // Table 2
            transaction1.insert("table2", 1, 2, 3);
            transaction1.insert("table2", 1, 2, 2);
            transaction1.insert("table2", 1, 1, 1);
            transaction1.insert("table2", 2, 2, 2);
            transaction1.insert("table2", 3, 4, 4);
            transaction1.insert("table2", 3, 3, 3);

            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            List<Record> expectedValues = new ArrayList<>();
            expectedValues.add((new Record(1, 2, 3)).concat(new Record(1, 1, 1)));
            expectedValues.add((new Record(1, 2, 3)).concat(new Record(1, 2, 2)));
            expectedValues.add((new Record(1, 2, 3)).concat(new Record(1, 2, 3)));
            expectedValues.add((new Record(1, 5, 5)).concat(new Record(1, 1, 1)));
            expectedValues.add((new Record(1, 5, 5)).concat(new Record(1, 2, 2)));
            expectedValues.add((new Record(1, 5, 5)).concat(new Record(1, 2, 3)));
            expectedValues.add((new Record(2, 4, 4)).concat(new Record(2, 2, 2)));
            expectedValues.add((new Record(3, 9, 9)).concat(new Record(3, 3, 3)));
            expectedValues.add((new Record(3, 9, 9)).concat(new Record(3, 4, 4)));

            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();

            int numRecords = 0;

            while (iter.hasNext() && numRecords < expectedValues.size()) {
                assertEquals("mismatch at record " + numRecords, expectedValues.get(numRecords), iter.next());
                numRecords++;
            }

            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", expectedValues.size(), numRecords);
        }
    }

    @Test
    public void testNonEmptyWithEmptyINLJ() {
        // Joins a non-empty table with an empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");
            for (int i = 0; i < 100; i++) {
                transaction1.insert("table1", i, "Jane", "Doe");
            }
            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();
            assertFalse("too many records", iter.hasNext());
        }
    }

    @Test
    public void testEmptyWithNonEmptyINLJ() {
        // Joins an empty table with a non-empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");
            for (int i = 0; i < 100; i++) {
                transaction1.insert("table2", i, "Jane", "Doe");
            }
            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();
            assertFalse("too many records", iter.hasNext());
        }
    }

    @Test
    public void testEmptyWithEmptyINLJ() {
        // Joins a empty table with an empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        try(Transaction transaction1 = d.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            transaction1.createTable(s, "table1");
            transaction1.createTable(s, "table2");
            transaction1.createIndex("table2", "id", false);
        }
        try(Transaction transaction2 = d.beginTransaction()) {
            // FROM table1 AS t1
            QueryPlan queryPlan = transaction2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table2", "t2", "t1.id", "t2.id");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.select("t2.id", PredicateOperator.GREATER_THAN_EQUALS, 0);
            queryPlan.project("t1.id", "t2.id");
            // run the query
            Iterator<Record> iter = queryPlan.indexTestExecute();
            assertFalse("too many records", iter.hasNext());
        }
    }

}