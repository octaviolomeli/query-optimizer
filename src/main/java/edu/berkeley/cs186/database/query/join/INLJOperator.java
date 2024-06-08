package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.IndexScanOperator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Index Nested Loop Join algorithm.
 */
public class INLJOperator extends JoinOperator {
    String rightTableName;
    public INLJOperator(QueryOperator leftSource, QueryOperator rightSource,
                        String leftColumnName, String rightColumnName,
                        TransactionContext transaction, String rightTableName) {
        super(leftSource, materialize(rightSource, transaction), makeArrayListWith(leftColumnName),
                makeArrayListWith(rightColumnName), transaction, JoinType.INLJ);
        this.stats = this.estimateStats();
        this.rightTableName = rightTableName;
    }

    @Override
    public Iterator<Record> iterator() {
        return new INLJIterator();
    }

    @Override
    public int estimateIOCost() {
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightRecords = getRightSource().estimateStats().getNumRecords();

        // Index stuff
        int height = this.getTransaction().getTreeHeight(rightTableName, getRightColumnName());
        int order = this.getTransaction().getTreeOrder(rightTableName, getRightColumnName());

        return numLeftPages + numLeftRecords *  (int) (height + Math.ceil(numRightRecords / (1.5 * order)));
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Note that the left table is the "outer" loop and the right table is the
     * "inner" loop.
     */
    private class INLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left relation
        private Iterator<Record> leftSourceIterator;
        // Index Operator over right relation
        private IndexScanOperator rightSourceIndexOperator;
        // Iterator for records in the right relation that match
        private Iterator<Record> rightSourceIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        public INLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            if (leftSourceIterator.hasNext()) leftRecord = leftSourceIterator.next();
            if (leftSourceIterator.hasNext()) {
                DataBox value = leftRecord.getValue(getLeftColumnIndex());
                this.rightSourceIndexOperator = new IndexScanOperator(getTransaction(),
                        rightTableName, getRightColumnName(), PredicateOperator.EQUALS, value);

                this.rightSourceIterator = rightSourceIndexOperator.iterator();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            while(true) {
                if (this.rightSourceIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightSourceIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftSourceIterator.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.leftRecord = leftSourceIterator.next();
                    DataBox value = leftRecord.getValue(getLeftColumnIndex());
                    this.rightSourceIndexOperator = new IndexScanOperator(getTransaction(),
                            rightTableName, getRightColumnName(), PredicateOperator.EQUALS, value);
                    this.rightSourceIterator = rightSourceIndexOperator.iterator();
                } else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}

