package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.LongDataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Simple Nested Loop Join algorithm.
 */
public class INLJOperator extends JoinOperator {
   String rightTable;

    public INLJOperator(QueryOperator leftSource, QueryOperator rightSource, String leftColumnName, String rightColumnName, TransactionContext transaction, String rightTable) {
        super(leftSource, materialize(rightSource, transaction), leftColumnName, rightColumnName, transaction, JoinType.INLJ);
        this.stats = this.estimateStats();
        this.rightTable = rightTable;
    }

    @Override
    public Iterator<Record> iterator() {
        return new INLJIterator();
    }

    @Override
    public int estimateIOCost() {
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();
        int numLeafPages = numLeftRecords / (2 * getTransaction().getTreeOrder(rightTable, getRightColumnName()));
        return numLeftPages + numLeftRecords * (1 + getTransaction().getTreeHeight(rightTable, getRightColumnName()) + numLeafPages);
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Note that the left table is the "outer" loop and the right table is the
     * "inner" loop.
     */
    private class INLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left relation
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right relation
        private Iterator<Record> rightSourceIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        public INLJIterator() {
            super();

            this.leftSourceIterator = getLeftSource().iterator();
            if (leftSourceIterator.hasNext()) leftRecord = leftSourceIterator.next();

            if (leftRecord != null) {
                this.rightSourceIterator = getTransaction().lookupKey(rightTable, getRightColumnName(), leftRecord.getValue(getLeftColumnIndex()));
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
                    this.rightSourceIterator = getTransaction().lookupKey(rightTable, getRightColumnName(), leftRecord.getValue(getLeftColumnIndex()));
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

