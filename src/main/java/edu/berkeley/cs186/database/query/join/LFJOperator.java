package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;
import java.lang.Math;

import java.util.*;

// Leapfrog Trie Join
public class LFJOperator extends JoinOperator {
    // Both relations should be sorted
    public LFJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
                prepareRight(transaction, rightSource, rightColumnName),
                leftColumnName, rightColumnName, transaction, JoinType.LFJ);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source is already sorted on the target column then this
     * returns the rightSource, otherwise it wraps the right source in a sort
     * operator.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (rightSource.sortedBy().contains(rightColumn)) return rightSource;
        return new SortOperator(transaction, rightSource, rightColumn);
    }

    @Override
    public Iterator<Record> iterator() {
        return new LeapfrogJoinIterator(this);
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    // Iterator to return joined results
    private class LeapfrogJoinIterator implements Iterator<Record> {
        private final LeapfrogIterator leftIterator;
        private final LeapfrogIterator rightIterator;
        private Record nextRecord; // The joined record to return
        private boolean atEnd;
        private int p;
        private final LeapfrogIterator[] iters;

        private LeapfrogJoinIterator(LFJOperator lfo) {
            super();
            leftIterator = new LeapfrogIterator(getLeftSource(), lfo);
            rightIterator = new LeapfrogIterator(getRightSource(), lfo);

            iters = new LeapfrogIterator[2];
            iters[0] = leftIterator;
            iters[1] = rightIterator;
            leapfrog_init();
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            return leapfrog_next();
        }

        // Initialize state and find first result
        public void leapfrog_init() {
            if (leftIterator.atEnd() || rightIterator.atEnd()) {
                this.atEnd = true;
                return;
            }
            this.atEnd = false;
            if (compare(leftIterator.key(), rightIterator.key()) < 0) {
                iters[0] = leftIterator;
                iters[1] = rightIterator;
            } else {
                iters[0] = rightIterator;
                iters[1] = leftIterator;
            }
            p = 0;
            nextRecord = leapfrog_search();
        }

        // Find the next record to join
        public Record leapfrog_search() {
            Record y = iters[Math.floorMod(p - 1, 2)].key(); // Max key of any iter
            while (true) {
                Record x = iters[p].key(); // Least key of any iterator
                if (x == null) {
                    this.atEnd = true;
                    return null;
                }
                if (compare(x, y) == 0) {
                    // All iters at same key
                    return x.concat(y);
                } else {
                    iters[p].seek(y);
                    if (iters[p].atEnd()) {
                        this.atEnd = true;
                        return null;
                    } else {
                        y = iters[p].key();
                        p = Math.floorMod(p + 1, 2);
                    }
                }
            }
        }

        // Return the next joined record
        public Record leapfrog_next() {
            iters[p].next();
            if (iters[p].atEnd()) {
                this.atEnd = true;
                return null;
            } else {
                p = Math.floorMod(p + 1, 2);
                return leapfrog_search();
            }
        }

        // Finds first element in common which is ≥ seekKey
        /*
        public void leapfrog_seek(int seekKey) {
            iters[p].seek(seekKey);
            if (iters[p].atEnd()) {
                this.atEnd = true;
            } else {
                p = (p + 1) % 2;
                leapfrog_search();
            }
        }
         */
    }

    // Iterator for one source. Helper iterator for LeapfrogJoinIterator.
    private static class LeapfrogIterator {
        private int index;
        LFJOperator outsideOperator;
        ArrayList<Record> sourceList = new ArrayList<>();

        private LeapfrogIterator(QueryOperator recordSource, LFJOperator outsideOperator) {
            this.outsideOperator = outsideOperator;
            recordSource.iterator().forEachRemaining(sourceList::add);
            index = 0;
        }

        // True when iterator is at the end
        public boolean atEnd() {
            return index >= sourceList.size();
        }

        // Proceeds to the next key
        public void next() {
            if (index >= sourceList.size()) {
                return;
            }
            index++;
        }

        /*
            Position the iterator at a least upper bound for seekKey.
            i.e. the least key ≥ seekKey, or move to end if no such key exists.

            Sought key must be ≥ the key at the current position.
        */
        public void seek(Record seekKey) {
            if (seekKey == null || key() == null) {
                return;
            }
            if (outsideOperator.compare(seekKey, key()) < 0) {
                return;
            }

            // Perform binary search to find the smallest element greater than or equal to seekKey
            int low = index;
            int high = sourceList.size() - 1;

            while (low <= high) {
                int mid = (low + high) / 2;
                if (outsideOperator.compare(sourceList.get(mid), seekKey) < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            index = low;
        }

        // Returns the key at the current iterator position.
        public Record key() {
            if (atEnd()) {
                return null;
            }
            return sourceList.get(index);
        }
    }
}