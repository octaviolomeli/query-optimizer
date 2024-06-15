package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;
import java.lang.Math;

import java.util.*;

// Leapfrog Join
public class LFJOperator extends JoinOperator {
    // Both relations should be sorted
    public LFJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
                prepareRight(transaction, rightSource, rightColumnName),
                makeArrayListWith(leftColumnName), makeArrayListWith(rightColumnName), transaction, JoinType.LFJ);
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
        return new LeapfrogJoinIterator();
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
        private Record nextRecord; // The joined record to return
        private boolean atEnd;
        private int p;
        private final LeapfrogIterator[] iters;
        // Stores duplicates to return
        private final ArrayList<Record> savedRecordsToReturn;
        private boolean isZeroLeftSource;

        private LeapfrogJoinIterator() {
            super();
            LeapfrogIterator leftIterator = new LeapfrogIterator(getLeftSource());
            LeapfrogIterator rightIterator = new LeapfrogIterator(getRightSource());

            savedRecordsToReturn = new ArrayList<>();
            iters = new LeapfrogIterator[2];
            leapfrog_init(leftIterator, rightIterator);
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.atEnd) {
                return false;
            }
            if (this.nextRecord == null && !this.savedRecordsToReturn.isEmpty()) {
                this.nextRecord = savedRecordsToReturn.remove(0);
            } else if (this.nextRecord == null) {
                this.nextRecord = leapfrog_next();
            }
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

        // Initialize state and find first result
        public void leapfrog_init(LeapfrogIterator leftIterator, LeapfrogIterator rightIterator) {
            if (leftIterator.atEnd() || rightIterator.atEnd()) {
                this.atEnd = true;
                return;
            }
            this.atEnd = false;

            if (compare(leftIterator.key(), rightIterator.key()) < 0) {
                iters[0] = leftIterator;
                iters[1] = rightIterator;
                this.isZeroLeftSource = true;
            } else {
                iters[0] = rightIterator;
                iters[1] = leftIterator;
                this.isZeroLeftSource = false;
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

                if (LFJOcompare(x, y, p, isZeroLeftSource) == 0) {
                    // All iters at same key
                    for (Integer indexX : iters[p].indicesWithValue(x, p, isZeroLeftSource)) {
                        for (Integer indexY : iters[Math.floorMod(p - 1, 2)].indicesWithValue(y, Math.floorMod(p - 1, 2), isZeroLeftSource)) {
                            this.savedRecordsToReturn.add(
                                    iters[p].keyAt(indexX)
                                            .concat(iters[Math.floorMod(p - 1, 2)].keyAt(indexY)));
                            iters[p].resetToIndex(indexX);
                            iters[Math.floorMod(p - 1, 2)].resetToIndex(indexY);
                        }
                    }

                    return savedRecordsToReturn.remove(0);
                } else {
                    iters[p].seek(y, p, isZeroLeftSource);
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

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
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
    }

    // Iterator for one source. Helper iterator for LeapfrogJoinIterator.
    private class LeapfrogIterator {
        private int index;
        ArrayList<Record> sourceList = new ArrayList<>();

        private LeapfrogIterator(QueryOperator recordSource) {
            recordSource.iterator().forEachRemaining(sourceList::add);
            this.index = 0;
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
        public void seek(Record seekKey, int p, boolean isZeroLeftSource) {
            if (seekKey == null || key() == null) {
                return;
            }
            if (LFJOcompare(key(), seekKey, p, isZeroLeftSource) >= 0) {
                return;
            }

            // Perform binary search to find the smallest element greater than or equal to seekKey
            int low = index;
            int high = sourceList.size() - 1;

            while (low <= high) {
                int mid = (low + high) / 2;
                if (LFJOcompare(sourceList.get(mid), seekKey, p, isZeroLeftSource) < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            index = low;
        }

        public void resetToIndex(int position) {
            this.index = position;
        }

        // Returns the key at the current iterator position.
        public Record key() {
            if (atEnd()) {
                return null;
            }
            return sourceList.get(index);
        }

        public Record keyAt(int position) {
            return sourceList.get(position);
        }

        // Given a value, return list of indices in sourceList that have that value.
        public List<Integer> indicesWithValue(Record seekValue, int p, boolean isZeroLeftSource) {
            List<Integer> indices = new ArrayList<>();
            int startIndex = binarySearch(seekValue, p, isZeroLeftSource);

            if (startIndex < 0) {
                // Value not found
                return indices;
            }

            // Expand to find all occurrences
            int left = startIndex;
            while (left >= 0 && LFJOcompare(sourceList.get(left), seekValue, p, isZeroLeftSource) == 0) {
                indices.add(left);
                left--;
            }

            int right = startIndex + 1;
            while (right < sourceList.size() && LFJOcompare(sourceList.get(right), seekValue, p, isZeroLeftSource) == 0) {
                indices.add(right);
                right++;
            }

            Collections.sort(indices); // Ensure indices are in ascending order
            return indices;
        }

        private int binarySearch(Record value, int p, boolean isZeroLeftSource) {
            int left = 0;
            int right = sourceList.size() - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                int cmp = LFJOcompare(sourceList.get(mid), value, p, isZeroLeftSource);

                if (cmp < 0) {
                    left = mid + 1;
                } else if (cmp > 0) {
                    right = mid - 1;
                } else {
                    // Ensure finding the first occurrence
                    if (mid == 0 || LFJOcompare(sourceList.get(mid - 1), value, p, isZeroLeftSource) != 0) {
                        return mid;
                    } else {
                        right = mid - 1;
                    }
                }
            }
            return -1; // Value not found
        }
    }
}