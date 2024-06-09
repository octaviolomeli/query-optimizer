package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.*;

// Leapfrog TrieJoin
public class LFTJOperator extends JoinOperator {
    // Both relations should be sorted
    public LFTJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        ArrayList<String> leftColumnNames,
                        ArrayList<String> rightColumnNames,
                        TransactionContext transaction) {
        super(leftSource,
                rightSource,
                leftColumnNames, rightColumnNames, transaction, JoinType.LFTJ);
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new LeapfrogTrieJoinIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    // Iterator to return joined results
    // NOT UPDATED YET
    private class LeapfrogTrieJoinIterator implements Iterator<Record> {
        private final LeapfrogTrieIterator leftIterator;
        private final LeapfrogTrieIterator rightIterator;
        private Record nextRecord; // The joined record to return
        private boolean atEnd;
        private int p;
        private final LeapfrogTrieIterator[] iters;
        private int depth;

        private LeapfrogTrieJoinIterator() {
            super();
            leftIterator = new LeapfrogTrieIterator(getLeftSource(), true);
            rightIterator = new LeapfrogTrieIterator(getRightSource(), false);

            iters = new LeapfrogTrieIterator[2];
            iters[0] = leftIterator;
            iters[1] = rightIterator;
            depth = -1;
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
            return null;
        }
    }

    // Iterator for one source. Helper iterator for LeapfrogTrieJoinIterator.
    private class LeapfrogTrieIterator {
        TrieNode trieRoot;
        TrieNode currNode;
        // Index for the current node's children
        int currentIndexInSortedChildren = 0;
        int depth = -1;

        private LeapfrogTrieIterator(QueryOperator recordSource, boolean isLeftSource) {
            trieRoot = new TrieNode(isLeftSource);
            for (Record rec : recordSource) {
                trieRoot.insert(rec, 0);
            }
            currNode = trieRoot;
        }

        // Proceed to the first key at the next depth
        public void open() {

        }

        // Return to the parent key at the previous depth
        public boolean up() {
            if (depth == -1 || currNode.getParent() == null) {
                return false;
            }
            currNode = currNode.getParent();
            return true;
        }

        public void next() {

        }

        public boolean atEnd() {
            return false;
        }
    }

    public class TrieNode {
        public HashMap<DataBox, TrieNode> children;
        public ArrayList<DataBox> sortedChildren;
        private int indexOfChildLastVisited;
        private TrieNode parent;
        private Record record;
        private final DataBox value;
        private final boolean isLeftSource;

        public TrieNode(boolean isLeftSource) {
            this.children = new HashMap<>();
            this.sortedChildren = new ArrayList<>();
            this.indexOfChildLastVisited = -1;
            this.parent = null;
            this.record = null;
            this.value = null;
            this.isLeftSource = isLeftSource;
        }

        public TrieNode(boolean isLeftSource, DataBox value) {
            this.children = new HashMap<>();
            this.sortedChildren = new ArrayList<>();
            this.indexOfChildLastVisited = -1;
            this.parent = null;
            this.record = null;
            this.value = value;
            this.isLeftSource = isLeftSource;
        }

        // Insert the values from a record into the Trie
        public void insert(Record record, int indexInCI) {
            // Last index
            if (this.isLeftSource && indexInCI == getLeftColumnIndexes().size() - 1) {
                this.record = record;
            } else if (!this.isLeftSource && indexInCI == getRightColumnIndexes().size() - 1) {
                this.record = record;
            }
            DataBox keyToInsert;
            int indexInRecord;
            if (isLeftSource) {
                indexInRecord = getLeftColumnIndexes().get(indexInCI);
            } else {
                indexInRecord = getRightColumnIndexes().get(indexInCI);
            }
            keyToInsert = record.getValue(indexInRecord);
            children.put(keyToInsert, new TrieNode(isLeftSource, keyToInsert));
            children.get(keyToInsert).parent = this;
            children.get(keyToInsert).insert(record, indexInCI + 1);
        }

        public boolean isEnd() {
            return record != null;
        }

        public TrieNode getParent() {
            return this.parent;
        }

        public int getIndexOfChildLastVisited() {
            return this.indexOfChildLastVisited;
        }

        public void setIndexOfChildLastVisited(int newIndex) {
            this.indexOfChildLastVisited = newIndex;
        }

        public Record getRecord() {
            if (isEnd()) {
                throw new NoSuchElementException();
            }
            return this.record;
        }

        // Sort the children of this TrieNode
        // Use after all insertions are finished
        public void sortChildren() {
            ArrayList<DataBox> childrenSorted = new ArrayList<>(children.keySet());
            childrenSorted.sort(new DataBoxComparator());
            sortedChildren = childrenSorted;
        }
    }

    // Comparator used for sorting the children to determine first key at next depth in Trie
    static class DataBoxComparator implements Comparator<DataBox> {
        public int compare(DataBox a, DataBox b)
        {
            return a.compareTo(b);
        }
    }
}
