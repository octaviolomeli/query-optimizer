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
    private class LeapfrogTrieJoinIterator implements Iterator<Record> {
        private Record nextRecord; // The joined record to return
        private boolean atEnd;
        private final LeapfrogTrieIterator[] iters;
        // Stores duplicates to return
        private final ArrayList<Record> savedRecordsToReturn;
        private int depth;
        private final int maxDepth;

        private LeapfrogTrieJoinIterator() {
            super();
            LeapfrogTrieIterator leftIterator = new LeapfrogTrieIterator(getLeftSource(), true);
            LeapfrogTrieIterator rightIterator = new LeapfrogTrieIterator(getRightSource(), false);
            savedRecordsToReturn = new ArrayList<>();
            depth = -1;
            maxDepth = getLeftColumnIndexes().size() - 1;

            iters = new LeapfrogTrieIterator[2];
            leapfrogtrie_init(leftIterator, rightIterator);
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null && !this.savedRecordsToReturn.isEmpty()){
                this.nextRecord = savedRecordsToReturn.remove(0);
            } else if (this.nextRecord == null){
                this.nextRecord = leapfrogtrie_next();
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

        private void leapfrogtrie_init(LeapfrogTrieIterator leftIterator, LeapfrogTrieIterator rightIterator) {
            if (leftIterator.currNode.children.isEmpty() || rightIterator.currNode.children.isEmpty()) {
                this.atEnd = true;
                return;
            }

            // Order `iters` by smallest first key
            leftIterator.open();
            rightIterator.open();
            DataBox leftKey = leftIterator.key();
            DataBox rightKey = rightIterator.key();
            if (leftKey.compareTo(rightKey) < 0) {
                iters[0] = leftIterator;
                iters[1] = rightIterator;
                this.atEnd = !leftIterator.seek(rightKey);
            } else {
                iters[0] = rightIterator;
                iters[1] = leftIterator;
                this.atEnd = !rightIterator.seek(leftKey);
            }
            leftIterator.up();
            rightIterator.up();

            // Align iterators on all levels
            while (depth <= maxDepth) {
                iters[0].open();
                iters[1].open();
                boolean successfulAlignment = leapfrogtrie_search();
                if (!successfulAlignment) {
                    iters[0].up();
                    iters[1].up();
                    iters[0].next();
                    iters[1].next();
                } else {
                    depth += 1;
                }
            }
            depth -= 1;
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record leapfrogtrie_next() {
            if (this.atEnd) {
                return null;
            }
            leapfrogtrie_search();
            return null;
        }

        // Align the iterators on the current level. Return false if not possible.
        private boolean leapfrogtrie_search() {
            LeapfrogTrieIterator iter1 = iters[0];
            LeapfrogTrieIterator iter2 = iters[1];
            if (depth == -1 && maxDepth != -1) {
                iter1.open();
                iter2.open();
                depth += 1;
            }
            DataBox iter1Key = iter1.key();
            DataBox iter2Key = iter2.key();
            if (iter1Key.compareTo(iter2Key) < 0) {
                return iter1.seek(iter2Key);
            } else if (iter1Key.compareTo(iter2Key) == 0) {
                return true;
            }
            else {
                return iter2.seek(iter1Key);
            }
        }
    }

    // Iterator for one source. Helper iterator for LeapfrogTrieJoinIterator.
    private class LeapfrogTrieIterator {
        TrieNode trieRoot;
        TrieNode currNode;

        private LeapfrogTrieIterator(QueryOperator recordSource, boolean isLeftSource) {
            trieRoot = new TrieNode(isLeftSource);
            currNode = trieRoot;
            // Insert all records
            for (Record rec : recordSource) {
                trieRoot.insert(rec, 0);
            }
            trieRoot.sortChildren();
        }

        // Proceed to the first key at the next depth
        public void open() {
            DataBox childKey = currNode.sortedChildren.get(0);
            currNode.setIndexOfChildLastVisited(0);
            currNode = currNode.children.get(childKey);
        }

        // Return to the parent key at the previous depth
        public void up() {
            if (currNode == trieRoot) {
                throw new UnsupportedOperationException();
            }
            currNode = currNode.parent;
        }

        // Go to sibling node on the same level.
        public void next() {
            if (atEnd()) {
                throw new NoSuchElementException();
            }
            int parentChildIndex = currNode.getParent().getIndexOfChildLastVisited();
            int nextChild = parentChildIndex + 1;
            currNode.getParent().setIndexOfChildLastVisited(nextChild);
            DataBox keyToVisit = currNode.getParent().sortedChildren.get(nextChild);
            currNode = currNode.getParent().children.get(keyToVisit);
        }

        /*
            Go to sibling node on the same level that has key ≥ seekKey
            Return true if the iterator moved to a new position
        */
        public boolean seek(DataBox seekKey) {
            currNode = currNode.getParent();
            int indexOfSeekKey = Collections.binarySearch(currNode.sortedChildren, seekKey, new DataBoxComparator());
            if (indexOfSeekKey < 0) {
                return false;
            }
            currNode = currNode.children.get(currNode.sortedChildren.get(indexOfSeekKey));
            return true;
        }

        /*
            Check if end of this level.
            Go to parent and see if any next children exists.
        */
        public boolean atEnd() {
            if (currNode == trieRoot) {
                throw new UnsupportedOperationException();
            }
            int parentChildIndex = currNode.getParent().getIndexOfChildLastVisited();
            return parentChildIndex == currNode.getParent().children.size() - 1;
        }

        public DataBox key() {
            return currNode.value;
        }
    }

    public class TrieNode {
        public HashMap<DataBox, TrieNode> children;
        public ArrayList<DataBox> sortedChildren;
        private int indexOfChildLastVisited;
        private TrieNode parent;
        private ArrayList<Record> records;
        private final DataBox value;
        private final boolean isLeftSource;

        public TrieNode(boolean isLeftSource) {
            this.children = new HashMap<>();
            this.sortedChildren = new ArrayList<>();
            this.indexOfChildLastVisited = -1;
            this.parent = null;
            this.records = null;
            this.value = null;
            this.isLeftSource = isLeftSource;
        }

        public TrieNode(boolean isLeftSource, DataBox value) {
            this.children = new HashMap<>();
            this.sortedChildren = new ArrayList<>();
            this.indexOfChildLastVisited = -1;
            this.parent = null;
            this.records = null;
            this.value = value;
            this.isLeftSource = isLeftSource;
        }

        // Insert the values from a record into the Trie
        public void insert(Record record, int indexInCI) {
            // Last index
            if (this.isLeftSource && indexInCI == getLeftColumnIndexes().size() - 1) {
                if (records == null) {
                    records = new ArrayList<>();
                }
                this.records.add(record);
            } else if (!this.isLeftSource && indexInCI == getRightColumnIndexes().size() - 1) {
                if (records == null) {
                    records = new ArrayList<>();
                }
                this.records.add(record);
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
            return records != null;
        }

        public TrieNode getParent() {
            if (this.parent == null) {
                throw new NoSuchElementException();
            }
            return this.parent;
        }

        public int getIndexOfChildLastVisited() {
            return this.indexOfChildLastVisited;
        }

        public void setIndexOfChildLastVisited(int newIndex) {
            this.indexOfChildLastVisited = newIndex;
        }

        public ArrayList<Record> getRecords() {
            if (isEnd()) {
                throw new NoSuchElementException();
            }
            return this.records;
        }

        // Sort the children of this TrieNode and recurse
        public void sortChildren() {
            if (sortedChildren.isEmpty()) {
                return;
            }
            ArrayList<DataBox> childrenSorted = new ArrayList<>(children.keySet());
            childrenSorted.sort(new DataBoxComparator());
            sortedChildren = childrenSorted;
            for (TrieNode child : children.values()) {
                child.sortChildren();
            }
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
