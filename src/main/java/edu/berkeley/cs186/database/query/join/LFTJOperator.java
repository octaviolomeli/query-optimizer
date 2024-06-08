//package edu.berkeley.cs186.database.query.join;
//
//import edu.berkeley.cs186.database.TransactionContext;
//import edu.berkeley.cs186.database.databox.DataBox;
//import edu.berkeley.cs186.database.query.JoinOperator;
//import edu.berkeley.cs186.database.query.QueryOperator;
//import edu.berkeley.cs186.database.query.SortOperator;
//import edu.berkeley.cs186.database.table.Record;
//
//import java.util.*;
//
//// Leapfrog TrieJoin
//public class LFTJOperator extends JoinOperator {
//    // Both relations should be sorted
//    public LFTJOperator(QueryOperator leftSource,
//                        QueryOperator rightSource,
//                        ArrayList<String> leftColumnNames,
//                        ArrayList<String> rightColumnNames,
//                        TransactionContext transaction) {
//        super(prepareLeft(transaction, leftSource, leftColumnNames),
//                prepareRight(transaction, rightSource, rightColumnNames),
//                leftColumnNames, rightColumnNames, transaction, JoinType.LFTJ);
//        this.stats = this.estimateStats();
//    }
//
//    /**
//     * If the left source is already sorted on the target column then this
//     * returns the leftSource, otherwise it wraps the left source in a sort
//     * operator.
//     */
//    private static QueryOperator prepareLeft(TransactionContext transaction,
//                                             QueryOperator leftSource,
//                                             String leftColumn) {
//        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
//        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
//        return new SortOperator(transaction, leftSource, leftColumn);
//    }
//
//    /**
//     * If the right source is already sorted on the target column then this
//     * returns the rightSource, otherwise it wraps the right source in a sort
//     * operator.
//     */
//    private static QueryOperator prepareRight(TransactionContext transaction,
//                                              QueryOperator rightSource,
//                                              String rightColumn) {
//        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
//        if (rightSource.sortedBy().contains(rightColumn)) return rightSource;
//        return new SortOperator(transaction, rightSource, rightColumn);
//    }
//
//    @Override
//    public Iterator<Record> iterator() {
//        return new LeapfrogTrieJoinIterator();
//    }
//
//    @Override
//    public List<String> sortedBy() {
//        return Arrays.asList(getLeftColumnName(), getRightColumnName());
//    }
//
//    @Override
//    public int estimateIOCost() {
//        //does nothing
//        return 0;
//    }
//
//    // Iterator to return joined results
//    // NOT UPDATED YET
//    private class LeapfrogTrieJoinIterator implements Iterator<Record> {
//        private final LeapfrogTrieIterator leftIterator;
//        private final LeapfrogTrieIterator rightIterator;
//        private Record nextRecord; // The joined record to return
//        private boolean atEnd;
//        private int p;
//        private final LeapfrogTrieIterator[] iters;
//        private int depth;
//
//        private LeapfrogTrieJoinIterator() {
//            super();
//            leftIterator = new LeapfrogTrieIterator(getLeftSource());
//            rightIterator = new LeapfrogTrieIterator(getRightSource());
//
//            iters = new LeapfrogTrieIterator[2];
//            iters[0] = leftIterator;
//            iters[1] = rightIterator;
//            depth = -1;
//        }
//
//        /**
//         * @return true if this iterator has another record to yield, otherwise
//         * false
//         */
//        @Override
//        public boolean hasNext() {
//            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
//            return this.nextRecord != null;
//        }
//
//        /**
//         * @return the next record from this iterator
//         * @throws NoSuchElementException if there are no more records to yield
//         */
//        @Override
//        public Record next() {
//            if (!this.hasNext()) throw new NoSuchElementException();
//            Record nextRecord = this.nextRecord;
//            this.nextRecord = null;
//            return nextRecord;
//        }
//
//        /**
//         * Returns the next record that should be yielded from this join,
//         * or null if there are no more records to join.
//         */
//        private Record fetchNextRecord() {
//            return null;
//        }
//    }
//
//    // Iterator for one source. Helper iterator for LeapfrogTrieJoinIterator.
//    private static class LeapfrogTrieIterator {
//        TrieNode trieRoot;
//        TrieNode currNode;
//        // Index for the current node's children
//        int currentIndexInSortedChildren = 0;
//        int depth = -1;
//
//        private LeapfrogTrieIterator(QueryOperator recordSource) {
//            trieRoot = new TrieNode();
//            for (Record rec : recordSource) {
//                trieRoot.insert(rec.getValues());
//            }
//            currNode = trieRoot;
//        }
//
//        // Proceed to the first key at the next depth
//        public void open() {
//            DataBox firstChild = sortChildren().get(0);
//            currNode = currNode.children.get(firstChild);
//            currentIndexInSortedChildren = 0;
//        }
//
//        // Return to the parent key at the previous depth
//        public boolean up() {
//            if (depth == -1 || currNode.getParent() == null) {
//                return false;
//            }
//            currNode = currNode.getParent();
//            return true;
//        }
//
//        public void next() {
//
//        }
//
//        public ArrayList<DataBox> sortChildren() {
//            ArrayList<DataBox> childrenSorted = new ArrayList<>(currNode.children.keySet());
//            childrenSorted.sort(new DataBoxComparator());
//            return childrenSorted;
//        }
//
//        public boolean atEnd() {
//            return false;
//        }
//    }
//
//    public static class TrieNode {
//        private final HashMap<DataBox, TrieNode> children;
//        private final TrieNode parent;
//        private boolean isEnd;
//
//        public TrieNode() {
//            this.children = new HashMap<>();
//            this.parent = null;
//            this.isEnd = false;
//        }
//
//        // Insert the values from a record into the Trie
//        public void insert(List<DataBox> valuesToInsert) {
//            if (valuesToInsert.isEmpty()) {
//                throw new NoSuchElementException();
//            }
//            if (valuesToInsert.size() == 1) {
//                this.isEnd = true;
//            } else {
//                DataBox value = valuesToInsert.removeFirst();
//                children.put(value, new TrieNode());
//                children.get(value).insert(valuesToInsert);
//            }
//        }
//
//        public boolean isEnd() {
//            return this.isEnd;
//        }
//
//        public TrieNode getParent() {
//            return this.parent;
//        }
//    }
//
//    // Comparator used for sorting the children to determine first key at next depth in Trie
//    static class DataBoxComparator implements Comparator<DataBox> {
//        public int compare(DataBox a, DataBox b)
//        {
//            return a.compareTo(b);
//        }
//    }
//}
