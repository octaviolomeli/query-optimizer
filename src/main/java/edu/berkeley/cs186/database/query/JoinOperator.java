package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;

public abstract class JoinOperator extends QueryOperator {
    public enum JoinType {
        SNLJ,
        PNLJ,
        BNLJ,
        SORTMERGE,
        SHJ,
        GHJ,
        INLJ,
        LFJ,
        LFTJ
    }
    protected JoinType joinType;

    // the source operators
    private QueryOperator leftSource;
    private QueryOperator rightSource;

    // join column indices
    private ArrayList<Integer> leftColumnIndexes;
    private ArrayList<Integer> rightColumnIndexes;

    // join column names
    private ArrayList<String> leftColumnNames;
    private ArrayList<String> rightColumnNames;

    // current transaction
    private TransactionContext transaction;

    /**
     * Create a join operator that pulls tuples from leftSource and rightSource.
     * Returns tuples for which leftColumnName and rightColumnName are equal.
     *
     * @param leftSource the left source operator
     * @param rightSource the right source operator
     * @param leftColumnNames the column(s) to join on from leftSource
     * @param rightColumnNames the column(s) to join on from rightSource
     */
    public JoinOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 ArrayList<String> leftColumnNames,
                 ArrayList<String> rightColumnNames,
                 TransactionContext transaction,
                 JoinType joinType) {
        super(OperatorType.JOIN);
        this.joinType = joinType;
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftColumnNames = leftColumnNames;
        this.rightColumnNames = rightColumnNames;
        this.leftColumnIndexes = new ArrayList<>();
        this.rightColumnIndexes = new ArrayList<>();
        this.setOutputSchema(this.computeSchema());
        this.transaction = transaction;
    }

    @Override
    public QueryOperator getSource() {
        throw new RuntimeException("There is no single source for join operators. use " +
                                     "getRightSource and getLeftSource and the corresponding set methods.");
    }

    @Override
    public Schema computeSchema() {
        // Get lists of the field names of the records
        Schema leftSchema = this.leftSource.getSchema();
        Schema rightSchema = this.rightSource.getSchema();

        // Set up join column attributes
        for (String leftColumn : leftColumnNames) {
            this.leftColumnIndexes.add(leftSchema.findField(leftColumn));
        }

        for (String rightColumn : rightColumnNames) {
            this.rightColumnIndexes.add(rightSchema.findField(rightColumn));
        }

        // Return concatenated schema
        return leftSchema.concat(rightSchema);
    }

    @Override
    public String str() {
        return String.format("%s on %s=%s (cost=%d)",
                this.joinType, this.leftColumnNames.toString(), this.rightColumnNames.toString(),
                this.estimateIOCost());
    }

    @Override
    public String toString() {
        String r = this.str();
        if (this.leftSource != null) {
            r += ("\n-> " + this.leftSource.toString()).replaceAll("\n", "\n\t");
        }
        if (this.rightSource != null) {
            r += ("\n-> " + this.rightSource.toString()).replaceAll("\n", "\n\t");
        }
        return r;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        TableStats leftStats = this.leftSource.estimateStats();
        TableStats rightStats = this.rightSource.estimateStats();
        return leftStats.copyWithJoin(this.leftColumnIndexes.get(0),
                rightStats,
                this.rightColumnIndexes.get(0));
    }

    /**
     * @return the query operator which supplies the left records of the join
     */
    protected QueryOperator getLeftSource() {
        return this.leftSource;
    }

    /**
     * @return the query operator which supplies the right records of the join
     */
    protected QueryOperator getRightSource() {
        return this.rightSource;
    }

    /**
     * @return the transaction context this operator is being executed within
     */
    public TransactionContext getTransaction() {
        return this.transaction;
    }

    /**
     * @return the name of the left column being joined on
     */
    public String getLeftColumnName() {
        return this.leftColumnNames.get(0);
    }

    public ArrayList<String> getLeftColumnNames() {
        return this.leftColumnNames;
    }

    /**
     * @return the name of the right column being joined on
     */
    public String getRightColumnName() {
        return this.rightColumnNames.get(0);
    }

    public ArrayList<String> getRightColumnNames() {
        return this.rightColumnNames;
    }

    /**
     * @return the position of the column being joined on in the left relation's
     * schema. Can be used to determine which value in the left relation's records
     * to check for equality on.
     */
    public int getLeftColumnIndex() {
        return this.leftColumnIndexes.get(0);
    }

    public ArrayList<Integer> getLeftColumnIndexes() {
        return this.leftColumnIndexes;
    }

    /**
     * @return the position of the column being joined on in the right relation's
     * schema. Can be used to determine which value in the right relation's records
     * to check for equality on.
     */
    public int getRightColumnIndex() {
        return this.rightColumnIndexes.get(0);
    }

    public ArrayList<Integer> getRightColumnIndexes() {
        return this.rightColumnIndexes;
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * @return 0 if leftRecord and rightRecord match on their join values,
     * a negative value if leftRecord's join value is less than rightRecord's
     * join value, or a positive value if leftRecord's join value is greater
     * than rightRecord's join value.
     */
    // Count how many are 0, how many are negative, how many are positive
    public int compare(Record leftRecord, Record rightRecord) {

        // 0 = negative, 1 = equal, 2 = positive
        int [] comparisons = new int[]{0, 0, 0};

        for (int i = 0; i < leftColumnIndexes.size(); i++) {
            DataBox leftRecordValue = leftRecord.getValue(leftColumnIndexes.get(i));
            DataBox rightRecordValue = rightRecord.getValue(rightColumnIndexes.get(i));

            int compareValue = leftRecordValue.compareTo(rightRecordValue);
            if (compareValue == 0) {
                comparisons[1] += 1;
            } else if (compareValue < 0) {
                comparisons[0] += 1;
            } else {
                comparisons[2] += 1;
            }
        }

        if (comparisons[2] > comparisons[1] && comparisons[2] > comparisons[0]) {
            return 1;
        }  else if (comparisons[1] > comparisons[2] && comparisons[1] > comparisons[0]) {
            return 0;
        }
        return -1;
    }

    public static ArrayList<String> makeArrayListWith(String columnName) {
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add(columnName);
        return columnNames;
    }
}
