package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.TestSourceOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestUtils {
    public static Schema createSchemaWithAllTypes() {
        return new Schema()
                .add("bool", Type.boolType())
                .add("int", Type.intType())
                .add("string", Type.stringType(1))
                .add("float", Type.floatType());
    }

    public static Record createRecordWithAllTypes() {
        return new Record(true, 1, "a", 1.2f);
    }

    public static TestSourceOperator createSourceWithAllTypes(int numRecords) {
        Schema schema = createSchemaWithAllTypes();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++)
            records.add(createRecordWithAllTypes());
        return new TestSourceOperator(records, schema);
    }

    public static TestSourceOperator createSourceWithAllTypesExcept1V1(int numRecords) {
        Schema schema = createSchemaWithAllTypes();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < numRecords - 1; i++)
            records.add(createRecordWithAllTypes());
        records.add(new Record(false, 3, "b", 1.9f));
        return new TestSourceOperator(records, schema);
    }

    public static TestSourceOperator createSourceWithAllTypesExcept1V2(int numRecords) {
        Schema schema = createSchemaWithAllTypes();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < numRecords - 1; i++)
            records.add(createRecordWithAllTypes());
        records.add(new Record(false, 4, "d", 5.9f));
        return new TestSourceOperator(records, schema);
    }

    public static TestSourceOperator createIncreasingSourceWithAllTypes(int numRecords) {
        Schema schema = createSchemaWithAllTypes();
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= numRecords; i++)
            records.add(createRecordWithAllTypesWithValue(i));
        return new TestSourceOperator(records, schema);
    }

    public static TestSourceOperator createIncreasingJumpSourceWithInts(int numRecords, int jump) {
        Schema schema = new Schema().add("int", Type.intType());
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= numRecords * jump; i+= jump)
            records.add(new Record(i));
        return new TestSourceOperator(records, schema);
    }

    public static Record createRecordWithAllTypesWithValue(int val) {
        return new Record(true, val, "" + (char) (val % 79 + 0x30), 1.0f);
    }

    public static TestSourceOperator createSourceWithInts(List<Integer> values) {
        Schema schema = new Schema().add("int", Type.intType());
        List<Record> recordList = new ArrayList<Record>();
        for (int v : values) recordList.add(new Record(v));
        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createIncreasingSourceWith3IntFields(int numRecords) {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.intType())
                .add("field3", Type.intType());
        List<Record> recordList = new ArrayList<Record>();
        for (int i = 0; i < numRecords; i++) {
            recordList.add(new Record(i, i, i));
        }
        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createIncreasingSourceWith3IntFields(int numRecords, int start) {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.intType())
                .add("field3", Type.intType());
        List<Record> recordList = new ArrayList<Record>();
        for (int i = start; i < numRecords + start; i++) {
            recordList.add(new Record(i, i, i));
        }
        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createEmptySourceWith3IntFields() {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.intType())
                .add("field3", Type.intType());
        List<Record> recordList = new ArrayList<Record>();
        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createDecreasingSourceWith3IntFields(int numRecords, int start) {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.intType())
                .add("field3", Type.intType());
        List<Record> recordList = new ArrayList<Record>();
        for (int i = numRecords + start - 1; i >= start; i--) {
            recordList.add(new Record(i, i, i));
        }
        return new TestSourceOperator(recordList, schema);
    }

    // LEAPFROG TRIEJOIN SOURCES
    public static TestSourceOperator createExampleSource1() {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.stringType(1))
                .add("field3", Type.intType())
                .add("field4", Type.stringType(2));
        List<Record> recordList = new ArrayList<Record>();
        recordList.add(new Record(1, "a", 1, "aa"));
        recordList.add(new Record(1, "c", 1, "bb"));
        recordList.add(new Record(1, "b", 2, "bb"));
        recordList.add(new Record(2, "a", 1, "aa"));
        recordList.add(new Record(2, "a", 2, "ab"));
        recordList.add(new Record(3, "c", 1, "aa"));
        return new TestSourceOperator(recordList, schema);
    }

    public static ArrayList<Record> getExampleSource1Records() {
        ArrayList<Record> recordList = new ArrayList<Record>();
        recordList.add(new Record(1, "a", 1, "aa"));
        recordList.add(new Record(1, "b", 2, "bb"));
        recordList.add(new Record(1, "c", 1, "bb"));
        recordList.add(new Record(2, "a", 1, "aa"));
        recordList.add(new Record(2, "a", 2, "ab"));
        recordList.add(new Record(3, "c", 1, "aa"));
        return recordList;
    }

    public static TestSourceOperator createExampleSource2() {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.stringType(1))
                .add("field3", Type.intType())
                .add("field4", Type.stringType(2));
        List<Record> recordList = new ArrayList<Record>();
        for (int i = 0; i < 10; i++) {
            recordList.add(new Record(1, "a", 1, "aa"));
        }
        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createExampleSource3(boolean leftSource) {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.intType())
                .add("field3", Type.intType())
                .add("field4", Type.stringType(1));
        List<Record> recordList = new ArrayList<Record>();
        if (leftSource) {
            recordList.add(new Record(5, 2, 1, "a"));
            recordList.add(new Record(2, 2, 1, "a"));
            recordList.add(new Record(1, 2, 1, "a"));
            recordList.add(new Record(1, 1, 3, "a"));
            recordList.add(new Record(1, 1, 2, "a"));
            recordList.add(new Record(1, 1, 1, "c"));
        } else {
            recordList.add(new Record(1, 2, 4, "a"));
            recordList.add(new Record(2, 2, 1, "b"));
            recordList.add(new Record(1, 3, 1, "a"));
            recordList.add(new Record(1, 1, 3, "a"));
            recordList.add(new Record(1, 1, 1, "b"));
            recordList.add(new Record(1, 1, 1, "a"));
        }
        return new TestSourceOperator(recordList, schema);
    }

    public static ArrayList<Record> getExampleSource3Records() {
        ArrayList<Record> result = new ArrayList<>();
        result.add((new Record(1, 1, 1, "b")).concat(new Record(1, 1, 1, "c")));
        result.add((new Record(1, 1, 1, "a")).concat(new Record(1, 1, 1, "c")));
        result.add((new Record(1, 1, 3, "a")).concat(new Record(1, 1, 3, "a")));
        result.add((new Record(2, 2, 1, "b")).concat(new Record(2, 2, 1, "a")));
        return result;
    }

    public static TestSourceOperator createExampleSource4(boolean leftSource) {
        Schema schema = new Schema()
                .add("field1", Type.intType())
                .add("field2", Type.intType())
                .add("field3", Type.intType())
                .add("field4", Type.stringType(1));
        List<Record> recordList = new ArrayList<Record>();
        if (leftSource) {
            recordList.add(new Record(5, 2, 1, "a"));
            recordList.add(new Record(2, 2, 1, "a"));
            recordList.add(new Record(1, 2, 1, "a"));
            recordList.add(new Record(1, 1, 3, "a"));
            recordList.add(new Record(1, 1, 2, "a"));
            recordList.add(new Record(1, 1, 1, "c"));
        } else {
            recordList.add(new Record(7, 1, 2, "a"));
            recordList.add(new Record(7, 1, 3, "b"));
            recordList.add(new Record(7, 2, 1, "a"));
            recordList.add(new Record(7, 3, 3, "a"));
            recordList.add(new Record(5, 4, 1, "b"));
            recordList.add(new Record(5, 4, 2, "a"));
        }
        return new TestSourceOperator(recordList, schema);
    }
}
