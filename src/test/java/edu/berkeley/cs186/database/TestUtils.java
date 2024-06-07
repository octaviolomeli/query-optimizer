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
        for (int i = numRecords + start; i > start; i--) {
            recordList.add(new Record(i, i, i));
        }
        return new TestSourceOperator(recordList, schema);
    }
}
