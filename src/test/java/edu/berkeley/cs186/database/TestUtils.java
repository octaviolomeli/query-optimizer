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

    public static Record createRecordWith3Values(ArrayList<DataBox> values) {
        if (values.size() != 3) {
            throw new UnsupportedOperationException();
        }
        return new Record(values.get(0), values.get(1), values.get(2));
    }

    public static TestSourceOperator createSourceWithSame3Values(ArrayList<DataBox> values, int numRecords) {
        Schema schema = new Schema()
                .add("field1", values.get(0).type())
                .add("field2", values.get(1).type())
                .add("field3", values.get(2).type());
        List<Record> recordList = new ArrayList<Record>();
        for (int i = 0; i < numRecords; i++) {
            recordList.add(createRecordWith3Values(values));
        }
        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createEmptySourceWith3Fields(ArrayList<DataBox> valuesForFields) {
        Schema schema = new Schema()
                .add("field1", valuesForFields.get(0).type())
                .add("field2", valuesForFields.get(1).type())
                .add("field3", valuesForFields.get(2).type());
        List<Record> recordList = new ArrayList<Record>();
        return new TestSourceOperator(recordList, schema);
    }
}
