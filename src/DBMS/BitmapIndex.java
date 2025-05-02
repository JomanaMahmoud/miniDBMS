package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

public class BitmapIndex implements Serializable {
    private String tableName;
    private String columnName;
    private HashMap<String, BitSet> BitMapIndex;

    public BitmapIndex(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public HashMap<String, BitSet> getBitMapIndex() {
        return BitMapIndex;
    }

    public void createBitMapIndex(ArrayList<String[]> records, int colIndex){
        BitMapIndex = new HashMap<String,BitSet>();
        for(int i = 0; i < records.size(); i++){
            String[] record = records.get(i);
            if(BitMapIndex.containsKey(record[colIndex])) {
                BitMapIndex.get(record[colIndex]).set(i);
            }
            else{
                BitSet bitSet = new BitSet();
                bitSet.set(i);
                BitMapIndex.put(record[colIndex], bitSet);
            }
        }
    }

    public void insertIntoBitMapIndex(String value, int insertionIndex){
        if(BitMapIndex.containsKey(value)){
            BitSet bitSet = BitMapIndex.get(value);
            bitSet.set(insertionIndex);
            BitMapIndex.put(value, bitSet);
        }
        else{
            BitSet bitSet = new BitSet();
            bitSet.set(insertionIndex);
            BitMapIndex.put(value, bitSet);
        }
    }

    public String getBitMapIndexByValue(String value, int BitMapLength) {
        BitSet bitMap = BitMapIndex.get(value);
        if (bitMap != null) {
            return  toBitString(bitMap, BitMapLength);
        }
        else{
            System.out.println("Error: Value " + value + " not found in Column " + columnName + " in Table " + tableName);
        }
        return null;
    }

    public static String toBitString(BitSet bitSet, int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        return sb.toString();
    }

}
