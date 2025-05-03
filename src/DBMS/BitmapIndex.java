package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

/**
 * Represents a Bitmap Index for a specific column in a database table.
 * The index maps each unique value in the column to a BitSet representing
 * the rows in which the value appears.
 */
public class BitmapIndex implements Serializable {
    private String tableName;
    private String columnName;
    private HashMap<String, BitSet> BitMapIndex;

    /**
     * Constructs a BitmapIndex for the specified table and column.
     *
     * @param tableName  the name of the table
     * @param columnName the name of the column to index
     * */
    public BitmapIndex(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    /**
     * Returns the name of the table.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the name of the column.
     *
     * @return the column name
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the internal map of the bitmap index.
     *
     * @return a HashMap mapping values to BitSets
     */
    public HashMap<String, BitSet> getBitMapIndex() {
        return BitMapIndex;
    }


    /**
     * Creates a bitmap index based on a list of records.
     * Each record is assumed to be a String array, and the column of interest
     * is specified by colIndex.
     *
     * @param records  the list of records
     * @param colIndex the index of the column to be indexed
     */
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

    /**
     * Inserts a new entry into the bitmap index at the given position.
     * Updates the BitSet for the specified value to mark the insertion index.
     *
     * @param value          the value to insert
     * @param insertionIndex the index in the BitSet to be set
     */
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

    /**
     * Retrieves a binary string representation of the bitmap for a specific value.
     *
     * @param value         the value to retrieve the bitmap for
     * @param BitMapLength  the expected length of the bitmap string
     * @return a binary string (e.g., "0100") representing the positions of the value
     */
    public String getBitMapIndexByValue(String value, int BitMapLength) {
        BitSet bitMap = BitMapIndex.get(value);
        if (bitMap != null)
            return  toBitString(bitMap, BitMapLength);
        else
            return toZeroBitString(BitMapLength);
    }

    /**
     * Converts a BitSet to a binary string of the specified size.
     *
     * @param bitSet the BitSet to convert
     * @param size   the length of the resulting string
     * @return a binary string representation of the BitSet
     */
    public static String toBitString(BitSet bitSet, int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        return sb.toString();
    }

    /**
     * Returns a binary string of all zeroes of the given size.
     * Useful for representing values not present in the index.
     *
     * @param size the length of the string
     * @return a binary string of zeroes
     */
    public static String toZeroBitString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++)
            sb.append('0');
        return sb.toString();
    }

}
