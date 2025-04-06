package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Represents a page within a table.
 */
public class Page implements Serializable
{
    private int pageNumber;
    private ArrayList<String[]> records;
    private int pageSize;

    /**
     * Constructs a new Page.
     *
     * @param pageSize   The maximum number of records allowed on the page.
     * @param pageNumber The identifier of the page within the table.
     */
    public Page(int pageSize,int pageNumber)
    {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.records = new ArrayList<String[]>();

    }

    /**
     * Retrieves the current page number.
     *
     * @return the page number
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * Checks if the page is full.
     *
     * @return True if the page has reached its maximum capacity, otherwise false.
     */
    public boolean isFull()
    {
        return records.size() == pageSize;
    }

    /**
     * Inserts a new record into the page.
     *
     * @param record An array of values representing the record.
     */
    public void insert(String[] record)
    {
        records.add(record);
    }

    public ArrayList<String[]> getRecords() {
        return records;  // Assuming records is an ArrayList<String[]>
    }

    public String[] getRecord(int recordIndex) {
        if (recordIndex >= 0 && recordIndex < records.size()) {
            return records.get(recordIndex);
        } else {
            return null;
        }
    }

    public int getRecordsCount() { return records.size(); }
}
