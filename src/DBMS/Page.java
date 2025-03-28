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
        records = new ArrayList<String[]>();
        this.pageSize = pageSize;
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
}
