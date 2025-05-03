package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Represents a table within the DBMS.
 */
public class Table implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final String tableName;
	private final String[] columnNames;
	private ArrayList<Page> pages;
	private int pageSize;


	/**
	 * Constructs a new Table.
	 *
	 * @param tableName   The name of the table.
	 * @param pageSize    The maximum number of records per page.
	 * @param columnNames An array of column names for the table.
	 */
	public Table(String tableName, int pageSize, String[] columnNames)
	{
		this.tableName = tableName;
		this.pages = new ArrayList<Page>();
		this.pageSize = pageSize;
		this.columnNames = columnNames;
	}

	/**
	 * Gets the name of the table.
	 *
	 * @return The table name.
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Inserts a record into the table, creating new pages if necessary.
	 *
	 * @param record An array of values representing the record.
	 */
	public void insert(String[] record)
	{
		if (pages.isEmpty())
		{
			pages.add(new Page(pageSize,0));
		}

		Page lastPage = pages.get(pages.size() - 1);
		if (lastPage.isFull())
		{
			pages.add(new Page(pageSize,pages.size()));
			lastPage = pages.get(pages.size() - 1);
		}

		lastPage.insert(record);
		
		boolean storeTablePage = false;
		storeTablePage = FileManager.storeTablePage(tableName,lastPage.getPageNumber(), lastPage);
		if(!storeTablePage)
			System.err.println("Error: Table '" + tableName + "' could not be stored correctly.");
	}
	public ArrayList<String[]> getRecords() {
		ArrayList<String[]> allRecords = new ArrayList<>();

		for (Page page : pages) {
			allRecords.addAll(page.getRecords());  // Fetch records from each page
		}

		return allRecords;
	}

	public String[] getColumnNames() {
		return columnNames;
	}

	public Page getPage(int pageNumber) {
		if (pageNumber >= 0 && pageNumber < pages.size()) {
			return pages.get(pageNumber);
		} else {
			return null;
		}
	}


	public ArrayList<Page> getPages() {
		return pages;  // Returns the internal list of pages
	}

	public int getPagesCount() { return pages.size(); }

	public int getRecordsCount() {
		int numberOfRecords = 0;
		for (Page page : pages) {
			numberOfRecords += page.getRecordsCount();
		}
		return numberOfRecords;
	}
	public int getColumnIndex(String colName) {
		// Iterate through the array of column names
		for (int i = 0; i < this.columnNames.length; i++) {
			// Compare the input column name with the current column name in the array
			if (this.columnNames[i].equals(colName)) {
				// If names match, return the current index 'i'
				return i;
			}
		}
		// If the loop finishes without finding a match, the column name is invalid
		// Throw an exception to indicate that the column does not exist in this table
		throw new IllegalArgumentException("Column '" + colName + "' not found in table '" + tableName + "'.");
	}
	public String[] getRecordByGlobalIndex(int globalRecordIndex) {
		// Calculate which page this record is on
		int pageNumber = globalRecordIndex / this.pageSize;

		// Calculate the index of the record within that page
		int recordIndexInPage = globalRecordIndex % this.pageSize;

		// Use the FileManager to load the specific page from disk
		Page page = FileManager.loadTablePage(this.tableName, pageNumber);

		if (page != null) {
			// Check if the record index is valid for the records currently in this page
			if (recordIndexInPage < page.getRecords().size()) {
				return page.getRecord(recordIndexInPage);
			}

		}

		// If the page was null or the record index was invalid in that page
		System.err.println("Warning: Could not retrieve record with global index " + globalRecordIndex +
				". Calculated page: " + pageNumber + ", index in page: " + recordIndexInPage);
		return null; // Indicate record not found or could not be loaded
	}
}
