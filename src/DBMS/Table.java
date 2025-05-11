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

	/**
	 * Retrieves all records stored in the table across all pages.
	 *
	 * @return An ArrayList of all records in the table.
	 */
	public ArrayList<String[]> getRecords() {
		ArrayList<String[]> allRecords = new ArrayList<>();

		for (Page page : pages) {
			allRecords.addAll(page.getRecords());  // Fetch records from each page
		}

		return allRecords;
	}

	/**
	 * Returns the array of column names for the table.
	 *
	 * @return An array of column names.
	 */
	public String[] getColumnNames() {
		return columnNames;
	}

	/**
	 * Retrieves a specific page by its page number.
	 *
	 * @param pageNumber The index of the page to retrieve.
	 * @return The Page object if it exists, otherwise null.
	 */
	public Page getPage(int pageNumber) {
		if (pageNumber >= 0 && pageNumber < pages.size()) {
			return pages.get(pageNumber);
		} else {
			return null;
		}
	}

	/**
	 * Retrieves the list of pages in the table.
	 *
	 * @return An ArrayList of Page objects.
	 */
	public ArrayList<Page> getPages() {
		return pages;  // Returns the internal list of pages
	}

	/**
	 * Gets the total number of pages in the table.
	 *
	 * @return The number of pages.
	 */
	public int getPagesCount() { return pages.size(); }

	/**
	 * Gets the total number of records in the table.
	 *
	 * @return The number of records across all pages.
	 */
	public int getRecordsCount() {
		int numberOfRecords = 0;
		for (Page page : pages) {
			numberOfRecords += page.getRecordsCount();
		}
		return numberOfRecords;
	}

	/**
	 * Retrieves the index of a given column name in the table.
	 *
	 * @param colName The name of the column to find.
	 * @return The index of the specified column.
	 * @throws IllegalArgumentException if the column does not exist.
	 */
	public int getColumnIndex(String colName) {
		for (int i = 0; i < this.columnNames.length; i++) {
			if (this.columnNames[i].equals(colName)) {
				return i;
			}
		}

		throw new IllegalArgumentException("Column '" + colName + "' not found in table '" + tableName + "'.");
	}

	/**
	 * Retrieves a record from the table based on its global index (across all pages).
	 *
	 * @param globalRecordIndex The global index of the record.
	 * @return The record as a String array, or null if not found.
	 */
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
