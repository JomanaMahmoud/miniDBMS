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
}
