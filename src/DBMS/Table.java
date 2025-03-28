package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Represents a table within the DBMS.
 */
public class Table implements Serializable
{
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
		FileManager.storeTablePage(tableName,lastPage.getPageNumber(), lastPage);
	}
}
