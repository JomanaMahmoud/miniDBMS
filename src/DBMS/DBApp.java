package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;
/**
 * The main class representing the Database Management System (DBMS).
 * It provides functionalities for creating tables, inserting records, and selecting data.
 */
public class DBApp {
	static int dataPageSize = -100;

	/**
	 * Creates a new table.
	 *
	 * @param tableName The name of the table you want to create.
	 * @param columnsNames The list of column names for the table.
	 */

	public static void createTable(String tableName, String[] columnsNames) {
		if (tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty");
		}
		if (columnsNames == null || columnsNames.length == 0) {
			throw new IllegalArgumentException("Column names cannot be null or empty");
		}

		if (FileManager.loadTable(tableName) != null) {
			throw new IllegalArgumentException("Table '" + tableName + "' already exists.");
		}

		File tablesDir = new File("Tables");
		if (!tablesDir.exists() && !tablesDir.mkdirs()) {
			return;
		}

		if (!tablesDir.canWrite()) {
			return;
		}

		Table newTable = new Table(tableName, 100, columnsNames);
		FileManager.storeTable(tableName, newTable);
	}


	/**
	 * Inserts a record into the specified table.
	 *
	 * @param tableName The name of the table to insert into.
	 * @param record    An array of values representing the record.
	 */
	public static void insert(String tableName, String[] record) {
		Table t = FileManager.loadTable(tableName);
		if (t != null) {
			t.insert(record);
			FileManager.storeTable(tableName, t);
		} else {
			System.out.println("Table " + tableName + " not found.");
		}
	}

	public static ArrayList<String[]> select(String tableName) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> result = t.getRecords();
		return result;
	}

	public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
		Table t = FileManager.loadTable(tableName);
		if (t == null) {
			System.out.println("Table " + tableName + " not found.");
			return new ArrayList<>();
		}

		Page page = t.getPage(pageNumber);
		if (page == null) {
			System.out.println("Page " + pageNumber + " not found.");
			return new ArrayList<>();
		}

		String[] record = page.getRecord(recordNumber);
		ArrayList<String[]> result = new ArrayList<>();

		if (record != null) {
			result.add(record);
		}

		return result;
	}



	public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
		Table t = FileManager.loadTable(tableName);

		if (t == null) {
			System.out.println("Table " + tableName + " not found.");
			return new ArrayList<>();
		}

		ArrayList<String[]> records = t.getRecords();
		ArrayList<String[]> result = new ArrayList<>();


		String[] columnNames = t.getColumnNames();
		ArrayList<Integer> colIndexes = new ArrayList<>();

		for (String col : cols) {
			int index = -1;

			for (int i = 0; i < columnNames.length; i++) {
				if (columnNames[i].equals(col)) {
					index = i;
					break;
				}
			}

			if (index == -1) {
				System.out.println("Column " + col + " not found.");
				return new ArrayList<>();
			}

			colIndexes.add(index);
		}

		for (String[] record : records) {
			boolean match = true;
			for (int i = 0; i < cols.length; i++) {
				int colIndex = colIndexes.get(i);
				if (!record[colIndex].equals(vals[i])) {
					match = false;
					break;
				}
			}
			if (match) {
				result.add(record);
			}
		}

		return result;
	}


	public static String getFullTrace(String tableName) {

		return "";
	}

	public static String getLastTrace(String tableName) {

		return "";
	}


	public static void main(String[] args) {

	}
}
