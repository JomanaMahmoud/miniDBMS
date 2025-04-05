package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.junit.Test;
/**
 * The main class representing the Database Management System (DBMS).
 * It provides functionalities for creating tables, inserting records, and selecting data.
 */
public class DBApp {
	static int dataPageSize = -100;

	// A map to store traces for each table.
	private static Map<String, ArrayList<String>> tableTraces = new HashMap<>();
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

		Table newTable = new Table(tableName,dataPageSize, columnsNames);
		FileManager.storeTable(tableName, newTable);

		// Initialize the trace list for this table if not already present
		if (!tableTraces.containsKey(tableName)) {
			tableTraces.put(tableName, new ArrayList<>());
		}
		// Log the creation trace directly
		tableTraces.get(tableName).add("Created table with columns: " + String.join(", ", columnsNames));
	}


	/**
	 * Inserts a record into the specified table.
	 *
	 * @param tableName The name of the table to insert into.
	 * @param record    An array of values representing the record.
	 */
	public static void insert(String tableName, String[] record) {
		if (tableName == null || record == null) {
	        throw new IllegalArgumentException("Table name and record must not be null.");
	    }
		
		Table t = FileManager.loadTable(tableName);
		if (t != null) {
			long startTime = System.nanoTime();  // Start time for execution time calculation
			t.insert(record);
			boolean storeTable = false;
			storeTable = FileManager.storeTable(tableName, t);
			if(!storeTable) {
				System.err.println("Error: Table '" + tableName + "' could not be stored correctly.");
			}

			long endTime = System.nanoTime();  // End time for execution time calculation
			long executionTime = (endTime - startTime) / 1000000;  // Convert to milliseconds

			// Log the insert trace directly with execution time
			if (!tableTraces.containsKey(tableName)) {
				tableTraces.put(tableName, new ArrayList<>());
			}
			tableTraces.get(tableName).add("Inserted: " + Arrays.toString(record) + ", at page number: " + t.size() + ", execution time (mil):" + executionTime);



		} else {
			System.err.println("Error: Table '" + tableName + "' not found.");
		}
	}

	public static ArrayList<String[]> select(String tableName) {
		long startTime = System.nanoTime();  // Start time for execution time calculation

		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> result = t.getRecords();

		long endTime = System.nanoTime();  // End time for execution time calculation
		long executionTime = (endTime - startTime) / 1000000;  // Convert to milliseconds

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

		ArrayList<String> trace = tableTraces.get(tableName);
		if (trace == null) {
			return "No traces found for table " + tableName;
		}
		return String.join("\n", trace);	}

	public static String getLastTrace(String tableName) {

		ArrayList<String> trace = tableTraces.get(tableName);
		if (trace == null || trace.isEmpty()) {
			return "No traces found for table " + tableName;
		}
		return trace.get(trace.size() - 1);
	}


	public static void main(String[] args)throws IOException {
		String[] cols = {"id","name","major","semester","gpa"}; createTable("student", cols); String[] r1 = {"1", "stud1", "CS", "5", "0.9"}; insert("student", r1); String[] r2 = {"2", "stud2", "BI", "7", "1.2"}; insert("student", r2); String[] r3 = {"3", "stud3", "CS", "2", "2.4"}; insert("student", r3); String[] r4 = {"4", "stud4", "DMET", "9", "1.2"}; insert("student", r4); String[] r5 = {"5", "stud5", "BI", "4", "3.5"}; insert("student", r5); System.out.println("Output of selecting the whole table content:"); ArrayList<String[]> result1 = select("student"); for (String[] array : result1) { for (String str : array) { System.out.print(str + " "); } System.out.println(); } System.out.println("--------------------------------"); System.out.println("Output of selecting the output by position:"); ArrayList<String[]> result2 = select("student", 1, 1); for (String[] array : result2) { for (String str : array) { System.out.print(str + " "); } System.out.println(); } System.out.println("--------------------------------"); System.out.println("Output of selecting the output by column condition:"); ArrayList<String[]> result3 = select("student", new String[]{"gpa"}, new String[]{"1.2"}); for (String[] array : result3) {
			for (String str : array) { System.out.print(str + " "); } System.out.println(); } System.out.println("--------------------------------"); System.out.println("Full Trace of the table:"); System.out.println(getFullTrace("student")); System.out.println("--------------------------------"); System.out.println("Last Trace of the table:"); System.out.println(getLastTrace("student")); System.out.println("--------------------------------"); System.out.println("The trace of the Tables Folder:"); System.out.println(FileManager.trace()); FileManager.reset(); System.out.println("--------------------------------"); System.out.println("The trace of the Tables Folder after resetting:"); System.out.println(FileManager.trace());
	}
	}

