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
		return new ArrayList<>();
	}


	public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {

		return new ArrayList<String[]>();
	}

	public static String getFullTrace(String tableName) {

		return "";
	}

	public static String getLastTrace(String tableName) {

		return "";
	}


	public static void main(String[] args) {
		//String[] cols = {"id","name","major","semester","gpa"};
		//createTable("student", cols);
		String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
		insert("student", r1);

		String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
		insert("student", r2);

		String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
		insert("student", r3);

		String[] r4 = {"4", "stud4", "DMET", "9", "1.2"};
		insert("student", r4);

		String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
		insert("student", r5);
		System.out.println("Output of selecting the whole table content:");
		ArrayList<String[]> result1 = select("student",1,1);
		for (String[] array : result1) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}

	}
}
