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
public class DBApp
{
	static int dataPageSize = -100;

	/**
	 * Creates a new table.
	 *
	 * @param tableName The name of the table you want to create.
	 * @param newtable  The instance of the table created
	 */

	public static void createTable(String tableName, String[] columnsNames) {
		if (tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null");
		}
		if (columnsNames == null || columnsNames.length == 0) {
			throw new IllegalArgumentException("column name cannot be null");
		}
		if (FileManager.loadTable(tableName)!=null){
			throw new IllegalArgumentException("Table already exists");
		}
		Table newTable = new Table(tableName, dataPageSize, columnsNames);
		FileManager.storeTable(tableName,newTable);
	}

	/**
	 * Inserts a record into the specified table.
	 *
	 * @param tableName The name of the table to insert into.
	 * @param record    An array of values representing the record.
	 */
	public static void insert(String tableName, String[] record)
	{
			Table t = FileManager.loadTable(tableName);
			if(t != null)
			{
				t.insert(record);
			}
			else
			{
				System.out.println("Table " + tableName + " not found.");
			}
	}

	public static ArrayList<String []> select(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		return new ArrayList<String []>();
	}

	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{

		return new ArrayList<String[]>();
	}

	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{

		return new ArrayList<String[]>();
	}

	public static String getFullTrace(String tableName)
	{

		return "";
	}

	public static String getLastTrace(String tableName)
	{

		return "";
	}


	public static void main(String[] args) {
		try {
			// Define the table name and columns
			String[] cols = {"id", "name", "major", "semester", "gpa"};

			// Call createTable function
			createTable("student", cols);

			// Print confirmation message
			System.out.println("Table 'student' created successfully!");

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}





}
