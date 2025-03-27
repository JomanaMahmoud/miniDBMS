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

	public static void createTable(String tableName, String[] columnsNames)
	{

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
		return t.records;
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


	public static void main(String []args) throws IOException
	{


	}



}
