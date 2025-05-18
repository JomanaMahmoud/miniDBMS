package DBMS;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * The main class representing the Database Management System (DBMS).
 * It provides functionalities for creating tables, inserting records, and selecting data.
 */
public class DBApp {
	static int dataPageSize = 2;

	// A map to store traces for each table.
	private static Map<String, ArrayList<String>> tableTraces = new HashMap<>();
	private static HashMap<String, ArrayList<String>> tableIndices = new HashMap<String, ArrayList<String>>();

	// MILESTONE 1
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

		tableTraces.put(tableName, new ArrayList<>());
		tableIndices.put(tableName, new ArrayList<String>());

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
		// Log the creation trace
		tableTraces.get(tableName).add("Table created name:" + tableName + ", columnsNames:" + Arrays.toString(columnsNames));

	}


	/**
	 * Inserts a record into the specified table.
	 *
	 * @param tableName The name of the table to insert into.
	 * @param record    An array of values representing the record.
	 */
	public static void insert(String tableName, String[] record) {
		if (tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		if (record == null || record.length == 0) {
			throw new IllegalArgumentException("Record cannot be null or empty.");
		}

		Table t = FileManager.loadTable(tableName);
		if (t != null) {
			long startTime = System.nanoTime();  // Start time for execution time calculation
			t.insert(record);

			if(tableIndices.containsKey(tableName)) {
				ArrayList<String> indices = tableIndices.get(tableName);
				String[] colNames = t.getColumnNames();
				String colName;
				for (int i = 0; i < colNames.length; i++) {
					colName = colNames[i];
					if (indices != null && indices.contains(colName)) {
						BitmapIndex b = FileManager.loadTableIndex(tableName, colName);
						if(b != null){
							b.insertIntoBitMapIndex(record[i], t.getRecordsCount()-1);
							FileManager.storeTableIndex(tableName,colName,b);
						}
					}
				}
			}

			boolean storeTable = FileManager.storeTable(tableName, t);
			if(!storeTable) {
				System.err.println("Error: Table '" + tableName + "' could not be stored correctly.");
			}

			long endTime = System.nanoTime();  // End time for execution time calculation
			long executionTime = (endTime - startTime) / 1000000;  // Convert to milliseconds

			// Log the insert trace directly with execution time
			int lastPageNumber = t.getPages().get(t.getPages().size() - 1).getPageNumber();
			tableTraces.get(tableName).add("Inserted:[" + String.join(", ", record) + "], at page number:" + lastPageNumber + ", execution time (mil):" + executionTime);

		} else {
			System.err.println("Error: Table '" + tableName + "' not found.");
		}
	}

	public static ArrayList<String[]> select(String tableName) {
		if(tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		long startTime = System.nanoTime();  // Start time for execution time calculation

		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> result = t.getRecords();

		long endTime = System.nanoTime();  // End time for execution time calculation
		long executionTime = (endTime - startTime) / 1000000;  // Convert to milliseconds


		// Log the select trace directly with execution time
		tableTraces.get(tableName).add("Select all pages:" + t.getPages().size() + ", records:" + result.size() + ", execution time (mil):" + executionTime);


		return result;
	}

	public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
		if(tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		long startTime = System.nanoTime();  // Start time for execution time calculation


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

		long endTime = System.nanoTime();
		long executionTime = (endTime - startTime) / 1000000;


		tableTraces.get(tableName).add("Select pointer page:" + pageNumber + ", record:" + recordNumber + ", total output count:" + result.size() + ", execution time (mil):" + executionTime);

		return result;
	}



	public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
		if(tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		long startTime = System.nanoTime();
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

		long endTime = System.nanoTime();
		long executionTime = (endTime - startTime) / 1000000;

		Map<Integer, Integer> pageMatchCounts = new TreeMap<>();
		for (Page page : t.getPages()) {
			int pageNum = page.getPageNumber();
			int count = 0;
			for (String[] rec : page.getRecords()) {
				boolean match = true;
				for (int i = 0; i < cols.length; i++) {
					int colIndex = colIndexes.get(i);
					if (!rec[colIndex].equals(vals[i])) {
						match = false;
						break;
					}
				}
				if (match) count++;
			}
			if (count > 0) {
				pageMatchCounts.put(pageNum, count);
			}
		}
		String entrySet = pageMatchCounts.entrySet().stream()
				.map(e -> "[" + e.getKey() + ", " + e.getValue() + "]")
				.collect(Collectors.joining(", ", "[", "]"));

		tableTraces.get(tableName).add("Select condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
				", Records per page:" + entrySet +
				", records:" + result.size() + ", execution time (mil):" + executionTime);

		return result;
	}

	public static String getFullTrace(String tableName) {
		if(tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		ArrayList<String> trace = (ArrayList<String>) tableTraces.get(tableName).clone();
		if (trace == null) {
			return "No traces found for table " + tableName;
		}
		else {
			Table t = FileManager.loadTable(tableName);
			List<String> indexedCols = tableIndices.getOrDefault(tableName, new ArrayList<>());
			Collections.sort(indexedCols);
			trace.add("Pages Count: " + t.getPagesCount() + ", Records Count: " + t.getRecordsCount() + ", Indexed Columns: " + indexedCols.toString());
		}
		return String.join("\n", trace);
	}

	public static String getLastTrace(String tableName) {
		if(tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		ArrayList<String> trace = tableTraces.get(tableName);
		if (trace == null || trace.isEmpty()) {
			return "No traces found for table " + tableName;
		}
		return trace.get(trace.size() - 1);
	}

	// MILESTONE 2
	/**
	 * Creates a bitmap index for a specific column in a given table.
	 * <p>
	 * The method first loads the table, identifies the index of the specified column,
	 * and then creates a bitmap index for that column. The index is stored in memory
	 * and persisted using the FileManager. It also logs the time taken to create the index
	 * and stores the column in the table's index registry.
	 *
	 * @param tableName the name of the table for which to create the bitmap index.
	 * @param colName   the name of the column to index.
	 * @throws IllegalArgumentException if either {@code tableName} or {@code colName} is {@code null}.
	 */
	public static void createBitMapIndex(String tableName, String colName) {
		if (tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		if(colName == null || colName == "" || colName == " ") {
			throw new IllegalArgumentException("Column name cannot be null or empty.");
		}

		Table t = FileManager.loadTable(tableName);
		if (t != null) {
			ArrayList<String[]> records = t.getRecords();
			String[] colNames = t.getColumnNames();

			int index = -1;
			for (int i = 0;i<colNames.length;i++){
				if (colNames[i].equals(colName)) {
					index = i;
					break;
				}
			}

			if (index == -1) {
				System.err.println("Error: Column '" + colName + "' not found.");
			}
			else {
				long startTime = System.nanoTime();
				BitmapIndex b = new BitmapIndex(tableName, colName,t.getRecordsCount());
				b.createBitMapIndex(records,index);

				if(tableIndices.containsKey(tableName)) {
					tableIndices.get(tableName).add(colName);
				}
				else {
					ArrayList<String> newList = new ArrayList<>();
					newList.add(colName);
					tableIndices.put(tableName, newList);
				}

				boolean storeTable = FileManager.storeTableIndex(tableName,colName,b);
				if (!storeTable)
					System.err.println("Error: Table '" + tableName + "' could not be stored correctly.");

				long endTime = System.nanoTime();
				long executionTime = (endTime - startTime) / 1000000;

				tableTraces.get(tableName).add("Index created for column: " + colName + ", execution time (mil):" + executionTime);
			}
		}
		else {
			System.err.println("Error: Table '" + tableName + "' not found.");
		}
	}


	/**
	 * Retrieves the bitmap representation of a specific value in a column for a given table.
	 * <p>
	 * The method loads the table and the corresponding bitmap index, then queries the bitmap
	 * to return the bit pattern associated with the specified value.
	 *
	 * @param tableName the name of the table.
	 * @param colName   the column from which the value's bitmap is requested.
	 * @param value     the value to look up in the bitmap index.
	 * @return a string representing the bitmap of the value, or {@code null} if the index is not found.
	 * @throws IllegalArgumentException if either {@code tableName} or {@code colName} is {@code null}.
	 */
	public static String getValueBits(String tableName, String colName, String value){
		if (tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		if(colName == null || colName == "" || colName == " ") {
			throw new IllegalArgumentException("Column name cannot be null or empty.");
		}

		if(value == null || value == "" || value == " ") {
			throw new IllegalArgumentException("Value cannot be null or empty.");
		}

		BitmapIndex b = FileManager.loadTableIndex(tableName,colName);
		String result = null;
		if (b != null)
			result = b.getBitMapIndexByValue(value);
		else
			System.err.println("Error: Index for Column '" + colName + "' not found.");
		return result;
	}

	/**
	 * Validates records of a table by checking if the corresponding pages exist.
	 * <p>
	 * This method identifies records from pages that are missing on disk and returns them.
	 * It also logs the number of records found to be missing from the file system.
	 *
	 * @param tableName the name of the table to validate.
	 * @return a list of string arrays, each representing a missing record.
	 */
	public static ArrayList<String []> validateRecords(String tableName){
		if(tableName == null || tableName == "" || tableName == " ") {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}

		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> resultRecords = new ArrayList<>();
		for(Page page : t.getPages()){
			if(FileManager.loadTablePage(tableName,page.getPageNumber())==null)
			{
				resultRecords.addAll(page.getRecords());
			}
		}
		tableTraces.get(tableName).add("Validating records: " + resultRecords.size() + " records missing.");
		return resultRecords;
	}

	public static void recoverRecords(String tableName, ArrayList<String[]> missing){
//		if(tableName == null || tableName.equals("") || tableName.equals(" ")) {
//			throw new IllegalArgumentException("Table name cannot be null or empty.");
//		}
//
//		if(missing == null || missing.isEmpty()) return;

		// Ensure the trace exists
		if(!tableTraces.containsKey(tableName)) {
			tableTraces.put(tableName, new ArrayList<>());
		}

		Table t = FileManager.loadTable(tableName);
		ArrayList<Integer> missingPages = new ArrayList<Integer>();
		for(Page page : t.getPages()){
			int pageNumber = page.getPageNumber();
			if(FileManager.loadTablePage(tableName,pageNumber)==null)
			{
				missingPages.add(pageNumber);
				FileManager.storeTablePage(tableName,pageNumber,page);
			}
		}

		// Update the trace
		tableTraces.get(tableName).add("Recovering " + missing.size() + " records in pages: " + missingPages.toString() +".");
	}

	public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) { // Removed throws DBAppException
		// Removed startTime here

		Table table = FileManager.loadTable(tableName);
		if (table == null) {
			System.err.println("Error: Table '" + tableName + "' not found during selectIndex.");
			// No trace added to tableTraces if table is not found
			return new ArrayList<>(); // Return empty list
		}

		// Validate that queried columns exist in the table before proceeding
		try {
			String[] tableCols = table.getColumnNames();
			HashSet<String> tableColNamesSet = new HashSet<>(Arrays.asList(tableCols));
			for(String col : cols) {
				if (!tableColNamesSet.contains(col)) {
					System.err.println("Error: Column '" + col + "' not found in table '" + tableName + "' during selectIndex.");
					// Add error trace to table if it exists
//					if (tableTraces.containsKey(tableName)) {
//						tableTraces.get(tableName).add("Select Index Error: Column '" + col + "' not found.");
//					}
					return new ArrayList<>(); // Return empty list on invalid column
				}
			}
		} catch (Exception e) { // Catch errors like table.getColumnNames() failing
			System.err.println("Error checking columns for table '" + tableName + "' during selectIndex: " + e.getMessage());
//			if (tableTraces.containsKey(tableName)) {
//				tableTraces.get(tableName).add("Select Index Error: Checking columns failed: " + e.getMessage());
//			}
			return new ArrayList<>(); // Return empty list on error
		}


		// Check if table has any indices registered
		if (!tableIndices.containsKey(tableName) || tableIndices.get(tableName).isEmpty()) {
			// Case 4: No indices defined for the table at all, or empty index list
			return selectCase4_NoIndexedColumns(table, cols, vals);
		} else {
			// Identify which query columns are indexed
			HashSet<String> indexedColsInTable = new HashSet<>(tableIndices.get(tableName));
			ArrayList<Integer> indexedColIndicesInQuery = new ArrayList<>(); // Indices (positions) in the 'cols' array
			ArrayList<Integer> notIndexedColIndicesInQuery = new ArrayList<>(); // Indices (positions) in the 'cols' array

			for (int i = 0; i < cols.length; i++) {
				if (indexedColsInTable.contains(cols[i])) {
					indexedColIndicesInQuery.add(i);
				} else {
					notIndexedColIndicesInQuery.add(i);
				}
			}

			// Dispatch based on the number of indexed columns found in the query
			if (indexedColIndicesInQuery.size() == 0) {
				// Case 4: No indexed columns in the query
				return selectCase4_NoIndexedColumns(table, cols, vals);
			} else if (indexedColIndicesInQuery.size() == cols.length) {
				// Case 1: All columns in the query are indexed
				return selectCase1_AllIndexedColumns(table, cols, vals, indexedColIndicesInQuery);
			} else if (indexedColIndicesInQuery.size() == 1) {
				// Case 3: Exactly one column in the query is indexed
				return selectCase3_OneIndexedColumn(table, cols, vals, indexedColIndicesInQuery.get(0));
			} else { // indexedColIndicesInQuery.size() > 1 && indexedColIndicesInQuery.size() < cols.length
				// Case 2: Multiple but not all columns in the query are indexed
				return selectCase2_SomeIndexedColumns(table, cols, vals, indexedColIndicesInQuery, notIndexedColIndicesInQuery);
			}
		}

	}


	private static ArrayList<String[]> selectCase1_AllIndexedColumns(Table table, String[] cols, String[] vals, ArrayList<Integer> indexedColIndicesInQuery) { // Removed throws DBAppException
		long startTime = System.nanoTime(); // Start time for trace
		ArrayList<String[]> result = new ArrayList<>(); // Prepare result list
		int indexedSelectionCount = 0; // For trace - cardinality of final combined BitSet

		try {
			Map<String, BitmapIndex> loadedIndices = new HashMap<>();
			// Load all required index bitmaps once
			for (int i : indexedColIndicesInQuery) {
				String colName = cols[i];
				BitmapIndex index = FileManager.loadTableIndex(table.getTableName(), colName);
				if (index == null) {
					long endTime = System.nanoTime();
					long executionTime = (endTime - startTime) / 1000000;
					System.err.println("Error: Index not found for column: '" + colName + "' unexpectedly during selectCase1 for table " + table.getTableName());
//					tableTraces.get(table.getTableName()).add("Select Case 1 Error: Index not found for " + colName + ", execution time (mil):" + executionTime);
					return result; // Return empty list on error
				}
				loadedIndices.put(colName, index);
			}


			// Perform bitwise AND
			BitSet combinedBitSet = null; // Initialize outside loop
			boolean firstIndex = true;
			for (int i : indexedColIndicesInQuery) {
				String colName = cols[i];
				String value = vals[i];
				BitmapIndex index = loadedIndices.get(colName); // Get already loaded index

				BitSet currentBitSet = index.getBitMapIndex().get(value);

				if (currentBitSet == null) {
					// If a required value doesn't exist in an index, no records match ALL conditions
					long endTime = System.nanoTime();
					long executionTime = (endTime - startTime) / 1000000;

					Arrays.sort(cols);
					Arrays.sort(vals);
					// Match PDF format for early exit trace
					tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
							", Indexed columns: " + Arrays.toString(cols) +
							", Indexed selection count: 0, Final count: 0, execution time (mil):" + executionTime);
					return result;
				}

				if (firstIndex) {
					combinedBitSet = (BitSet) currentBitSet.clone(); // Clone to avoid modifying original index's BitSet
					firstIndex = false;
				} else {
					combinedBitSet.and(currentBitSet);
					// If at any point the combined set becomes empty, no need to continue
					if (combinedBitSet.isEmpty()) {
						long endTime = System.nanoTime();
						long executionTime = (endTime - startTime) / 1000000;

						Arrays.sort(cols);
						Arrays.sort(vals);
						// Match PDF format for early exit trace
						tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
								", Indexed columns: " + Arrays.toString(cols) +
								", Indexed selection count: 0, Final count: 0, execution time (mil):" + executionTime);
						return result;
					}
				}
			}

			// combinedBitSet now contains the global record indices that match all indexed conditions.
			// If indexedColIndicesInQuery was empty (should be caught by dispatch, but defensive check)
			if (combinedBitSet != null) {
				indexedSelectionCount = combinedBitSet.cardinality(); // Calculate cardinality for trace
				// Iterate through the set bits of the combined result
				for (int i = combinedBitSet.nextSetBit(0); i >= 0; i = combinedBitSet.nextSetBit(i + 1)) {
					// Load the specific record corresponding to global index 'i' from disk
					// *** PLACEHOLDER: Use your implemented method to load a record by its global index ***
					String[] record = table.getRecordByGlobalIndex(i); // Call your method here

					if (record != null) {
						result.add(record);
					} else {
						// Handle case where record index is invalid or record cannot be loaded
						System.err.println("Warning: Could not load record with global index: " + i + " during selectCase1 for table " + table.getTableName());
						// Continue to the next index even if one record fails to load
					}
				}
			}


		} catch (Exception e) { // Catch broader exceptions during index ops or loading
			long endTime = System.nanoTime();
			long executionTime = (endTime - startTime) / 1000000;
			System.err.println("Error during selectCase1_AllIndexedColumns for table '" + table.getTableName() + "': " + e.getMessage());
			e.printStackTrace(); // Print stack trace for debugging
//			tableTraces.get(table.getTableName()).add("Select Case 1 Error: " + e.getMessage() + ", execution time (mil):" + executionTime);
			return new ArrayList<>(); // Return empty list on error
		}

		long endTime = System.nanoTime(); // End time for trace
		long executionTime = (endTime - startTime) / 1000000;

		Arrays.sort(cols);
		Arrays.sort(vals);
		// Trace for successful completion (matching PDF output format)
		tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
				", Indexed columns: " + Arrays.toString(cols) + // Use cols array for trace
				", Indexed selection count: " + indexedSelectionCount + // Cardinality of combined index result
				", Final count: " + result.size() +
				", execution time (mil):" + executionTime);
		return result;
	}


	private static ArrayList<String[]> selectCase2_SomeIndexedColumns(Table table, String[] cols, String[] vals, ArrayList<Integer> indexedColIndicesInQuery, ArrayList<Integer> notIndexedColIndicesInQuery) { // Removed throws DBAppException
		long startTime = System.nanoTime(); // Start time for trace
		ArrayList<String[]> result = new ArrayList<>(); // Prepare result list
		int indexedSelectionCount = 0; // Counter for cardinality before linear filtering (for trace)

		try {
			Map<String, BitmapIndex> loadedIndices = new HashMap<>();
			// Load indexed columns' bitmaps once
			for (int i : indexedColIndicesInQuery) {
				String colName = cols[i];
				BitmapIndex index = FileManager.loadTableIndex(table.getTableName(), colName);
				if (index == null) {
					long endTime = System.nanoTime();
					long executionTime = (endTime - startTime) / 1000000;
					System.err.println("Error: Index not found for column: '" + colName + "' unexpectedly during selectCase2 for table " + table.getTableName());
//					tableTraces.get(table.getTableName()).add("Select Case 2 Error: Index not found for " + colName + ", execution time (mil):" + executionTime);
					return result;
				}
				loadedIndices.put(colName, index);
			}

			// Perform bitwise AND on indexed columns
			BitSet combinedBitSet = null; // Initialize outside loop
			boolean firstIndex = true;
			for (int i : indexedColIndicesInQuery) {
				String colName = cols[i];
				String value = vals[i];
				BitmapIndex index = loadedIndices.get(colName); // Get already loaded index

				BitSet currentBitSet = index.getBitMapIndex().get(value);

				if (currentBitSet == null) {
					// If a required value doesn't exist in an index, no records match ALL indexed conditions
					long endTime = System.nanoTime();
					long executionTime = (endTime - startTime) / 1000000;
					ArrayList<String> indexedColNames = new ArrayList<>();
					for(int idx : indexedColIndicesInQuery) indexedColNames.add(cols[idx]);
					ArrayList<String> notIndexedColNames = new ArrayList<>();
					for(int idx : notIndexedColIndicesInQuery) notIndexedColNames.add(cols[idx]);

					Collections.sort(indexedColNames);
					Collections.sort(notIndexedColNames);
					// Match PDF format for early exit trace
					tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
							", Indexed columns: " + indexedColNames.toString() +
							", Indexed selection count: 0, Non Indexed: " + notIndexedColNames.toString() +
							", Final count: 0, execution time (mil):" + executionTime);
					return result;
				}

				if (firstIndex) {
					combinedBitSet = (BitSet) currentBitSet.clone();
					firstIndex = false;
				} else {
					combinedBitSet.and(currentBitSet);
					if (combinedBitSet.isEmpty()) {
						long endTime = System.nanoTime();
						long executionTime = (endTime - startTime) / 1000000;
						ArrayList<String> indexedColNames = new ArrayList<>();
						for(int idx : indexedColIndicesInQuery) indexedColNames.add(cols[idx]);
						ArrayList<String> notIndexedColNames = new ArrayList<>();
						for(int idx : notIndexedColIndicesInQuery) notIndexedColNames.add(cols[idx]);

						Collections.sort(indexedColNames);
						Collections.sort(notIndexedColNames);
						// Match PDF format for early exit trace
						tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
								", Indexed columns: " + indexedColNames.toString() +
								", Indexed selection count: 0, Non Indexed: " + notIndexedColNames.toString() +
								", Final count: 0, execution time (mil):" + executionTime);
						return result;
					}
				}
			}

			// combinedBitSet now contains the global record indices that match all indexed conditions.
			// If indexedColIndicesInQuery was empty (should be caught by dispatch, but defensive check)
			if (combinedBitSet == null && !indexedColIndicesInQuery.isEmpty()) {
				long endTime = System.nanoTime();
				long executionTime = (endTime - startTime) / 1000000;
				System.err.println("Logical Error: combinedBitSet is null after processing indexed columns in selectCase2 for table " + table.getTableName());
//				tableTraces.get(table.getTableName()).add("Select Case 2 Error: Logical Error combinedBitSet null, execution time (mil):" + executionTime);
				return result;
			}

			// Calculate indexed selection count for trace
			if (combinedBitSet != null) {
				indexedSelectionCount = combinedBitSet.cardinality();
			}


			// Pre-calculate column indices for linear scan once
			Map<Integer, Integer> notIndexedColMap = new HashMap<>(); // Maps query index to table index
			// ArrayList<String> notIndexedColNames = new ArrayList<>(); // ** REMOVED - Will populate just before trace **
			try {
				for(int j : notIndexedColIndicesInQuery) {
					String colName = cols[j];
					notIndexedColMap.put(j, table.getColumnIndex(colName));
					// notIndexedColNames.add(colName); // ** REMOVED - Populate later **
				}
			} catch (IllegalArgumentException e) {
				long endTime = System.nanoTime();
				long executionTime = (endTime - startTime) / 1000000;
				System.err.println("Error getting column index for non-indexed column during selectCase2: " + e.getMessage());
				e.printStackTrace(); // Print stack trace for debugging
//				tableTraces.get(table.getTableName()).add("Select Case 2 Error: Invalid non-indexed column: " + e.getMessage() + ", execution time (mil):" + executionTime);
				return result; // Return empty list if a column is invalid
			}


			// Iterate through the set bits of the combined result (from indexed columns)
			// And apply linear filtering for non-indexed columns
			if (combinedBitSet != null) { // Only iterate if there are indexed results
				for (int i = combinedBitSet.nextSetBit(0); i >= 0; i = combinedBitSet.nextSetBit(i + 1)) {
					// Load the specific record corresponding to global index 'i' from disk
					String[] record = table.getRecordByGlobalIndex(i); // Call your method here

					if (record != null) {
						// Now linearly check the conditions for the non-indexed columns
						boolean nonIndexedMatch = true;
						for (Map.Entry<Integer, Integer> entry : notIndexedColMap.entrySet()) {
							int queryColIndex = entry.getKey(); // Index in the original 'cols' array
							int tableColIndex = entry.getValue(); // Index in the record array

							// Add check for valid tableColIndex in case getColumnIndex failed somehow (though it throws exception)
							if (tableColIndex < 0 || tableColIndex >= record.length) {
								System.err.println("Warning: Invalid table column index " + tableColIndex + " for record during selectCase2 for table " + table.getTableName());
								nonIndexedMatch = false; // Treat as non-match if index is bad
								break;
							}

							if (!record[tableColIndex].equals(vals[queryColIndex])) {
								nonIndexedMatch = false;
								break;
							}
						}

						if (nonIndexedMatch) {
							result.add(record);
						}
					} else {
						System.err.println("Warning: Could not load record with global index: " + i + " during selectCase2 for table " + table.getTableName());
						// Continue even if one record fails to load
					}
				}
			}


		} catch (Exception e) { // Catch broader exceptions
			long endTime = System.nanoTime();
			long executionTime = (endTime - startTime) / 1000000;
			System.err.println("Error during selectCase2_SomeIndexedColumns for table '" + table.getTableName() + "': " + e.getMessage());
			e.printStackTrace(); // Print stack trace for debugging
//			tableTraces.get(table.getTableName()).add("Select Case 2 Error: " + e.getMessage() + ", execution time (mil):" + executionTime);
			return new ArrayList<>(); // Return empty list on error
		}

		long endTime = System.nanoTime(); // End time for trace
		long executionTime = (endTime - startTime) / 1000000;
		// Trace creation at the end, populating nonIndexedColNames just before use
		ArrayList<String> indexedColNames = new ArrayList<>();
		for(int idx : indexedColIndicesInQuery) indexedColNames.add(cols[idx]);
		ArrayList<String> notIndexedColNames = new ArrayList<>(); // ** CREATE LIST HERE **
		for(int idx : notIndexedColIndicesInQuery) notIndexedColNames.add(cols[idx]); // ** Populate it here **

		Collections.sort(indexedColNames);
		Collections.sort(notIndexedColNames);
		// Match PDF format for successful completion trace
		tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
				", Indexed columns: " + indexedColNames.toString() + // Use list of names
				", Indexed selection count: " + indexedSelectionCount + // Cardinality of combined index result
				", Non Indexed: " + notIndexedColNames.toString() + // Use list of names
				", Final count: " + result.size() +
				", execution time (mil):" + executionTime);
		return result;
	}

	private static ArrayList<String[]> selectCase3_OneIndexedColumn(Table table, String[] cols, String[] vals, int indexedIdx) { // Removed throws DBAppException
		String colName = cols[indexedIdx];
		String value = vals[indexedIdx];
		long startTime = System.nanoTime(); // Start time for trace
		int indexedSelectionCount = 0;
		ArrayList<String> otherColNames = new ArrayList<>();

		ArrayList<String[]> result = new ArrayList<>(); // Prepare result list

		try {
			BitmapIndex index = FileManager.loadTableIndex(table.getTableName(), colName);
			if (index == null) {
				long endTime = System.nanoTime();
				long executionTime = (endTime - startTime) / 1000000;
				// Handle error
				System.err.println("Error: Index not found for column: '" + colName + "' unexpectedly during selectCase3 for table " + table.getTableName());
//				tableTraces.get(table.getTableName()).add("Select Case 3 Error: Index not found for " + colName + ", execution time (mil):" + executionTime);
				return result; // Return empty list on error
			}

			BitSet bitSet = index.getBitMapIndex().get(value);
			if (bitSet == null) {
				long endTime = System.nanoTime();
				long executionTime = (endTime - startTime) / 1000000;
				// Match PDF format for early exit trace
				tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
						", Indexed columns: [" + cols[indexedIdx] + "]" +
						", Indexed selection count: 0, Non Indexed: [], Final count: 0, execution time (mil):" + executionTime);
				return result; // Value not in index, no matches
			}

			indexedSelectionCount = bitSet.cardinality(); // Count records from index


			// Pre-calculate column indices for linear scan once
			Map<Integer, Integer> otherColMap = new HashMap<>(); // Maps query index to table index
			try {
				for (int j = 0; j < cols.length; j++) {
					if (j != indexedIdx) { // Only include non-indexed columns
						String otherColName = cols[j];
						otherColMap.put(j, table.getColumnIndex(otherColName));
						otherColNames.add(otherColName);
					}
				}
			} catch (IllegalArgumentException e) {
				long endTime = System.nanoTime();
				long executionTime = (endTime - startTime) / 1000000;
				System.err.println("Error getting column index for non-indexed column during selectCase3: " + e.getMessage());
				e.printStackTrace();
//				tableTraces.get(table.getTableName()).add("Select Case 3 Error: Invalid non-indexed column: " + e.getMessage() + ", execution time (mil):" + executionTime);
				return result; // Return empty list if a column is invalid
			}


			// Iterate through the set bits of the index for the single indexed column
			for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
				// Load the specific record corresponding to global index 'i' from disk
				String[] record = table.getRecordByGlobalIndex(i); // Call your method here

				if (record != null) {
					// Linearly check the conditions for the OTHER (non-indexed) columns
					boolean otherColumnsMatch = true;
					for (Map.Entry<Integer, Integer> entry : otherColMap.entrySet()) {
						int queryColIndex = entry.getKey();
						int tableColIndex = entry.getValue();

						// Add check for valid tableColIndex
						if (tableColIndex < 0 || tableColIndex >= record.length) {
							System.err.println("Warning: Invalid table column index " + tableColIndex + " for record during selectCase3 for table " + table.getTableName());
							otherColumnsMatch = false; // Treat as non-match
							break;
						}

						if (!record[tableColIndex].equals(vals[queryColIndex])) {
							otherColumnsMatch = false;
							break;
						}
					}

					if (otherColumnsMatch) {
						result.add(record);
					}
				} else {
					System.err.println("Warning: Could not load record with global index: " + i + " during selectCase3 for table " + table.getTableName());
					// Continue even if one record fails to load
				}
			}

		} catch (Exception e) { // Catch broader exceptions
			long endTime = System.nanoTime();
			long executionTime = (endTime - startTime) / 1000000;
			System.err.println("Error during selectCase3_OneIndexedColumn for table '" + table.getTableName() + "': " + e.getMessage());
			e.printStackTrace(); // Print stack trace for debugging
//			tableTraces.get(table.getTableName()).add("Select Case 3 Error: " + e.getMessage() + ", execution time (mil):" + executionTime);
			return new ArrayList<>(); // Return empty list on error
		}

		long endTime = System.nanoTime(); // End time for trace
		long executionTime = (endTime - startTime) / 1000000;
		// Trace for successful completion (matching PDF output format)
		Collections.sort(otherColNames);
		tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
				", Indexed columns: [" + cols[indexedIdx] + "]" + // Trace format from example
				", Indexed selection count: " + indexedSelectionCount + // Cardinality of the single index result
				", Non Indexed: " + otherColNames.toString() + // List of non-indexed names
				", Final count: " + result.size() +
				", execution time (mil):" + executionTime);

		return result;
	}


	private static ArrayList<String[]> selectCase4_NoIndexedColumns(Table table, String[] cols, String[] vals) {
		long startTime = System.nanoTime(); // Start time for trace
		ArrayList<String[]> result = select(table.getTableName(), cols, vals);
		long endTime = System.nanoTime(); // End time for trace
		long executionTime = (endTime - startTime) / 1000000;
		Arrays.sort(cols);
		tableTraces.get(table.getTableName()).add("Select index condition: " + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
				", Indexed selection count: 0" +
				", Non Indexed: " + Arrays.toString(cols) +
				", Final count: " + result.size() +
				", execution time (mil):" + executionTime);
		return result;
	}

	public static void main(String[] args) throws IOException {

	}
}