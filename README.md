# Mini DBMS Implementation - GUC CSEN604

[![Milestone 2 Tests](https://github.com/JomanaMahmoud/miniDBMS/actions/workflows/gradle-ci.yml/badge.svg)](https://github.com/JomanaMahmoud/miniDBMS/actions/workflows/gradle-ci.yml)
<!-- [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) -->

A Java-based project for the CSEN604 - Data Bases II course at the German University in Cairo (GUC), Spring 2025. This project implements a subset of DBMS functionalities, focusing on core operations, data storage, indexing, and recovery.

## Table of Contents

- [Introduction](#introduction)
- [Project Objectives](#project-objectives)
- [Features Implemented](#features-implemented)
  - [Milestone 1](#milestone-1)
  - [Milestone 2](#milestone-2)
- [Simplification Assumptions](#simplification-assumptions)
- [Technology Stack](#technology-stack)
- [Core Components](#core-components)
- [Trace Functionality](#trace-functionality)
- [Data Recovery (MS2)](#data-recovery-ms2)
- [Usage Example](#usage-example)
- [Team Members](#team-members)
- [Acknowledgements](#acknowledgements)

## Introduction

This project aims to provide a deeper understanding of the internal components and functionalities of a Database Management System (DBMS). The implementation focuses on the behavior and performance of these components, built progressively across two milestones.

## Project Objectives

*   Implement fundamental DBMS operations: table creation, insertion, and selection.
*   Explore different data retrieval methods: full scan, conditional select, and direct record access.
*   Implement data persistence by storing tables and pages on disk.
*   Introduce and utilize Bitmap indexing for efficient querying.
*   Implement data recovery mechanisms for missing pages.
*   Maintain a trace of operations performed on tables for auditing and debugging.
*   Analyze the behavior and performance of the implemented components.

## Features Implemented

### Milestone 1
*(Deadline: 10/4 at 11:59 pm)*

1.  **Create Table**:
    *   Defines a new table schema with specified column names.
    *   Stores table metadata persistently.
2.  **Insert into Table**:
    *   Adds new records (rows) to a specified table.
    *   Handles page management and stores data persistently.
3.  **Select from Table**:
    *   **3.1. All data (`SELECT *`)**: Retrieves all records from a table.
    *   **3.2. Conditional Select (`SELECT * WHERE ...`)**: Retrieves records based on specified column-value conditions.
    *   **3.3. Pointer Select (Direct Record Access)**: Retrieves a specific record using its page number and record number within that page.
4.  **Retrieve Trace**:
    *   Provides a full or partial log of operations performed on a table, including timestamps.

### Milestone 2
*(Deadline: 19/5 at 11:59 pm)*

1.  **Create Bitmap Index**:
    *   Generates a Bitmap index for a specified column in a table.
    *   Persists the index to disk.
2.  **Insert into Bitmap Index**:
    *   Updates the Bitmap index automatically upon new record insertions into the table.
3.  **Select using Index**:
    *   Optimizes conditional selects by utilizing available Bitmap indexes.
    *   Handles cases with:
        *   All condition columns indexed (ANDing bitmap results).
        *   Only one condition column indexed (index lookup + linear scan on results).
        *   Multiple (but not all) condition columns indexed (ANDing available index results + linear scan).
        *   No relevant indexes created (falls back to linear scan).
4.  **Data Recovery (Restoring)**:
    *   Identifies missing data (whole pages).
    *   Recovers missing records from a provided list, restoring them to their original positions within the table structure.

## Simplification Assumptions

To focus on core DBMS concepts, the following simplifications are made:

**General (Milestone 1 & 2):**
1.  **Data Types Normalization**: All data is treated as `STRING`. No type transformations or null/empty cell considerations are required.
2.  **Star Selects (`SELECT *`)**: All select operations return all columns of the matching records.
3.  **Select Output Format**: The output for select operations is an `ArrayList<String[]>`.
4.  **Insertion Input Format**: Records for insertion are provided as `String[]`.
5.  **No Primary Key**: New records are always appended to the end of the table. Pages are filled based on `dataPageSize`.
6.  **`dataPageSize`**: A constant defining the maximum number of rows/tuples per page.

**Milestone 2 Specific:**
1.  **Bitmap Index Implementation**: Follows the standard Bitmap index structure as discussed in the course.
2.  **Index Creation**: Indexes are created explicitly via a function call, not automatically upon table creation.
3.  **Index Updates**: Existing indexes are updated with every new insertion into the table.
4.  **Data Loss Scope**: Data loss for recovery scenarios is assumed to be one or more complete pages. No partial page data loss or individual record deletion functionality is implemented.

## Technology Stack

*   **Java**: Core programming language.
*   **Gradle**: For dependency management and building.
*   **JUnit**: For unit testing.

## Core Components

The project revolves around several key Java files:

1.  **`DBApp.java`**:
    *   The main class containing the implementation of all DBMS functionalities (create, insert, select, index operations, recovery, trace).
    *   Students are required to implement methods within this class without changing signatures.
    *   Contains the `dataPageSize` constant.

2.  **`FileManager.java`**:
    *   Provided utility class responsible for serializing and deserializing tables, pages, and indexes to/from disk.
    *   Manages a "Tables" directory where each table and its associated data (pages, indexes) are stored in subfolders.
    *   Key methods include:
        *   `storeTable(tableName, Table)` / `loadTable(tableName)`
        *   `storeTablePage(tableName, pageNumber, Page)` / `loadTablePage(tableName, pageNumber)`
        *   `storeTableIndex(tableName, columnName, BitmapIndex)` / `loadTableIndex(tableName, columnName)` (MS2)
        *   `reset()`: Clears the "Tables" directory.
        *   `trace()`: Returns a string representation of the "Tables" directory structure.

3.  **`DBAppTests.java` / `DBAppTestsMS2.java`**:
    *   Provided JUnit test files for evaluating the `DBApp.java` implementation. These files are not to be modified.

4.  **Optional Helper Classes**:
    *   `Table.java`: Could represent a table's metadata and structure.
    *   `Page.java`: Could represent a single page containing records.
    *   `BitMapIndex.java` (MS2): Could represent the Bitmap index structure for a column.

## Trace Functionality

The DBMS maintains a trace of operations for each table. This can be retrieved using:

*   `getFullTrace(String tableName)`: Returns a string containing all operations performed on the table with timestamps.
*   `getLastTrace(String tableName)`: Returns a string containing only the last operation performed on the table.

Example trace line:
`Inserted: [1, stud1, CS, 5, 0.9], at page number:0, execution time (mil):5`

`FileManager.trace()` provides a trace of the file system structure within the "Tables" directory.
Example: `Tables{ student{ 0.db 1.db student.db } }`

## Data Recovery (MS2)

Data recovery is implemented for scenarios where entire pages of a table are lost.
1.  `validateRecords(String tableName)`: Checks for missing records by inspecting the sequence and completeness of pages. It returns an `ArrayList<String[]>` of the records that were in the deleted pages.
2.  `recoverRecords(String tableName, ArrayList<String[]> missingRecords)`: Takes the list of missing records and re-inserts them into their original positions within the table structure. This involves recreating pages if necessary and placing records correctly, not just appending them. The table's data on disk and its trace log are updated.

## Usage Example

The `main` method in `DBApp.java` can be used for testing the implementation. Below is a condensed example demonstrating some functionalities:

```java
// In DBApp.java
public static void main(String[] args) throws IOException {
    FileManager.reset(); // Clear previous data

    // --- Milestone 1 Example ---
    String[] studentCols = {"id", "name", "major", "semester", "gpa"};
    createTable("student", studentCols);

    insert("student", new String[]{"1", "stud1", "CS", "5", "0.9"});
    insert("student", new String[]{"2", "stud2", "BI", "7", "1.2"});
    // ... more inserts

    System.out.println("Output of selecting the whole table content:");
    ArrayList<String[]> result1 = select("student");
    for (String[] array : result1) {
        System.out.println(String.join(" ", array));
    }

    System.out.println("Output of selecting by column condition (gpa = 1.2):");
    ArrayList<String[]> result3 = select("student", new String[]{"gpa"}, new String[]{"1.2"});
    for (String[] array : result3) {
        System.out.println(String.join(" ", array));
    }

    // --- Milestone 2 Example ---
    createBitMapIndex("student", "gpa");
    createBitMapIndex("student", "major");

    System.out.println("Bitmap of CS from major index: " + getValueBits("student", "major", "CS"));

    System.out.println("Output of selecting using index (major=CS AND gpa=1.2):");
    ArrayList<String[]> resultIdx = selectIndex("student",
                                           new String[]{"major", "gpa"},
                                           new String[]{"CS", "1.2"});
    for (String[] array : resultIdx) {
        System.out.println(String.join(" ", array));
    }

    System.out.println("Full Trace of student table:");
    System.out.println(getFullTrace("student"));

    // Example for Data Recovery (conceptual)
    // Code to simulate page deletion would go here (e.g., using FileManager or direct file ops)
    // ArrayList<String[]> missing = validateRecords("student");
    // if (!missing.isEmpty()) {
    //     recoverRecords("student", missing);
    //     System.out.println("Records recovered. Missing records count now: " + validateRecords("student").size());
    // }
}
```

## Team Members

*   [Jomana Mahmoud](https://github.com/JomanaMahmoud)
*   [Yehia Rasheed](https://github.com/yehiarasheed)
*   [Rawan Hossam](https://github.com/rawanhossam27)
*   [Nada Yasser](https://github.com/NadaYasser8)

## Acknowledgements

*   This project was completed under the supervision of **Dr. Mohamed Karam**, Faculty of Media Engineering and Technology, German University in Cairo (GUC).
*   We would also like to acknowledge the GUC for providing the CSEN604 - Data Bases II course.
