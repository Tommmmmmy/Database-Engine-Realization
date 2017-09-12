import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;

public class commandInterface {
	
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "©2016 Chris Irwin Davis";
	static boolean isExit = false;
	/*
	 * Page size for all files is pageSize bytes by default.
	 * You may choose to make it user modifiable
	 */
	static int pageSize = 512; 

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		tableCreater.createFiles();
		/* Display the welcome screen */
		splashScreen();
		
		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").replace("'", "").replace("\"", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			System.out.println();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	/** ***********************************************************************
	 *  Method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			System.out.println(line("*",80));
			System.out.println("SUPPORTED COMMANDS");
			System.out.println("All commands below are case insensitive");
			System.out.println();
			System.out.println("\tSHOW TABLES;                                     Display all tables.");
			System.out.println("\tCREATE TABLE table_name;                         Create a table");
			System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
			System.out.println("\tSELECT * FROM table_name WHERE ();               Display records which satisfies the condition.");
			System.out.println("\tINSERT INTO table_name () VALUES ();             Insert a record into the table.");
			System.out.println("\tDELETE FROM table_name WHERE row_id = <value>;   Deleate a record whose row_id is <value>.");			
			System.out.println("\tUPDATE table_name SET () WHERE ();               Update a record.");						
			System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
			System.out.println("\tVERSION;                                         Show the program version.");
			System.out.println("\tHELP;                                            Show this help information.");
			System.out.println("\tEXIT;                                            Exit the program.");
			System.out.println();
			System.out.println();
			System.out.println(line("*",80));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) throws IOException {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		userCommand = userCommand.toLowerCase();
		if(userCommand.toLowerCase().replace("_", "").replace(";", "").replace(" ", "").equals("selecttablenamefromdavisbasetables")
				|| userCommand.toLowerCase().replace("_", "").replace(";", "").replace(" ", "").equals("showtables")){
			System.out.println(userCommand);
			showTable(userCommand);
		}
		else{
			ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

			/*
			*  This switch handles a very small list of hardcoded commands of known syntax.
			*  You will want to rewrite this method to interpret more complex commands. 
			*/
			switch (commandTokens.get(0)) {
				case "select":
					parseQueryString(userCommand);
					break;
				case "drop":
					System.out.println("STUB: Calling your method to drop items");
					dropTable(userCommand);
					break;
				case "create":
					parseCreateString(userCommand);
					break;
				case "insert":
					insertRecord(userCommand);
					break;
				case "delete":
					deleteRecord(userCommand);
					break;
				case "update":
					updateRecord(userCommand);
					break;
				case "help":
					help();
					break;
				case "version":
					displayVersion();
					break;
				case "exit":
					isExit = true;
					break;
				case "quit":
					isExit = true;
				default:
					System.out.println("I didn't understand the command: \"" + userCommand + "\"");
					break;
			}
		}
		
	}
	

	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		try {
			LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
			LinkedHashMap<Integer, String[]> data = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + dropTableString.split(" ")[2].trim());
			if(data.size() == 0){
				System.out.println("This table does not exist!");
				return;
			}
			Operations.dropOperation(dropTableString.split(" ")[2]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 * @throws IOException 
	 */
	public static void parseQueryString(String queryString) throws IOException {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		String[] temp = queryString.toLowerCase().split("from");
		String column = temp[0].split("select")[1].trim().replace(", ", ",");
		String table = temp[1].trim().split("where")[0].trim();
		String condition = "";
		if(temp[1].trim().split("where").length > 1){
			condition = temp[1].trim().split("where")[1].trim();
		}
		List<String> columns = null;
		if(!column.equals("*")){
			columns = Arrays.asList(column.split(","));
		}
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
		LinkedHashMap<Integer, String[]> data = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + table);
		if(data.size() == 0){
			System.out.println("This table does not exist!");
			return;
		}
		Operations.selectWhereOperation(table, columns, condition);
	}
	
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 */
	public static void parseCreateString(String createTableString) {
		
		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		/* Define table file name */
		String tableFileName = createTableTokens.get(2);

		/* YOUR CODE GOES HERE */
		//
		String[] columns = createTableString.split("\\(")[1].trim().split("\\)")[0].trim().split(",");
		
		/*  Code to create a .tbl file to contain table data */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 *  Note that this doesn't create the table file in the correct directory structure
			 */
			RandomAccessFile tableFile = new RandomAccessFile("data\\user_data\\" + tableFileName  + ".tbl", "rw");
			tableFile.setLength(pageSize);
			tableFile.seek(0);
			tableFile.write(0x0D);
			tableFile.seek(2);
			tableFile.writeShort(pageSize);
			tableFile.writeInt(-1);
			LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
			LinkedHashMap<Integer, String[]> data = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + tableFileName);
			if(data.size() > 0){
				System.out.println("This table already exists!");
				return;
			}
			Operations.createOperation(columns, tableFileName);
			tableFile.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
		/*  Code to insert a row in the davisbase_tables table 
		 *  i.e. database catalog meta-data 
		 */
		
		/*  Code to insert rows in the davisbase_columns table  
		 *  for each column in the new table 
		 *  i.e. database catalog meta-data 
		 */
	}
	
	public static void showTable(String showTableString) throws IOException{
		System.out.println("STUB: Calling your method to show all tables in the database");
		System.out.println("Parsing the string:\"" + showTableString + "\"");
		RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
		file.seek(1);
		int pages = (int)(file.length() / pageSize);
		for(int i = 0; i < pages; i++){
			file.seek(pageSize * i + 1);
			int count = file.readByte();
			file.seek(pageSize * i + 8);
			short[] locations = new short[count];
			for(int j = 0; j < count; j++){
				locations[j] = file.readShort();
			}
			for(int j = 0; j < count; j++){
				file.seek(locations[j] + 6);
				int columns = file.readByte();
				file.seek(locations[j] + 7);
				byte type = file.readByte();
				if(type == 0x00 || type == 0x01 || type == 0x02 || type == 0x03){
					continue;
				}
				else if(type == 0x04){
					file.seek(locations[j] + 7 + columns);
					System.out.println(file.readByte());
				}
				else if(type == 0x05){
					file.seek(locations[j] + 7 + columns);
					System.out.println(file.readShort());
				}
				else if(type == 0x06){
					file.seek(locations[j] + 7 + columns);
					System.out.println(file.readInt());
				}
				else if(type == 0x07){
					file.seek(locations[j] + 7 + columns);
					System.out.println(file.readLong());
				}
				else if(type == 0x08){
					file.seek(locations[j] + 7 + columns);
					System.out.println(file.readFloat());
				}
				else if(type == 0x09){
					file.seek(locations[j] + 7 + columns);
					System.out.println(file.readDouble());
				}
				else{
					file.seek(locations[j] + 7 + columns);
					int length = new Integer(type-0x0C);
					String result = "";
					for(int m = 0; m < length; m++){
						byte temp = file.readByte();
						if(temp != 0){
							result += (char)temp;
						}
					}
					System.out.println(result);
				}
			}
		}
		
	}
	
	public static void insertRecord(String insertRecordString) throws IOException{
		System.out.println("STUB: Calling your method to insert a record into the table");
		System.out.println("Parsing the string:\"" + insertRecordString + "\"");
		String[] left = insertRecordString.split("into")[1].trim().split(" ");
		String tableName = left[0];
		List<String> record = new ArrayList<>();
		List<String> columnList = new ArrayList<>();
		String temp = insertRecordString.split("values")[0].trim();
		temp = temp.split("into")[1].trim();
		if(temp.contains("(")){
			temp = temp.split("\\(")[1].trim();
			temp = temp.split("\\)")[0].trim();
			String[] columns = temp.split(",");
			for(String str : columns){
				columnList.add(str.trim());
			}
		}
		String value = insertRecordString.split("values")[1].trim();
		String[] values = value.substring(1, value.length() - 1).split(",");
		for(int i = 0; i < values.length; i++){
			record.add(values[i].replace("'", "").trim());
		}
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
		LinkedHashMap<Integer, String[]> data = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + tableName);
		if(data.size() == 0){
			System.out.println("This table does not exist!");
			return;
		}
		Operations.insertOperation(tableName, columnList, record);
	}
	
	public static void deleteRecord(String deleteRecordString) throws IOException{
		System.out.println("STUB: Calling your method to delete a record from the table");
		System.out.println("Parsing the string:\"" + deleteRecordString + "\"");
		String condition = deleteRecordString.split("where")[1].trim();
		String tableName = deleteRecordString.split("where")[0].trim().split(" ")[2].trim();
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
		LinkedHashMap<Integer, String[]> data = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + tableName);
		if(data.size() == 0){
			System.out.println("This table does not exist!");
			return;
		}
		Operations.deleteOperation(tableName, condition);
	}
	
	public static void updateRecord(String updateRecordString) throws IOException{
		System.out.println("STUB: Calling your method to update a record in the table");
		System.out.println("Parsing the string:\"" + updateRecordString + "\"");
		String tableName = updateRecordString.split("set")[0].trim().split(" ")[1].trim();
		List<String> action = new ArrayList<>();
		String columnName = updateRecordString.split("set")[1].trim().split("=")[0].trim();
		String value = updateRecordString.split("set")[1].trim().split("=")[1].trim().split("where")[0].trim().replace("'", "");
		action.add(columnName);
		action.add(value);
		String temp = updateRecordString.split("set")[1].trim();
		String condition = "";
		if(temp.split("where").length > 1){
			condition = temp.split("where")[1].trim();
		}
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
		LinkedHashMap<Integer, String[]> data = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + tableName);
		if(data.size() == 0){
			System.out.println("This table does not exist!");
			return;
		}
		Operations.updateOperation(tableName, action, condition);
	}
}
