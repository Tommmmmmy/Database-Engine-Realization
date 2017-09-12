import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Operations {
	final static int pageSize = 512;
	static String database = "";
	public static void createOperation(String[] columns, String tableFileName){
		try{
			if(columns[0].trim().split(" ").length != 4 || (!columns[0].trim().split(" ")[1].equalsIgnoreCase("INT"))){
				System.out.println("The first column is not an int primary key!");
				return;
			}
			int lastKey = getClass.getLastKey("davisbase_tables");
			List<String> record = new ArrayList<>();
			record = Arrays.asList(new String[]{lastKey+1+"", tableFileName, 0+"", 0+""});
			insertOperation("davisbase_tables", new ArrayList<>(), record);
			String is_nullable = "YES";
			for(int i = 0; i < columns.length; i++){
				is_nullable = "YES";
				if(columns[i].trim().split(" ").length == 4){
					if((columns[i].trim().split(" ")[2].equalsIgnoreCase("primary") && columns[i].trim().split(" ")[3].equalsIgnoreCase("key")) 
							|| (columns[i].trim().split(" ")[2].equalsIgnoreCase("NOT") && columns[i].trim().split(" ")[3].equalsIgnoreCase("NULL"))){
						is_nullable = "NO";
					}
				}
				lastKey = getClass.getLastKey("davisbase_columns");
//				System.out.println(lastKey);
				record = Arrays.asList(new String[]{lastKey+1+"", tableFileName, columns[i].trim().split(" ")[0].trim(), columns[i].trim().split(" ")[1].trim(), i+1+"", is_nullable});
				insertOperation("davisbase_columns", new ArrayList<>(), record);
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	public static void dropOperation(String tableName) throws IOException{
		try {
			File file = new File("data\\user_data\\" + database + "\\", tableName + ".tbl");
			file.delete();
			LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo("davisbase_tables");
			LinkedHashMap<Integer, String[]> record = getClass.getRecord("davisbase_tables", columnInfo, "table_name = " + tableName);
			int rowId = -1;
			for(Map.Entry<Integer, String[]> entry : record.entrySet()){
				rowId = entry.getKey();
			}
			deleteOperation("davisbase_tables", "rowid = " + rowId);
			
			columnInfo = getClass.getColumnInfo("davisbase_columns");
			record = getClass.getRecord("davisbase_columns", columnInfo, "table_name = " + tableName);
			for(Map.Entry<Integer, String[]> entry : record.entrySet()){
				rowId = entry.getKey();
				deleteOperation("davisbase_columns", "rowid = " + rowId);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void insertOperation(String tableName, List<String> columnList, List<String> record) throws IOException{
		RandomAccessFile file;
		if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")){
			file = new RandomAccessFile("data\\catalog\\" + tableName + ".tbl", "rw");
		}
		else{
			file = new RandomAccessFile("data\\user_data\\" + Operations.database + "\\" + tableName + ".tbl", "rw");
		}
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo(tableName);
		for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			if(columnList.size() != 0 && (entry.getValue()[2].equalsIgnoreCase("no") && !columnList.contains(entry.getKey())) && 
					!(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))){
				System.out.println("Not null columns don't have values!");
				return;
			}
		}
		String primaryKey = "";
		for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			primaryKey = entry.getKey();
			break;
		}
		LinkedHashMap<Integer, String[]> data = getClass.getRecord(tableName, columnInfo, primaryKey + "=" + record.get(0));
		if(data.size() > 0){
			System.out.println("Duplicate primary key!");
			return;
		}
//		for(int i = 0; i < record.size(); i++){
//			record.add(i, record.get(i).replace("\"", "").replace("\'", ""));
//		}
		int payLoad = 1;

		for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			if(entry.getKey().equalsIgnoreCase("rowid") && (tableName.equalsIgnoreCase("davisbase_tables") 
					|| tableName.equalsIgnoreCase("davisbase_columns"))){
				continue;		
			}
			String type = entry.getValue()[0];
			payLoad += getClass.getLength(type);
			payLoad += 1;
		}
		List<Integer> notSpecifyColumnIndex = new ArrayList<>(); 
		int idx = 0;
		for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			if(entry.getKey().equalsIgnoreCase("rowid") && (tableName.equalsIgnoreCase("davisbase_tables") 
					|| tableName.equalsIgnoreCase("davisbase_columns"))){
				idx++;
				continue;		
			}
			if(columnList.size() != 0 && !columnList.contains(entry.getKey())){
				notSpecifyColumnIndex.add(idx);
			}
			idx++;
		}
		for(Integer inte : notSpecifyColumnIndex){
			record.add(inte.intValue(), String.valueOf(0));
		}
		int rowId = getClass.getLastKey(tableName) + 1;
		int columnNum = columnInfo.size();
		if(tableName.equalsIgnoreCase("davisbase_tables") 
					|| tableName.equalsIgnoreCase("davisbase_columns")){
			columnNum = columnInfo.size() - 1;
		}
		int page = -1;
		int k = 0;
		for(; k < (int)file.length() / pageSize; k++){
			file.seek(k * pageSize);
			if(file.readByte() == 0x0D){
				int minKey = -1;
				int maxKey = -1;
				file.seek(k * pageSize + 1);
				int numOfRecords = file.readByte();
				if(numOfRecords == 0){
					page = k;
					break;
				}
				file.seek(k * pageSize + 8);
				short firstRecordPosition = file.readShort();
				file.seek(k * pageSize + 2);
				short lastRecordPosition = file.readShort();
				file.seek(firstRecordPosition + 2);
				minKey = file.readInt();
				file.seek(lastRecordPosition + 2);
				maxKey = file.readInt();
				file.seek(k * pageSize + 4);
				int pointer = file.readInt();
				if(minKey == 0 || maxKey == 0){
					page = 0;
				}
				else if(minKey <= rowId && maxKey >= rowId && enoughSpace(file, k, payLoad)){
					page = k;
					break;
				}
				else if(maxKey < rowId && enoughSpace(file, k, payLoad)){
					page = k;
					break;
				}
			}
		}
		if(page == -1){
			page = newPage(file, k-1) - 1;
		}
		file.seek(page * pageSize + 1);
		int numOfRecords = file.readByte();
		short newPosition = (short)((int)(file.readShort()) - payLoad - 6);
		short[] positions = new short[numOfRecords];
		for(int i = 0; i < numOfRecords; i++){
			file.seek(page * pageSize + 8 + i * 2);
			positions[i] = file.readShort();
		}
		file.seek(page * pageSize + 1);
		file.write(numOfRecords + 1);
		file.writeShort(newPosition);
		file.seek(newPosition);
		byte[] typeCode;
		if(tableName.equalsIgnoreCase("davisbase_tables") 
					|| tableName.equalsIgnoreCase("davisbase_columns")){
			typeCode = new byte[columnInfo.size()-1];
			int index = 0;
			boolean bool = true;
			for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
				if(bool){
					bool = false;
					continue;
				} 
				typeCode[index++] = getClass.getTypeCode(entry.getValue()[0]);
			}
		}
		else{
			typeCode = new byte[columnInfo.size()];
			int index = 0;
			for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){ 
				typeCode[index++] = getClass.getTypeCode(entry.getValue()[0]);
			}
		}
		file.writeShort(payLoad);
		file.writeInt(rowId);
		file.write(columnNum);
		for(int i = 0; i < typeCode.length; i++){
			file.writeByte(typeCode[i]);
		}
		file.seek(page * pageSize + 8);
		for(short pos : positions){
			file.writeShort(pos);
		}
		file.writeShort(newPosition);
		file.seek(newPosition + 7 + typeCode.length);
		for(int i = 0; i < columnNum; i++){
			String temp = record.get(i);
			if(tableName.equalsIgnoreCase("davisbase_tables") 
					|| tableName.equalsIgnoreCase("davisbase_columns")){
				temp = record.get(i + 1);
			}
			if(typeCode[i] == 0x04){
				file.write(Integer.valueOf(temp));
			}
			else if(typeCode[i] == 0x05){
				file.writeShort(Short.valueOf(temp));
			}
			else if(typeCode[i] == 0x06){
				file.writeInt(Integer.valueOf(temp));
			}
			else if(typeCode[i] == 0x07){
				file.writeLong(Long.valueOf(temp));
			}
			else if(typeCode[i] == 0x08){
				file.writeFloat(Float.valueOf(temp));
			}
			else if(typeCode[i] == 0x09){
				file.writeDouble(Double.valueOf(temp));
			}
			else if(typeCode[i] == 0x0A){
				if(temp.equalsIgnoreCase("0")){
					file.writeLong(0);
				}
				else{
					String[] date = temp.split("_")[0].split("-");
					String[] time = temp.split("_")[1].split(":");
					ZoneId zoneId = ZoneId.of("America/Chicago");
					ZonedDateTime zonedTime = ZonedDateTime.of(Integer.valueOf(date[0]), Integer.valueOf(date[1]), Integer.valueOf(date[2]),
							Integer.valueOf(time[0]), Integer.valueOf(time[1]), Integer.valueOf(time[2]), 1234, zoneId);
					file.writeLong(zonedTime.toInstant().toEpochMilli() / 1000);
				}
			}
			else if(typeCode[i] == 0x0B){
				if(temp.equalsIgnoreCase("0")){
					file.writeLong(0);
				}
				else{
					String[] date = temp.split("-");
					ZoneId zoneId = ZoneId.of("America/Chicago");
					ZonedDateTime zonedTime = ZonedDateTime.of(Integer.valueOf(date[0]), Integer.valueOf(date[1]), Integer.valueOf(date[2]),
							0, 0, 0, 1234, zoneId);
					file.writeLong(zonedTime.toInstant().toEpochMilli() / 1000);
				}
			}
			else{
				if(temp.equalsIgnoreCase("0")){
					file.writeLong(0);
				}
				else{
					file.writeBytes(temp);
					if(temp.length() < 20){
						for(int j = 0; j < 20 - temp.length(); j++){
							file.writeByte(0);
						}
					}
				}
			}
		}
		file.close();
	}	
	
	public static void selectWhereOperation(String table, List<String> columns, String condition) throws IOException{
		try {
			LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo(table);
			LinkedHashMap<Integer, String[]> record = getClass.getRecord(table, columnInfo, condition);
			if(record.size() == 0){
				System.out.println("No such records!");
				return;
			}
			List<Integer> columnIndex = new ArrayList<>();
			Map<Integer, Integer> map1 = new HashMap<>();
			Map<Integer, String> map2 = new HashMap<>();
			for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
				if(columns != null && columns.contains(entry.getKey()) || columns == null){
					columnIndex.add(Integer.valueOf(entry.getValue()[1]));
					int length = getClass.getLength(entry.getValue()[0]);
					map1.put(Integer.valueOf(entry.getValue()[1]), length);
					map2.put(Integer.valueOf(entry.getValue()[1]), entry.getKey());
					System.out.print(entry.getKey());
					for(int i = 0; i < length - entry.getKey().length(); i++){
						System.out.print(" ");
					}
					System.out.print(" ");
				}
			}
			System.out.println("");
			if(map1.size() == 0 || map2.size() == 0){
				System.out.println("No such a column!");
				return;
			}
			for(Map.Entry<Integer, String[]> entry : record.entrySet()){
				int rowId = entry.getKey();
				if(table.equalsIgnoreCase("davisbase_tables") || table.equalsIgnoreCase("davisbase_columns")){
					if(columnIndex.contains(1)){
						System.out.print(rowId);
						for(int j = 0; j < 5 - (rowId+"").length(); j++){
							System.out.print(" ");
						}
						System.out.print(" ");
					}
					for(int i = 2; i <= entry.getValue().length + 1; i++){
						if((columnIndex.size() != 0 && columnIndex.contains(i)) || columnIndex.size() == 0){
								System.out.print(entry.getValue()[i - 2]);
								for(int j = 0; j < Math.abs(Math.max(map1.get(i), map2.get(i).length()) - entry.getValue()[i - 2].length()); j++){
									System.out.print(" ");
								}
								System.out.print(" ");
						}
					}
					System.out.println("");
				}
				else{
					for(int i = 1; i <= entry.getValue().length; i++){
						if((columnIndex.size() != 0 && columnIndex.contains(i)) || columnIndex.size() == 0){
							    System.out.print(entry.getValue()[i - 1]);
							    for(int j = 0; j < Math.abs(Math.max(map1.get(i), map2.get(i).length()) - entry.getValue()[i - 1].length()); j++){
									System.out.print(" ");
								}
								System.out.print(" ");
						}
					}
					System.out.println("");
				}
				
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("No such a file exists!");
			e.printStackTrace();
		}
	}
	
	public static void deleteOperation(String tableName, String condition) throws IOException{
		RandomAccessFile file;
		if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")){
			file = new RandomAccessFile("data\\catalog\\" + tableName + ".tbl", "rw");
		}
		else{
			file = new RandomAccessFile("data\\user_data\\" + tableName + ".tbl", "rw");
		}
		String[] conditions = handleWhere(condition);
		int rowId = Integer.valueOf(conditions[2]);
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo(tableName);
		LinkedHashMap<Integer, String[]> data = getClass.getRecord(tableName, columnInfo, "row_id" + "=" + rowId);
		if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")){
			data = getClass.getRecord(tableName, columnInfo, "rowid" + "=" + rowId);
		}
		if(data.size() == 0){
			System.out.println("No such a record!");
			return;
		}
		int payLoad = 1;
		boolean first = true;
		for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			if(first && (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))){
				first = false;
				continue;		
			}
			String type = entry.getValue()[0];
			payLoad += getClass.getLength(type);
			payLoad += 1;
		}
		int page = -1;
		int k = 0;
		for(; k < (int)file.length() / pageSize; k++){
			file.seek(k * pageSize);
			if(file.readByte() == 0x0D){
				int minKey = -1;
				int maxKey = -1;
				file.seek(k * pageSize + 1);
				int numOfRecords = file.readByte();
				file.seek(k * pageSize + 8);
				short firstRecordPosition = file.readShort();
				file.seek(k * pageSize + 2);
				short lastRecordPosition = file.readShort();
				file.seek(firstRecordPosition + 2);
				minKey = file.readInt();
//				System.out.println(minKey);
				file.seek(lastRecordPosition + 2);
				if(lastRecordPosition + 2 > file.length()){
					System.out.println("No such record has been found!");
					return;
				}
				maxKey = file.readInt();
//				System.out.println(maxKey);
				file.seek(k * pageSize + 4);
				if(minKey == 0 || maxKey == 0){
					if(k == (int)file.length() / pageSize - 1){
						System.out.println("No such record has been found!");
						return;
					}
					else{
						continue;
					}
				}
				else if(minKey <= rowId && maxKey >= rowId){
					page = k;
					break;
				}
			}
		}
		if(page == -1){
			System.out.println("No such record has been found!");
			return;
		}
		file.seek(page * pageSize + 1);
		int numOfRecords = file.readByte();
		List<Short> positions = new ArrayList<>();
		file.seek(page * pageSize + 8);
		short startPosition = file.readShort();
		short target = -1;
		for(int i = 0; i < numOfRecords; i++){
			file.seek(page * pageSize + 8 + i * 2);
			short position = file.readShort();
			file.seek(position + 2);
			int temp = file.readInt();
			if(temp != rowId){
				positions.add(position);
				if(startPosition > position){
					startPosition = position;
				}
			}
			else{
				target = position;
			}
		}
		if(target == -1){
			System.out.println("No such record in the table!");
			return;
		}
		file.seek(page * pageSize + 1);
		file.writeByte(numOfRecords - 1);
		file.writeShort(startPosition);
		file.seek(page * pageSize + 8);
		for(short position : positions){
			file.writeShort(position);
		}
		file.writeShort(0);
		file.seek(target);
		for(int i = 0; i < payLoad + 6; i++){
			file.writeByte(0);
		}
		if(numOfRecords == 1){
			file.seek(page * pageSize + 2);
			file.writeShort((page + 1) * pageSize);
		}
		file.close();
	}
	
	public static void updateOperation(String tableName, List<String> action, String condition) throws IOException{
		RandomAccessFile file;
		if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")){
			file = new RandomAccessFile("data\\catalog\\" + tableName + ".tbl", "rw");
		}
		else{
			file = new RandomAccessFile("data\\user_data\\" + tableName + ".tbl", "rw");
		}
		String columnName = action.get(0).trim();
		String value = action.get(1).trim();
		LinkedHashMap<String, String[]> columnInfo = getClass.getColumnInfo(tableName);
		LinkedHashMap<Integer, String[]> record = getClass.getRecord(tableName, columnInfo, condition);
		if(record.size() == 0){
			System.out.println("No such a record!");
			return;
		}
		String primaryKey = "";
        for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			primaryKey = entry.getKey();
			break;
		}
        if(primaryKey.equalsIgnoreCase(columnName)){
        	System.out.println("You cannot revise primary keys!");
			return;
        }
		String columnType = ""; 
		int index = 0;
		for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
			if(entry.getKey().trim().equalsIgnoreCase(columnName)){
				columnType = entry.getValue()[0].trim();
				break;
			}
			index += 1;
		}
		if(columnInfo.containsKey("rowid") && (tableName.equalsIgnoreCase("davisbase_tables") 
				|| tableName.equalsIgnoreCase("davisbase_columns"))){
			index -= 1;
		}
		List<Integer> rowIdList = new ArrayList<>();
		for(Map.Entry<Integer, String[]> entry : record.entrySet()){
			rowIdList.add(entry.getKey());
		}
		int page = 0;
		for(Integer rowId : rowIdList){
			for(int i = 0; i < (int)file.length() / pageSize; i++){
				file.seek(i * pageSize);
				if(file.readByte() == 0x0D){
					file.seek(i * pageSize + 1);
					int numOfRecords = file.readByte();
					file.seek(i * pageSize + 8);
					short firstRecordPosition = file.readShort();
					file.seek(i * pageSize + 2);
					short lastRecordPosition = file.readShort();
					file.seek(firstRecordPosition + 2);
					int minKey = file.readInt();
//					System.out.println(minKey);
					file.seek(lastRecordPosition + 2);
					int maxKey = i;
					if(lastRecordPosition + 2 > (i+1) * pageSize){
						page = i;
						continue;
					}
				    maxKey = file.readInt();
//					System.out.println(maxKey);
					file.seek(i * pageSize + 4);
					int pointer = file.readInt();
					if(minKey == 0 || maxKey == 0){
						page = i;
					}
					else if(minKey <= rowId && maxKey >= rowId){
						page = i;
					}
				}
		}
			file.seek(page * pageSize);
			if(file.readByte() != 0x0D){
				continue;
			}
			file.seek(page * pageSize + 1);
			int numOfRecords = file.readByte();
			for(int j = 0; j < numOfRecords; j++){
				file.seek(page * pageSize + 8 + j * 2);
				short position = file.readShort();
				file.seek(position + 2);
				int temp = file.readInt();
				if(temp == rowId.intValue()){
					file.seek(position + 6);
					int columnNum = file.readByte();
					int prePayLoad = 0;
					for(int m = 0; m < index; m++){
						file.seek(position + 7 + m);
						if(((rowId == 1 || rowId == 2)&&tableName.equalsIgnoreCase("davisbase_tables"))
								|| ((rowId >= 1 || rowId <= 10)&&tableName.equalsIgnoreCase("davisbase_columns"))){
							prePayLoad += getClass.getSystemTypeLength(file.readByte());
						}
						else{
							prePayLoad += getClass.getTypeLength(file.readByte());
						}
					}
					file.seek(position + 7 + columnNum + prePayLoad);
					if(columnType.equalsIgnoreCase("TINYINT")){
						file.write(Integer.valueOf(value));
					}
					else if(columnType.equalsIgnoreCase("SMALLINT")){
						file.writeShort(Short.valueOf(value));
					}
					else if(columnType.equalsIgnoreCase("INT")){
						file.writeInt(Integer.valueOf(value));
					}
					else if(columnType.equalsIgnoreCase("BIGINT")){
						file.writeLong(Long.valueOf(value));
					}
					else if(columnType.equalsIgnoreCase("REAL") || columnType.equalsIgnoreCase("FLOAT")){
						file.writeFloat(Float.valueOf(value));
					}
					else if(columnType.equalsIgnoreCase("DOUBLE")){
						file.writeDouble(Double.valueOf(value));
					}
					else if(columnType.equalsIgnoreCase("DATETIME")){
						String[] date = value.split("_")[0].split("-");
						String[] time = value.split("_")[1].split(":");
						ZoneId zoneId = ZoneId.of("America/Chicago");
						ZonedDateTime zonedTime = ZonedDateTime.of(Integer.valueOf(date[0]), Integer.valueOf(date[1]), Integer.valueOf(date[2]),
								Integer.valueOf(time[0]), Integer.valueOf(time[1]), Integer.valueOf(time[2]), 1234, zoneId);
						file.writeLong(zonedTime.toInstant().toEpochMilli() / 1000);
					}
					else if(columnType.equalsIgnoreCase("DATE")){
						String[] date = value.split("-");
						ZoneId zoneId = ZoneId.of("America/Chicago");
						ZonedDateTime zonedTime = ZonedDateTime.of(Integer.valueOf(date[0]), Integer.valueOf(date[1]), Integer.valueOf(date[2]),
								0, 0, 0, 1234, zoneId);
						file.writeLong(zonedTime.toInstant().toEpochMilli() / 1000);
					}
					else{
						file.writeBytes(value);
						for(int i = 0; i < 20 - value.length(); i++){
							file.writeByte(0);
						}
					}
				}
			}	
	}	
		file.close();
	}
	
	public static boolean enoughSpace(RandomAccessFile file, int page, int payLoad) throws IOException{
		file.seek(pageSize * page + 1);
		int headerSpace = 8 + 2 * file.readByte() + 2;
		file.seek(pageSize * page + 2);
		int usedSpace = (page + 1) * pageSize - file.readShort() + headerSpace;
		int leftSpace = pageSize - usedSpace;
		if(leftSpace >= payLoad + 6){
			return true;
		}
		else{
			return false;
		}
	}
	
	public static int newPage(RandomAccessFile file, int page) throws IOException{
		file.setLength((page + 2) * pageSize);
		file.seek((page + 1) * pageSize);
		file.write(0x0D);
		file.seek((page + 1) * pageSize + 2);
		file.writeShort((page + 2) *pageSize);
		file.writeInt(-1);
		return page + 2;
	}
	
	public static String[] handleWhere(String condition){
		if(condition.length() == 0){
			return new String[0];
		}
		else{
			String[] parts = new String[3];
			if(condition.contains("=") && !condition.contains("!=")){
				parts[0]=condition.split("=")[0].trim();
				parts[1]="=";
				parts[2]=condition.split("=")[1].trim();
			}
			else if(condition.contains("!=")){
				parts[0]=condition.split("!=")[0].trim();
				parts[1]="!=";
				parts[2]=condition.split("!=")[1].trim();
			}
			else if(condition.contains("<")){
				parts[0]=condition.split("<")[0].trim();
				parts[1]="<";
				parts[2]=condition.split("<")[1].trim();
			}
			else if(condition.contains(">")){
				parts[0]=condition.split(">")[0].trim();
				parts[1]=">";
				parts[2]=condition.split(">")[1].trim();
			}
			else if(condition.contains("<=")){
				parts[0]=condition.split("<=")[0].trim();
				parts[1]="<=";
				parts[2]=condition.split("<=")[1].trim();
			}
			else if(condition.contains(">=")){
				parts[0]=condition.split(">=")[0].trim();
				parts[1]=">=";
				parts[2]=condition.split(">=")[1].trim();
			}
		    return parts;
		}
	}
	
}
