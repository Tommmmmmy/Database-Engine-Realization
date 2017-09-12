import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class getClass {
	final static int pageSize = 512;
	public static LinkedHashMap<String, String[]> getColumnInfo(String table) throws IOException{
		LinkedHashMap<String, String[]> recordType = new LinkedHashMap<>();
		try {
			RandomAccessFile file1 = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			RandomAccessFile file2 = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			long totalFileLength1 = file1.length();
			long totalFileLength2 = file2.length();
			int page1 = (int)(totalFileLength1 / pageSize);
			int page2 = (int)(totalFileLength2 / pageSize);
			for(int i = 0; i < page1; i++){
				file1.seek(i * pageSize);
				if(file1.readByte() == 0x05){
					continue;
				}
				else{
					file1.seek(i * pageSize + 1);
					int n = file1.readByte();
					short[] position = new short[n];
					file1.seek(i * pageSize + 8);
					for(int j = 0; j < n; j++){
						position[j] = file1.readShort();
					}
					boolean bool = false;
					for(int j = 0; j < n; j++){
						file1.seek(position[j] + 7);
						int length = new Integer(file1.readByte() - 0x0C);
						file1.seek(position[j] + 10);
						String tempName = "";
						for(int m = 0; m < length; m++){
							byte temp = file1.readByte();
							if(temp != 0){
								tempName += (char)temp;
							}		
						}
						if(tempName.equalsIgnoreCase(table)){
							bool = true;
							break;
						}
					}
					if(!bool){
						System.out.println("This table does not exist!");
						return recordType;
					}
				}
			}
			file1.close();
			for(int i = 0; i < page2; i++){
				file2.seek(i * pageSize);
				if(file2.readByte() == 0x05){
					continue;
				}
				else{
					file2.seek(i * pageSize + 1);
					int n = file2.readByte();
					short[] position = new short[n];
					file2.seek(i * pageSize + 8);
					for(int j = 0; j < n; j++){
						position[j] = file2.readShort();
					}
					for(int j = 0; j < n; j++){
						file2.seek(position[j] + 7);
						int length = getSystemTypeLength(file2.readByte());
						file2.seek(position[j] + 8);
						int lengthOfColumn = new Integer(file2.readByte() - 0x0C);
						file2.seek(position[j] + 9);
						int lengthOfType = new Integer(file2.readByte() - 0x0C);
						file2.seek(position[j] + 11);
						int lengthOfNullable = new Integer(file2.readByte() - 0x0C);
						String name = "";
						for(int m = 0; m < length; m++){
							byte temp = file2.readByte();
							if(temp != 0){
								name += (char)temp;
							}
						}
						String columnName = "";
						String type = "";
						int orPosition = 0;
						String nullable = "";
						for(int m = 0; m < lengthOfColumn; m++){
							byte temp = file2.readByte();
							if(temp != 0){
								columnName += (char)temp;
							}
						}
						for(int m = 0; m < lengthOfType; m++){
							byte temp = file2.readByte();
							if(temp != 0){
								type += (char)temp;
							}
						}
						orPosition = file2.readByte();
						for(int m = 0; m < lengthOfNullable; m++){
							byte temp = file2.readByte();
							if(temp != 0){
								nullable += (char)temp;
							}
						}
						String[] info = new String[3];
						info[0] = type;
						info[1] = orPosition + "";
						info[2] = nullable;
						if(name.equalsIgnoreCase(table)){
							recordType.put(columnName, info);
						}
				  }
				}
			}
			file2.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return recordType;
	}
	public static LinkedHashMap<Integer, String[]> getRecord(String table, LinkedHashMap<String, String[]> columnInfo, String condition) throws IOException{
		LinkedHashMap<Integer, String[]> map = new LinkedHashMap<Integer, String[]>();
		RandomAccessFile file = null;
		String[] conditions = Operations.handleWhere(condition);
		if(table.equalsIgnoreCase("davisbase_tables") || table.equalsIgnoreCase("davisbase_columns")){
			file = new RandomAccessFile("data\\catalog\\" + table + ".tbl", "rw");
		}
		else{
			file = new RandomAccessFile("data\\user_data\\" + table + ".tbl", "rw");
		}
		long totalFileLength = file.length();
		int page = (int)(totalFileLength / pageSize);
		for(int i = 0; i < page; i++){
			file.seek(i * pageSize);
			if(file.readByte() == 0x05){
				continue;
			}
			else{
				file.seek(i * pageSize + 1);
				int numOfRecord = file.readByte();
				short[] position = new short[numOfRecord];
				file.seek(i * pageSize + 8);
				for(int j = 0; j < numOfRecord; j++){
					position[j] = file.readShort();
				}
				for(int j = 0; j < numOfRecord; j++){
					file.seek(position[j] + 2);
					int rowId = file.readInt();
					int columnNum = file.readByte();
					byte[] type = new byte[columnNum];
					for(int m = 0; m < columnNum; m++){
						type[m] = file.readByte();
					}
					String[] data = new String[columnNum];
					for(int k = 0; k < columnNum; k++){
						if(type[k] == 0x00){
							file.readByte();
							data[k] = "null";
						}
						else if(type[k] == 0x01){
							file.readShort();
							data[k] = "null";
						}
						else if(type[k] == 0x02){
							file.readInt();
							data[k] = "null";
						}
						else if(type[k] == 0x03){
							file.readLong();
							data[k] = "null";
						}
						else if(type[k] == 0x04){
							data[k] = file.readByte() + "";
						}
						else if(type[k] == 0x05){
							data[k] = file.readShort() + "";
						}
						else if(type[k] == 0x06){
							data[k] = file.readInt() + "";
						}
						else if(type[k] == 0x07){
							data[k] = file.readLong() + "";
						}
						else if(type[k] == 0x08){
							data[k] = file.readFloat() + "";
						}
						else if(type[k] == 0x09){
							data[k] = file.readDouble() + "";
						}
						else if(type[k] == 0x0A){
							long temp = file.readLong();
							if(temp == 0){
								data[k] = "0000-00-00_00:00:00";
							}
							else{
								ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochSecond(file.readLong()), ZoneId.of("America/Chicago"));
								data[k] = time.toLocalDateTime() + "";
							}
						}
						else if(type[k] == 0x0B){
							long temp = file.readLong();
							if(temp == 0){
								data[k] = "0000-00-00";
							}
							else{
								ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochSecond(temp), ZoneId.of("America/Chicago"));
								data[k] = time.toLocalDate() + "";
							}
						}
						else{
							int length = new Integer(type[k] - 0x0C);
							String str = "";
							for(int n = 0; n < length; n++){
								byte temp = file.readByte();
								if(temp == 0){
									str += " ";
								}
								else{
									str += (char)temp;
								}
							}
							data[k] = str;
						}
					}
					if(satisfyCondition(table, columnInfo, data, rowId, conditions, type)){
						map.put(rowId, data);
					}
				}
			}
		}
		file.close();
		return map;
	}
   	
	public static int getLastKey(String table) throws IOException{
		int result = 0;
		RandomAccessFile file;
		if(table.equalsIgnoreCase("davisbase_tables") || table.equalsIgnoreCase("davisbase_columns")){
			file = new RandomAccessFile("data\\catalog\\" + table + ".tbl", "rw");
		}
		else{
			file = new RandomAccessFile("data\\user_data\\" + table + ".tbl", "rw");
		}
		for(int i = 0; i < (int)file.length() / pageSize; i++){
			file.seek(i * pageSize + 1);
			int numOfRecords = file.readByte();
			if(numOfRecords == 0){
				return result;
			}
			file.seek((i) * pageSize + 2);
			short maxPosition = file.readShort();
			file.seek(maxPosition + 2);
			int rowId = file.readInt();
			if(rowId > result){
				result = rowId;
			}
		}
		file.close();
		return result;
	}
	
	public static int getLength(String type){
		if(type.equalsIgnoreCase("TINYINT")){
			return 1;
		}
		else if(type.equalsIgnoreCase("SMALLINT")){
			return 2;
		}
		else if(type.equalsIgnoreCase("INT")){
			return 4;
		}
		else if(type.equalsIgnoreCase("BIGINT")){
			return 8;
		}
		else if(type.equalsIgnoreCase("REAL") || type.equalsIgnoreCase("FLOAT")){
			return 4;
		}
		else if(type.equalsIgnoreCase("DOUBLE")){
			return 8;
		}
		else if(type.equalsIgnoreCase("DATETIME") || type.equalsIgnoreCase("DATE")){
			return 8;
		}
		else if(type.equalsIgnoreCase("TEXT")){
			return 20;
		}
		else{
			return 0;
		}
	}
	
	public static byte getTypeCode(String type){
		if(type.equalsIgnoreCase("TINYINT")){
			return 0x04;
		}
		else if(type.equalsIgnoreCase("SMALLINT")){
			return 0x05;
		}
		else if(type.equalsIgnoreCase("INT")){
			return 0x06;
		}
		else if(type.equalsIgnoreCase("BIGINT")){
			return 0x07;
		}
		else if(type.equalsIgnoreCase("REAL") || type.equalsIgnoreCase("FLOAT")){
			return 0x08;
		}
		else if(type.equalsIgnoreCase("DOUBLE")){
			return 0x09;
		}
		else if(type.equalsIgnoreCase("DATETIME")){
			return 0x0A;
		}
		else if(type.equalsIgnoreCase("DATE")){
			return 0x0B;
		}
		else{
			return 0x0C + (byte)20;
		}
	}
	
	public static int getTypeLength(byte code){
		if(code == 0x00){
			return 1;
		}
		else if(code == 0x01){
			return 2;
		}
		else if(code == 0x02){
			return 4;
		}
		else if(code == 0x03){
			return 8;
		}
		else if(code == 0x04){
			return 1;
		}
		else if(code == 0x05){
			return 2;
		}
		else if(code == 0x06){
			return 4;
		}
		else if(code == 0x07){
			return 8;
		}
		else if(code == 0x08){
			return 4;
		}
		else if(code == 0x09){
			return 8;
		}
		else if(code == 0x0A){
			return 8;
		}
		else if(code == 0x0B){
			return 8;
		}
		else{
			return 20;
		}
	}
	
	public static int getSystemTypeLength(byte code){
		if(code == 0x00){
			return 1;
		}
		else if(code == 0x01){
			return 2;
		}
		else if(code == 0x02){
			return 4;
		}
		else if(code == 0x03){
			return 8;
		}
		else if(code == 0x04){
			return 1;
		}
		else if(code == 0x05){
			return 2;
		}
		else if(code == 0x06){
			return 4;
		}
		else if(code == 0x07){
			return 8;
		}
		else if(code == 0x08){
			return 4;
		}
		else if(code == 0x09){
			return 8;
		}
		else if(code == 0x0A){
			return 8;
		}
		else if(code == 0x0B){
			return 8;
		}
		else{
			return new Integer(code - 0x0C);
		}
	}
		
		public static boolean satisfyCondition(String table, LinkedHashMap<String, String[]> columnInfo, String[] data, int rowId, String[] conditions, byte[] type){
			if(conditions.length == 0){
				return true;
			}
			else{
				String columnType = ""; 
				int index = 0;
				long left = -1;
				if(conditions[0].equalsIgnoreCase("rowid")){
					left = rowId;
					columnType = "INT";
				}
				else{
					for(Map.Entry<String, String[]> entry : columnInfo.entrySet()){
						if(entry.getKey().equals(conditions[0])){
							columnType = entry.getValue()[0];
							break;
						}
						index += 1;
					}
				}
				if(table.equalsIgnoreCase("davisbase_tables") || table.equalsIgnoreCase("davisbase_columns")){
					String[] newData = new String[data.length + 1];
					newData[0] = String.valueOf(rowId);
					for(int i = 0; i < data.length; i++){
						newData[i + 1] = data[i];
					}
					data = newData;
				}
				if(columnType.equalsIgnoreCase("INT") || columnType.equalsIgnoreCase("TINYINT") || columnType.equalsIgnoreCase("SMALLINT") || columnType.equalsIgnoreCase("BIGINT") || columnType.equalsIgnoreCase("REAL") || columnType.equalsIgnoreCase("DOUBLE")
						){
					if(left == -1){
						
						left = Long.parseLong(data[index]);
					}
					long right = Long.parseLong(conditions[2]);
					
					if(conditions[1].equalsIgnoreCase("=")){
						if(left == right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("<")){
						if(left < right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase(">")){
						if(left > right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("<=")){
						if(left <= right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase(">=")){
						if(left >= right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("!=")){
						if(left != right){
							return true;
						}
					}
					else{
						System.out.println("Please type correct operators!");
						return false;
					}
				}
				else if(columnType.equalsIgnoreCase("DATETIME") || columnType.equalsIgnoreCase("DATE")){
					double left1 = Double.parseDouble(data[index]);
					double right = Double.parseDouble(conditions[2]);
					if(conditions[1].equalsIgnoreCase("=")){
						if(left1 == right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("<")){
						if(left1 < right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase(">")){
						if(left1 > right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("<=")){
						if(left1 <= right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase(">=")){
						if(left1 >= right){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("!=")){
						if(left1 != right){
							return true;
						}
					}
					else{
						System.out.println("Please type correct operators!");
						return false;
					}
				}
				else{
					if(index > data.length - 1){
						return false;
					}
					String left1 = data[index].trim();
					String right = conditions[2].replaceAll("\"", "");
					right = right.replaceAll("'", "").trim();
					if(conditions[1].equalsIgnoreCase("=")){
						if(left1.equalsIgnoreCase(right)){
							return true;
						}
					}
					else if(conditions[1].equalsIgnoreCase("!=")){
						if(!left1.equalsIgnoreCase(right)){
							return true;
						}
					}
					else{
						System.out.println("Please type correct operators!");
						return false;
					}
				}
				return false;
			}
		}
}
