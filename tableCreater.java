import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class tableCreater {
	final static int pageSize = 512;

	public static void createFiles() throws IOException{
		File file = new File("data\\catalog\\davisbase_tables.tbl");
		file.getParentFile().getParentFile().mkdir();
		file.getParentFile().mkdir();
		file.createNewFile();
		file = new File("data\\catalog\\davisbase_columns.tbl");
		file.createNewFile();
		file = new File("data\\user_data");
		file.mkdir();
		createTable();
		createColumn();
	}
	
	public static void createTable() throws IOException{
		RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
		file.setLength(pageSize);
		file.seek(0);
		file.write(0x0D);
		file.write(2);
		int lenInt = 4;
		int lenShort = 2;
		int recordLength1 = lenShort + lenInt + 1 + 3 + "davisbase_tables".length() + lenInt + lenShort ;
		int recordIndex1 = pageSize - recordLength1;
		int recordLength2 = lenShort + lenInt + 1 + 3 + "davisbase_columns".length() + lenInt + lenShort;
		int recordIndex2 = recordIndex1 - recordLength2;
		file.writeShort(recordIndex2);
		file.writeInt(-1);
		file.writeShort(recordIndex1);
		file.writeShort(recordIndex2);
		
		file.seek(recordIndex1);
		file.writeShort(recordLength1 - 6);
		file.writeInt(1);
		file.write(0x03);
		file.write(0x1C);
		file.write(0x06);
		file.write(0x05);
		file.writeBytes("davisbase_tables");
		file.writeInt(2);
		file.writeShort(34);
		
		file.seek(recordIndex2);
		file.writeShort(recordLength2 - 6);
		file.writeInt(2);
		file.write(0x03);
		file.write(0x1D);
		file.write(0x06);
		file.write(0x05);
		file.writeBytes("davisbase_columns");
		file.writeInt(10);
		file.writeShort(40);
		
		file.close();
	}
	
	public static void createColumn() throws IOException{
		RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
		file.setLength(pageSize);
		file.seek(0);
		file.write(0x0D);
		file.write(10);
		int[] recordIndex = new int[10];
		int[] recordLength = new int[10];
		int lenInt = 4;
		int lenShort = 2;
		recordLength[0] = lenShort + lenInt + 1 + 5 + "davisbase_tables".length() 
				+ "rowid".length() + "INT".length() + 1 + "NO".length();
		recordLength[1] = lenShort + lenInt + 1 + 5 + "davisbase_tables".length() 
				+ "table_name".length() + "TEXT".length() + 1 + "NO".length();
		recordLength[2] = lenShort + lenInt + 1 + 5 + "davisbase_tables".length() 
				+ "record_count".length() + "INT".length() + 1 + "NO".length();
		recordLength[3] = lenShort + lenInt + 1 + 5 + "davisbase_tables".length() 
				+ "avg_length ".length() + "SMALLINT".length() + 1 + "NO".length();
		recordLength[4] = lenShort + lenInt + 1 + 5 + "davisbase_columns".length() 
				+ "rowid".length() + "INT".length() + 1 + "NO".length();
		recordLength[5] = lenShort + lenInt + 1 + 5 + "davisbase_columns".length() 
				+ "table_name".length() + "TEXT".length() + 1 + "NO".length();
		recordLength[6] = lenShort + lenInt + 1 + 5 + "davisbase_columns".length() 
				+ "column_name".length() + "TEXT".length() + 1 + "NO".length();
		recordLength[7] = lenShort + lenInt + 1 + 5 + "davisbase_columns".length() 
				+ "data_type".length() + "TEXT".length() + 1 + "NO".length();
		recordLength[8] = lenShort + lenInt + 1 + 5 + "davisbase_columns".length() 
				+ "ordinal_position".length() + "TINYINT".length() + 1 + "NO".length();
		recordLength[9] = lenShort + lenInt + 1 + 5 + "davisbase_columns".length() 
				+ "is_nullable".length() + "TEXT".length() + 1 + "NO".length();
		recordIndex[0] = pageSize - recordLength[0];
		for(int i = 1; i < 10; i++){
			recordIndex[i] = recordIndex[i - 1] - recordLength[i];
		}
		file.writeShort(recordIndex[9]);
		file.writeInt(-1);
		for(int i = 0; i < 10; i++){
			file.writeShort(recordIndex[i]);
		}
		
		file.seek(recordIndex[0]);
		file.writeShort((short)((int)recordLength[0] - lenShort - lenInt));
		file.writeInt(1);
		file.write(5);
		file.write(12 + "davisbase_tables".length());
		file.write(12 + "rowid".length());
		file.write(12 + "INT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_tables");
		file.writeBytes("rowid");
		file.writeBytes("INT");
		file.write(1);
		file.writeBytes("NO");
		
		file.seek(recordIndex[1]);
		file.writeShort((short)((int)recordLength[1] - lenShort - lenInt));
		file.writeInt(2);
		file.write(5);
		file.write(12 + "davisbase_tables".length());
		file.write(12 + "table_name".length());
		file.write(12 + "TEXT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_tables");
		file.writeBytes("table_name");
		file.writeBytes("TEXT");
		file.write(2);
		file.writeBytes("NO");
		
		file.seek(recordIndex[2]);
		file.writeShort((short)((int)recordLength[2] - lenShort - lenInt));
		file.writeInt(3);
		file.write(5);
		file.write(12 + "davisbase_tables".length());
		file.write(12 + "record_count".length());
		file.write(12 + "INT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_tables");
		file.writeBytes("record_count");
		file.writeBytes("INT");
		file.write(3);
		file.writeBytes("NO");
		
		file.seek(recordIndex[3]);
		file.writeShort((short)((int)recordLength[3] - lenShort - lenInt));
		file.writeInt(4);
		file.write(5);
		file.write(12 + "davisbase_tables".length());
		file.write(12 + "avg_length".length());
		file.write(12 + "SMALLINT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_tables");
		file.writeBytes("avg_length");
		file.writeBytes("SMALLINT");
		file.write(4);
		file.writeBytes("NO");
		
		file.seek(recordIndex[4]);
		file.writeShort((short)((int)recordLength[4] - lenShort - lenInt));
		file.writeInt(5);
		file.write(5);
		file.write(12 + "davisbase_columns".length());
		file.write(12 + "rowid".length());
		file.write(12 + "INT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_columns");
		file.writeBytes("rowid");
		file.writeBytes("INT");
		file.write(1);
		file.writeBytes("NO");
		
		file.seek(recordIndex[5]);
		file.writeShort((short)((int)recordLength[5] - lenShort - lenInt));
		file.writeInt(6);
		file.write(5);
		file.write(12 + "davisbase_columns".length());
		file.write(12 + "table_name".length());
		file.write(12 + "TEXT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_columns");
		file.writeBytes("table_name");
		file.writeBytes("TEXT");
		file.write(2);
		file.writeBytes("NO");
		
		file.seek(recordIndex[6]);
		file.writeShort((short)((int)recordLength[6] - lenShort - lenInt));
		file.writeInt(7);
		file.write(5);
		file.write(12 + "davisbase_columns".length());
		file.write(12 + "column_name".length());
		file.write(12 + "TEXT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_columns");
		file.writeBytes("column_name");
		file.writeBytes("TEXT");
		file.write(3);
		file.writeBytes("NO");
		
		file.seek(recordIndex[7]);
		file.writeShort((short)((int)recordLength[7] - lenShort - lenInt));
		file.writeInt(8);
		file.write(5);
		file.write(12 + "davisbase_columns".length());
		file.write(12 + "data_type".length());
		file.write(12 + "TEXT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_columns");
		file.writeBytes("data_type");
		file.writeBytes("TEXT");
		file.write(4);
		file.writeBytes("NO");
		
		file.seek(recordIndex[8]);
		file.writeShort((short)((int)recordLength[8] - lenShort - lenInt));
		file.writeInt(9);
		file.write(5);
		file.write(12 + "davisbase_columns".length());
		file.write(12 + "ordinal_position".length());
		file.write(12 + "TINYINT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_columns");
		file.writeBytes("ordinal_position");
		file.writeBytes("TINYINT");
		file.write(5);
		file.writeBytes("NO");
		
		file.seek(recordIndex[9]);
		file.writeShort((short)((int)recordLength[9] - lenShort - lenInt));
		file.writeInt(10);
		file.write(5);
		file.write(12 + "davisbase_columns".length());
		file.write(12 + "is_nullable".length());
		file.write(12 + "TEXT".length());
		file.write(0x04);
		file.write(12 + "NO".length());
		file.writeBytes("davisbase_columns");
		file.writeBytes("is_nullable");
		file.writeBytes("TEXT");
		file.write(6);
		file.writeBytes("NO");
		
		file.close();
	}
}
