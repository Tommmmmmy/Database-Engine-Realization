        SHOW TABLES;                                     Display all tables.
	CREATE TABLE table_name;                         Create a table
	SELECT * FROM table_name;                        Display all records in the table.
	SELECT * FROM table_name WHERE ();               Display records which satisfies the condition.
	INSERT INTO table_name () VALUES ();             Insert a record into the table.
	DELETE FROM table_name WHERE row_id = <value>;   Deleate a record whose row_id is <value>.
	UPDATE table_name SET () WHERE ();               Update a record.
	DROP TABLE table_name;                           Remove table data and its schema.

Please make sure that your test cases follow the formats above.

In davisbase_tables table, two columns (record_count and avg_length) are useless which are also optional according 
to the professor's requirement.

I determine whether a table exists in the database by the means of seeing if the name is or not in davisbase_tables table
instead of checking if there exists a file with the name in the user_data directory.

You cannot revise anything in the system tables except when you create a table!

For null values, I specify them as 0s. 

For the security, I demand the application that it cannot revise primary keys.