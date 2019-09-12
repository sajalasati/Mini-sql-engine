# Mini-sql-engine
A mini SQL engine like MySQL (but fewer functionalties only) implemented using Python

## Usage
- The code has been developed and tested on __Python 3__.
- On running `engine.py`, it loads the schema as mentioned in `metadata.txt` file.
	- `metadata.txt` contains both the table names, alongwith the attribute names the tables. Please refer to the syntax as written in `metadata.txt` file for creating new tables / modify exisiting tables.
	- The table data, for table names mentioned in `metadata.txt` is present in __table_name.txt__ file within the same folder in csv format.
- For simplicity, attributes values are allowed to be integers only.
- Commands are terminated by a __semicolon__ as in MySQL.
- `python3 engine.py <valid sql query within double quotes>;`
	- Example: `python3 engine.py "select * from table_name where condition;"`

## Requirements
- Only `sqlparse==0.3.0` needs to be installed other than the regular python3 built-in modules.

## Supported Operations
- Also mentioned in `Assignment-1.pdf` file.
- Select all records from one or more tables:
	- eg. `Select * from table_name;`
- Aggregate functions: Simple aggregate functions on a single column
	- sum, average, max and min
	- eg. `Select max(col1) from table1;`
- Project Columns(could be any number of columns) from one or more tables:
	- eg. `Select col1, col2 from table_name;`
- Select/project with distinct from one or more tables:
	- eg. `Select distinct col1, col2 from table_name;`
- Select with where from one or more tables:
	- eg. `Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;`
- Projection of one or more(including all the columns) from two tables with one join condition:
	- eg.`Select * from table1, table2 where table1.col1=table2.col2;`
	- eg.`Select col1, col2 from table1,table2 where table1.col1=table2.col2;`