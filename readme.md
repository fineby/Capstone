THE PROJECT DETAILS AND INFORMATION:
    Under the scope of Capstone project, which a described at the file “Data Engineering - Capstone Project Requirements Document.pdf” at the Doc directory, the raw json data from four files was loaded to PySpark dataframes, cleaned and mapped in accordance with instructions (file Mapping Document.xlsx at the Doc directory). To add missed phone codes and transform US states abbreviations to the source directory “Assets” was added two additional csv files. All charts placed in the file “Capstone charts.pdf” at the Doc directory. Virtual Environment with “requirements.txt” placed at the Doc directory.
	The project used plenty of Python’s libraries (pandas, numpy, re, os, time, datetime), PySpark, as well as local interaction with MySQL database by “jdbc” and “mysql” connectors.
	The main project was divided to three Python files (with relative path to the program folder):
	1. ‘etl’, where data loaded, cleaned and loaded to the MySQL Database;
	2. ‘main’,which manage all modules, loaded from database, made all requests to create output for ‘Main Menu’ positions, including Pandas dataframes for charts;
	3. ‘charts’, which creates all charts based on graphic components of Matplotlib, Seaborn, Plotly and Folium libraries.
    Additionally was created and added to the git ignore file with default credentials for the MySQL database.
    To demonstrate coding skills the code did not strictly follow DRY principle, used PySpark functional style of request or created SQL request with help of databases “temp view” option, for the same coding problem, but in different parts of Capstone project (so, in some place same timestamp used in string format for manipulation, in other place used date format).
    In a functional way the code has a cycle structure, when the user is able to repeat any data request from “Main Menu” or inside the request. The exit from the program is possible only from ‘Main Menu’.

TECHNICAL CHALLENGES

ETL Module:
	The data loaded in series from json files, cleaned, re-ordered as needed by project condition with help of PySpark and Pandas. Was used two different styles for schemas and one ‘load as it is’ to re-order fields at the dataframes. All initial types for the fields were determined, as a string, except the field for the key. After all transformations, types for non-string fields were changed as needed.
	The data from the link to the end point loaded and displayed code for the operation.
	Data was saved to the MySQL database (database dropped, in case it exists) by function. For all string types the maximum length of the field was calculated to use at the command to change type to the varchar type at the MySQL database.
	For execution conditions with capitalized names for tables in SQL MySQL was reinstalled with an option to allow such names at the database (screenshots MySQL_install_1.png, MySQL_install_2.png at the Doc directory).

MAIN Module:
	All data from each table was loaded from the database to PySpark dataframes. Some tables were loaded as “temp view” for further use for SQL requests, for one “temp view” string data for date field was converted to date format.
	Was created all Pandas dataframe with the help of four PySpark functional commands and two SQL requests. To create one of the data frames a technique was used to replace list values at the dictionary to one calculated value with further transformation to the Pandas dataframe. A combination of joins with new line literals was used to create a column for the chart, where all new words start from a new line. Month number was converted to the month name.
	‘Main Menu’ part allows you to make requests as separate functions (three with PySpark and same with SQL requests). Functions filtering user input and allow request or database modifications (by key field “SSN”) only in case the input fulfills certain conditions, as well some requests autocorrect user input (like missed first zero in date or month, low or capitalize letters for the name). Two filters for modifications used Regex to check phone format and Email format. Some functions allow found data with different user input to the same request.

CHARTS Module:
	All functions from this module received Pandas dataframe, which need to be displayed. When constructed with help of Folium library, as well loaded geotags for the US states to connect it with state names at the Pandas dataframe.
	Chart divided to two groups, as pointed at the Capstone task. Each group has one last chart which displays at the browser, with the program back to the “Main Menu” (states map saved and delayed from the disk to allow such kind of operation, Plotly charts is interactive). 
	All charts are different types and based on the color schema, which was recommended by designers.
