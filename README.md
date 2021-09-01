# opiateDatabase
Uploading opiate LCMS XML files to a postgres or mysql database.

### Dependencies
Python 3.7.x
A UNIX-like operating system (Linux, OS X). Not tested on Windows.

### PostgreSQL Installation & Create Local Database
If you don't have PostgreSQL installed on your computer. Open the following link and follow the instructions to download and install. https://postgresapp.com/

Run Postgres app and click 'PSQL' to open the command line. 

In the command line type:
```
  CREATE DATABASE opiates;
```
To make sure the database was successfully created, you should issue the command (from the postgres command prompt):
```
  \l
```
You should see output similar to:
```
  opiates    | postgres | UTF8     | en_US.UTF-8 | en_US.UTF-8 
```

To create a user, other than the admin postgres default user, issue the command
```CREATE USER username WITH PASSWORD 'password'; ```
where username is the name of the user you want to create and 'password' is the password chosen for the user.

You can use the command ```\du``` to make sure the user was created. As you will see in the output, the user has no privileges. To give the new user privileges, issue the command ```GRANT ALL PRIVILEGES ON DATABASE opiates TO user; ```(where user is the actual username to be given privileges).

### Connecting With Your Graphical Interface
Postgres.app includes psql, a command line client for PostgreSQL. However, there are plenty of great graphical clients available for PostgreSQL listed on the [postgress.app website](https://postgresapp.com/documentation/gui-tools.html).

[TablePlus](https://tableplus.com/) is a GUI with the capability to create, access, query & edit multiple relational databases. Follow the link to download and install TablePlus to your computer. 

- Once installed, open the TablePlus app, from the welcome screen, click *Create a New Connection* at the bottom, or right-click on the connection view and choose *New*.
- Choose database type and hit *Create*.
- Configure your connection by using the opiates as the database name, your username and password just created plus standard properties such as host, port, etc. 

![Screenshot](support_docs/tableplus_screenshot.png)

## Installation
To create a virtual environment, first you need to install virtualenv. Depending on your computer, use ```pip3``` if Python3 is not your default Python interpreter.
```
pip install virtualenv
```
Then create a folder and activate a virtual environment where 'projectname' is the name of your project
```
mkdir ~/Projects/OpiatesDB
cd ~/Projects/OpiatesDB
python3 -m venv projectname
source projectname/bin/activate
```
Clone the repository.
```
(venv) user@machine $ git clone git@url-to-repo/opiate_postgres.git
```
Then navigate to the subfolder
```
(venv) user@machine $ cd projectname/opiate_postgres
```
Install the dependencies (You will not need to use ```pip3``` here. 
```
(venv) user@machine $ pip -r requirements.txt
```
### Link databases with environment variables
Once the virtual environment has been created, edit the "activate" file by in the environment directory by 
```
nano projectname/bin/activate
```
There should be a definition for deactivate in the activate script, and you want to unset your variables at the end of it:
```
deactivate () {
    ...

    # Unset My Server's variables
    unset MY_SERVER_NAME
    unset MY_DATABASE_URL
}
```
Then at the end of the activate script, set the variables:
```
export PG_DB="postgresql://username:password@localhost/opiates"
export SQLITE_DB="sqlite:////Path/to/file/opiates.db"
```
It is important that no spaces exists between ```PG_DB```/```SQLITE_DB``` and the ```="..."``` as the variables won't be recongized properly. Also note, the username and password are those you created earlier for the postgres database. The path to you SQLite database is where you would like the database to exist. Recommend placing the database within the project folder such as ```/Projects/OpiatesDB/opiates.db```. Save and exit the editor. 

To test the changes, activate the environment and ```echo``` the variable and the output should match the URL you just placed.
```
source projectname/bin/activate
(venv) user@machine $ echo $PG_DB
postgresql://username:password@localhost/opiates

(venv) user@maching $ echo $SQLITE_DB
sqlite:////Path/to/file/opiates.db
```
After seeing the variables identified successfully, deactivate the virtual environment by typing ```deactivate``` into the terminal. Then, type the commands like above but not we will see blank lines for the variables as the variables are specific to the environment.
```
user@machine $ echo $PG_DB

user@maching $ echo $SQLITE_DB

```

### Using the Program
For development, it is convenient to run the program from within the project directory where the file xml_db.py exists.

Commands are constructed as follows. Start by typing ```python``` into the command line, followed by a space the program file then an XML file or directory which contains the XML files that you wish to upload. For example:
```
python xml_db.py path/to/file.xml
```
or 
```
python xml_db.py path/to/directory
```
By default, the program uses postgres. If you wish to use MySQL, type ```-db sqlite``` in between the program file and the XML file or directory such as:
```
python xml_db.py -db sqlite path/to/file.xml
```

Help text is available for the program by using the -h or --help options:
```
python xml_db.py -h

usage: xml_db_test.py [-h] [-db {postgres,sqlite}] user_input

Place opiate XML files into SQL tables.

positional arguments:
  user_input            The path to the XML file or directory with the XML
                        files.

optional arguments:
  -h, --help            show this help message and exit
  -db {postgres,sqlite}, --database {postgres,sqlite}
                        Choose between a PostgreSQL or SQLite database
  -re, --replace        Replace file X with the a new one of the same name.
```
If the file exists in the database, you will see a message which reads where *xml_file* is the name of the xml file chosen for uploading:
```
"xml_file" found in db. Not added to db
```
Otherwise, another message will appear indicating it will proceed to upload the file:
```
"xml_file" not in db, will continue
```

### Output
The program creates three tables Batch, Calibration, and Results.

The Batch table contains batch level data including:
- xml_file: Name of XML file
- modified_filename: Name of XML file to fit proper filename nomenclature
- timestamp: Time XML file was created
- instrument: Serial number of the MS instument
- numb_sample: The number of samples in the LC batch, not including water samples.

The Calibration table contains compound specific information for that LCMS batch including:
- xml_file: Name of XML file associated with the batch when the calibration curve data was injected and gathered
- modified_filename: Name of XML file to fit proper filename nomenclature
- timestamp: Time calibration curve was generated
- compound: Name of opiate compound
- curve_type: linear or reference point (RF)
- origin: Decision to include the origin in the calibration curve
- weighting: The weight given to each point in the calibration curve (x, 1/x, 1/x^2)
- slope: Slope of the curve
- rsquared: The R^2 value of fit of the calibration curve
- cc: continuing calibration
- int_std_id: The identification number of the internal standard associated with the compound as given by the LCMS method
- int_std: The name of the internal standard

The Results table contains the metadata of each injection by compound. Information in the 21 columns includes:
- xml_file: The XML file associated with the sample injection
- modified_filename: Name of XML file to fit proper filename nomenclature
- lc_batch: The LCMS batch associated with the sample injection
- injection_time: Time of sample injection
- vial: Vial position on sample plate
- sample: The name of the sample
- sample_id: The number in the sequence of the LCMS batch
- sample_type: The type of sample (calibrator, control, unknown)
- compound: The name of the opiate compound
- compound_id: The identification number of the compound as programmed in the LCMS method
- peak_area: The value of the area under the chromatographic curve
- concentration: The calculated concentration from peak area relative to the calibration curve
- RT: The chromatographic retention time of the compound peak
- SN: The signal-to-noise ratio of the compound's chromatographic peak
- confirming_ion_area: The value of the area under the chromatographic curve of the confirming ion
- int_std_id: The identification number of the internal standard compound as programmed in the LCMS method
- int_std: The name of the internal standard associated with the compound
- is_peak_area: The value of the area under the chromatographic curve for the internal standard 
- is_RT: The chromatographic retention time of the internal standard peak
- is_SN: The signal-to-noise ratio of the internal standard's chromatographic peak
- is_confirming_ion_area: The value of the area under the chromatographic curve of the internal standard's confirming ion

