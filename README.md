Python script to create and populate a mySQL database using .csv data from [National Center for Education Statistics IPEDS portal](https://nces.ed.gov/ipeds/datacenter/institutionlist.aspx?stepId=1).

Uses Python 2.7.12

After downloading project and setting up virtual environment, run `pip install -r requirements.txt`  to install dependencies. Update settings.py with the information for the database you will populate.

Script dynamically creates tables using SQLAlchemy based on .csv column names and user-inputted column type.
 
An assumption is made that a primary table ('school': name can be changed in settings.py) will be created: each row representing a school where `UnitId` is the primary key (this can be changed in settings.py).

After the primary table is created, tables containing other data can be added. For each subsequent table, `UnitId` will represent a Foreign Key, referencing the primary table.

To run: `python run.py`

To run tests: `python tests.py`