# GamifyDB
Information Integration Project on Video Games data integration - Universität Stuttgart

Steps to run the project:
1. Clone the official git repository to your system: git clone  https://github.com/VedantKP/GamifyDB
2. Replace the “<password>” string with your postgres password in the following files:
      data-extractor/create_schema.py: in the dbDetails object on line 3.
      data-extractor/insert_data_db.py: in the connString on line 7.
      data-extractor/readAndMergeIntoCsv.py: in the db_url string on line.
      flask-app/app.py: in the app.config[‘DATABASE_URI’] string on line 6.
3. Open a command prompt.
4. Ensure your working directory is the cloned repository’s folder: GamifyDB
5. Install python’s virtualenv package: pip install virtualenv
6. Create a virtual environment: virtualenv env
7. Activate virtual environment: 
    For Windows: .\env\Scripts\activate
    For Mac and Linux: source env/bin/activate
8. Install all required packages: pip install -r requirements.txt
9. Change working directory to data-extractor: cd data-extractor  
10. Create Schema in postgres: python create_schema.py
11. Integrate all the data (Includes duplicate detection and fusion): python integrate_data.py
12. Insert data into the database: python insert_data_db.py 
13. Change working directory to flask-app: cd ..\flask-app\
14. Run the flask app: python app.py
15. Click on the link given in the console output or paste https://localhost:5000 in the browser to open the interface to the application.
