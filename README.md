# GamifyDB
Information Integration Project on Video Games data integration - Universität Stuttgart

Steps to run the project:
1. Ensure postgres is setup correctly
2. Run "python create_schema.py" to create schema in postgres
3. Run "python insert_data_db.py" to insert the data in your newly created database.
4. Change to flask-app folder: "cd flask-app"
5. Create a new virtual environment: "virtualenv env"
6. Activate virtual env (Windows): ".\env\Scripts\activate"
7. Install all requirements: "pip install requirements.txt"
8. Run the flask app: "python app.py"
9. Open localhost:5000 to view output.  
