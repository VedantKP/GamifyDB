from flask import Flask, render_template, url_for, request, g
import psycopg2
import atexit

app = Flask(__name__)
app.config['DATABASE_URI'] = 'postgresql://postgres:<password>@localhost:5432/gamify_db'
conn = None

class Game():
    def __init__(self, id, name, rating, votes, year, url, plot, genre, na_sales, eu_sales, jp_sales, other_sales, global_sales, company_name=None, company_url=None, company_country=None, company_startyear=None):
        self.id = id
        self.name = name
        self.rating = rating
        self.votes = votes
        self.year = year
        self.url = url
        self.plot = plot
        self.genre = genre
        self.na_sales = na_sales
        self.eu_sales = eu_sales
        self.jp_sales = jp_sales
        self.other_sales = other_sales
        self.global_sales = global_sales
        self.company_name = company_name
        self.company_url = company_url
        self.company_country = company_country
        self.company_startyear = company_startyear
    

    def __str__(self) -> str:
        return f"id: {self.id}, name: {self.name}, rating: {self.rating}, votes: {self.votes}, year: {self.year}, url: {self.url}, plot: {self.plot}, "\
            f"genre: {self.genre}, na_sales: {self.na_sales}, eu_sales: {self.eu_sales}, jp_sales: {self.jp_sales}, other_sales: {self.other_sales}, global_sales: {self.global_sales}"\
            f"company_name: {self.company_name}, company_url: {self.company_url}, company_country: {self.company_country}, company_startyear: {self.company_startyear}"

@app.route('/game/<int:id>')
def display_game(id):
    db = get_db()
    cursor = db.cursor()
    query = 'select g.id as id, g.name as name, g.rating as rating, g.votes as votes, g.year as year, g.url as url, g.plot as plot, g.genre as genre, g.na_sales as na_sales, g.eu_sales as eu_sales, g.jp_sales as jp_sales, g.other_sales as other_sales, g.global_sales as global_sales, c.company_name as company_name, c.company_url as company_url, c.company_country as company_country, c.company_startyear as company_startyear from game g inner join game_developers on g.id = game_developers.game_id inner join company c on game_developers.company_id = c.id where g.id = {}'.format(id)
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) == 0: #when game developer data is not available
        query = 'select * from game where id = {}'.format(id)
        cursor.execute(query)
        result = cursor.fetchone()
        print('result for no company data is: \n{}'.format(result))
        result = [Game(*result)]
    else:
        result = [Game(*row) for row in result]
    columns = [desc[0] for desc in cursor.description]
    data = {
        "columns": columns,
        "resultSet": result
    }
    # print('result after using Games objects: {}'.format([vars(x) for x in result]))
    # print('columns are: {}'.format(columns))
    return render_template('game.html', data=data)
    # return "work in progress"


@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        query = request.form['query']
        if query is not None and query != "":
            db = get_db()
            cursor = db.cursor()
            idx = query.index(' ')
            query = query[:idx] + ' id,' + query[idx:]
            print('query is: {}'.format(query))
            cursor.execute(query=query)
            result = cursor.fetchall()
            # print('cursor.description: {}'.format(cursor.description))
            # cursor.description = [desc for desc in cursor.description if desc[0] != 'id']
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            data = {
                "columns": columns, 
                "resultSet": result,
                "ids": list(map(lambda x: x[0], result))
            }
            # print('column names are: {}'.format(columns))
            # ids = list(map(lambda x: x[0], result))
            print('data obtained for query: {} is\n {}'.format(query,result))
            print('query result type: {}'.format(type(result)))
            # print('ids = {}'.format(ids))
            # print('ids type = {}'.format(type(ids)))
            print('data being sent is:\n{}'.format(data))
            return render_template("index.html", data=data)
    else:
        db = get_db()
        if db is not None:
            print('db connection available in index()')
        return render_template('index.html')
    
def get_db():
    global conn
    if conn is None:
        print('***opening connection to db!***')
        conn = psycopg2.connect(app.config['DATABASE_URI'])
    return conn

with app.app_context():
    db = get_db()
    if db is not None:
        print('connected to db!')
    else:
        print('could not connect to db!')


def custom_enumerate(iterable, start=0):
    """
    Enumerate function for iterating over an iterable with index and value.

    Parameters:
    - iterable: The iterable to enumerate.
    - start: Optional. The starting index (default is 0).

    Returns:
    - An iterator that yields tuples containing index and value.
    """
    index = start
    for value in iterable:
        yield index, value
        index += 1

@app.context_processor
def inject_enumerate():
    return dict(enumerate=custom_enumerate)

def close_db():
    global conn
    if conn is not None:
        conn.close()
        
atexit.register(close_db)

if __name__ == "__main__":
    app.run(debug=True,port=5000)