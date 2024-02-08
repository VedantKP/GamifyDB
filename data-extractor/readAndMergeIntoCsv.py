# import psycopg2
import pandas as pd
from sqlalchemy import create_engine

db_url = 'postgresql://postgres:<password>@localhost:5432/gamify_db'

'''
Function reads all game data and extracts it into a CSV. This CSV is used for creating visualizations
'''
if __name__ == "__main__":
    engine = create_engine(db_url)
    df = pd.read_sql_query('select name, rating, votes, year, url, plot, genre, na_sales, eu_sales, jp_sales, other_sales, global_sales, company_name, company_url, company_country, company_startyear from "game" inner join "game_developers" on game.id = game_developers.game_id inner join "company" on game_developers.company_id = company.id',con=engine)
    df.to_csv('combinedData.csv',encoding='utf-8', index=False)
