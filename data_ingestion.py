import os
from shutil import unpack_archive
from typing import Tuple
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine, sql
from dotenv import load_dotenv
import time


@task()
def extract_zipfiles(directory: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract file from zip archive into current directory"""
    for file in os.listdir(directory):
        unpack_archive(directory + file, directory)
    df_movies = pd.read_csv('./data/movie_dataset.csv')
    df_actors = pd.read_csv('./data/actorfilms.csv')
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            os.remove(directory + file)
    return df_movies, df_actors[['Actor', 'Film', 'Year']]


@task()
def fix_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Convert release_date to datetime"""
    df['release_date'] = pd.to_datetime(df['release_date'])
    return df


@task()
def create_genres_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create movies_genres and genres dataframes, fix the issue with Science Fiction"""
    movies_genres = []

    for genres_list, movie_id in zip(df['genres'], df['id']):
        if isinstance(genres_list, str):
            for g in genres_list.split():
                movies_genres.append((movie_id, g))

    df_movies_genres = pd.DataFrame(movies_genres, columns=['movie_id', 'genre'])
    df_movies_genres.loc[df_movies_genres['genre'] == 'Science', 'genre'] = 'Science Fiction'
    df_movies_genres = df_movies_genres[df_movies_genres['genre'] != 'Fiction']

    df_genres = pd.DataFrame(enumerate(df_movies_genres.genre.unique(), 1),
                             columns=['genre_id', 'genre'])
    df_movies_genres = df_movies_genres.merge(df_genres)[['movie_id', 'genre_id']]
    return df_movies_genres, df_genres


@task()
def create_directors_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create movies_directors and directors dataframes, fix the encoding issue"""
    df_directors = pd.DataFrame(enumerate(df['director'].unique(), 1),
                                columns=['director_id', 'director_name'])

    df_movies_directors = df[['id', 'director']].merge(df_directors, left_on='director', right_on='director_name') \
                            [['id', 'director_id']].rename(columns={'id': 'movie_id'})

    df_directors['director_name'] = df_directors['director_name'].str.encode('utf-8').str.decode('unicode-escape')
    return df_movies_directors, df_directors


@task()
def create_production_companies_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create movies_production_companies and production_companies dataframes"""
    movies_production_companies = []

    for prod_companies, movie_id in zip(df.production_companies, df.id):
        if isinstance(prod_companies, str):
            for company_data in eval(prod_companies):
                movies_production_companies.append((company_data['id'], company_data['name'], movie_id))

    df_movies_prod_companies = pd.DataFrame(movies_production_companies,
                                            columns=['production_company_id', 'production_company', 'movie_id'])
    df_production_companies = df_movies_prod_companies[['production_company_id', 'production_company']] \
        .drop_duplicates()
    return df_movies_prod_companies[['production_company_id', 'movie_id']], df_production_companies


@task()
def create_production_countries_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create movies_production_countries and production_countries dataframes"""
    movies_production_countries = []

    for prod_countries, movie_id in zip(df.production_countries, df.id):
        if isinstance(prod_countries, str):
            for country_data in eval(prod_countries):
                movies_production_countries.append((country_data['iso_3166_1'], country_data['name'], movie_id))

    df_movies_production_countries = pd.DataFrame(movies_production_countries,
                                                  columns=['production_country_id', 'production_country', 'movie_id'])

    df_production_countries = df_movies_production_countries[['production_country_id', 'production_country']] \
        .drop_duplicates()
    df_movies_production_countries = df_movies_production_countries[['production_country_id', 'movie_id']]
    return df_movies_production_countries, df_production_countries


@task()
def create_crew_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create movies_crew, crew, departments, jobs, crew_jobs dataframes"""
    movies_crew = []

    for crew_members, movie_id in zip(df.crew, df.id):
        if isinstance(crew_members, str):
            for crew_data in eval(crew_members):
                movies_crew.append((crew_data['id'],
                                    crew_data['name'],
                                    crew_data['department'],
                                    crew_data['job'],
                                    movie_id))

    df_movies_crew = pd.DataFrame(movies_crew,
                                  columns=['crew_member_id', 'name', 'department', 'job', 'movie_id'])
    df_movies_crew['name'] = df_movies_crew['name'].str.encode('utf-8').str.decode('unicode-escape')

    df_crew = df_movies_crew[['crew_member_id', 'movie_id']].drop_duplicates()

    df_departments = pd.DataFrame(enumerate(df_movies_crew['department'].unique(), 1),
                                  columns=['department_id', 'department_name'])

    df_jobs = df_movies_crew[['job', 'department']].merge(df_departments,
                                                          left_on='department',
                                                          right_on='department_name')
    df_jobs = df_jobs[['job', 'department_id']].drop_duplicates() \
        .reset_index() \
        .rename(columns={'index': 'job_id'})
    df_jobs['job_id'] += 1

    df_crew_jobs = df_movies_crew.merge(df_jobs)[['crew_member_id', 'job_id']].drop_duplicates()
    df_movies_crew = df_movies_crew[['movie_id', 'crew_member_id']].drop_duplicates()
    return df_movies_crew, df_crew, df_departments, df_jobs, df_crew_jobs


@task()
def create_languages_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create movies_spoken_languages and languages dataframes"""
    movies_spoken_languages = []

    for spoken_languages_list, movie_id in zip(df.spoken_languages, df.id):
        if isinstance(spoken_languages_list, str):
            for lang_data in eval(spoken_languages_list):
                movies_spoken_languages.append((lang_data['iso_639_1'], lang_data['name'], movie_id))

    df_movies_spoken_languages = pd.DataFrame(movies_spoken_languages,
                                              columns=['language_short', 'language', 'movie_id'])
    df_languages = df_movies_spoken_languages[['language_short', 'language']].drop_duplicates()
    df_movies_spoken_languages = df_movies_spoken_languages[['language_short', 'movie_id']]
    return df_movies_spoken_languages, df_languages


@task()
def create_statuses_tables(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create movies_statuses and statuses dataframes"""
    df_statuses = pd.DataFrame(enumerate(df['status'].unique(), 1),
                               columns=['status_id', 'status'])
    df_movies_statuses = df[['id', 'status']].merge(df_statuses)[['id', 'status_id']] \
        .rename(columns={'id': 'movie_id'})
    return df_movies_statuses, df_statuses


@task()
def create_actors_tables(actors_dataset: pd.DataFrame, movies_dataset: pd.DataFrame) -> (
        Tuple)[pd.DataFrame, pd.DataFrame]:
    """Create movies_actors and actors dataframes"""
    actors_dataset.columns = ['actor', 'title', 'year']
    movies_dataset['year'] = movies_dataset['release_date'].dt.year
    actors_dataset = actors_dataset.merge(movies_dataset[['id', 'title', 'year']], how='left')
    actors_dataset.dropna(inplace=True)
    df_actors = pd.DataFrame(enumerate(actors_dataset['actor'].unique(), 1),
                             columns=['actor_id', 'actor'])

    df_movies_actors = actors_dataset.merge(df_actors)[['actor_id', 'id']].rename(columns={'id': 'movie_id'})
    df_movies_actors = df_movies_actors.astype(int)
    return df_movies_actors, df_actors


@task()
def create_movies_table(df: pd.DataFrame,
                        df_movies_directors: pd.DataFrame,
                        df_languages: pd.DataFrame,
                        df_movies_statuses: pd.DataFrame) -> pd.DataFrame:
    """Create movies dataframe"""
    df_movies = df.merge(df_movies_directors, left_on='id', right_on='movie_id') \
        .merge(df_languages, left_on='original_language', right_on='language_short') \
        .merge(df_movies_statuses, left_on='id', right_on='movie_id')
    return df_movies[['id', 'title', 'homepage', 'budget', 'popularity', 'release_date', 'director_id',
                      'revenue', 'runtime', 'vote_average', 'vote_count', 'tagline',
                      'original_language', 'overview', 'status_id']] \
        .rename(columns={'id': 'movie_id', 'original_language': 'original_language_short'})


@task()
def write_table_to_database(df: pd.DataFrame, connection: str, table_name='str') -> None:
    """Write table to MySQL database"""
    df.to_sql(table_name,
              con=connection,
              if_exists='replace',
              index=False)
    print(f'Table {table_name} updated')
    return


@flow()
def load_to_database() -> None:
    """The main ETL function"""
    start_time = time.time()
    df_movies, df_actors = extract_zipfiles('./data/')
    df_movies = fix_datetime(df_movies)
    df_movies_genres, df_genres = create_genres_tables(df_movies)
    df_movies_directors, df_directors = create_directors_tables(df_movies)
    df_movies_production_companies, df_production_companies = create_production_companies_tables(df_movies)
    df_movies_production_countries, df_production_countries = create_production_countries_tables(df_movies)
    df_movies_crew, df_crew, df_departments, df_jobs, df_crew_jobs = create_crew_tables(df_movies)
    df_movies_spoken_languages, df_languages = create_languages_tables(df_movies)
    df_movies_statuses, df_statuses = create_statuses_tables(df_movies)
    df_movies_actors, df_actors = create_actors_tables(df_actors, df_movies)
    df_movies = create_movies_table(df_movies, df_movies_directors, df_languages, df_movies_statuses)

    load_dotenv()
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    port = os.getenv('MYSQL_PORT')
    database = os.getenv('MYSQL_DATABASE')
    connection = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(user, password, port, database)

    dataframes = ['df_movies_spoken_languages', 'df_movies_genres', 'df_movies_actors',
                  'df_movies_production_countries', 'df_movies_production_companies', 'df_movies_crew', 'df_movies',
                  'df_crew_jobs', 'df_jobs', 'df_crew', 'df_departments', 'df_directors', 'df_production_countries',
                  'df_production_companies', 'df_statuses', 'df_languages', 'df_genres', 'df_actors']

    tables = ['movies_spoken_languages', 'movies_genres', 'movies_actors', 'movies_production_countries',
              'movies_production_companies', 'movies_crew', 'movies', 'crew_jobs', 'jobs', 'crew', 'departments',
              'directors', 'production_countries', 'production_companies', 'statuses', 'languages', 'genres', 'actors']

    for d, t in zip(dataframes, tables):
        write_table_to_database(d, connection, t)
    print(f'Data has been loaded. Elapsed time: {round(time.time() - start_time, 2)} sec')


if __name__ == '__main__':
    load_to_database()
