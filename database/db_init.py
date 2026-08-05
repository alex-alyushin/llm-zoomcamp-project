import os

from database.database_connect import database_connect
from database.database_queries import init_messages

if __name__ == "__main__":

    from dotenv import load_dotenv
    load_dotenv()

    get_connection = lambda: database_connect(
        host=os.getenv("POSTGRES_HOST"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    init_messages(conn=get_connection())
    print("DB inited")
