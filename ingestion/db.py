import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env
load_dotenv()

# Get the database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

# Create the engine — this is SQLAlchemy's connection to your database
engine = create_engine(DATABASE_URL)


def test_connection():
    # Use the engine to open a connection and run a query
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM assets"))
        for row in result:
            print(row)


if __name__ == "__main__":
    test_connection()