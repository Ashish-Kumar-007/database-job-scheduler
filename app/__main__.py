from app.config import Settings
from app.db.engine import check_database_connection, create_engine


def main() -> None:
    engine = create_engine(Settings.from_env())
    try:
        check_database_connection(engine)
    finally:
        engine.dispose()
    print("Database connection healthy.")


if __name__ == "__main__":
    main()
