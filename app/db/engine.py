from sqlalchemy import Engine, create_engine as sqlalchemy_create_engine, text

from app.config import Settings


def create_engine(settings: Settings) -> Engine:
    return sqlalchemy_create_engine(
        settings.database_url,
        pool_pre_ping=True,
    )


def check_database_connection(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1")).scalar_one()
