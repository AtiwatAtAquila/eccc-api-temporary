from pydantic_settings import BaseSettings
import yaml
import logging


class Settings(BaseSettings):
    DATABASE_URL: str
    SYNC_DATABASE_URL: str  # sync version for Alembic
    EGAT_API_USER: str
    EGAT_API_PWD: str
    TSO_API_USER: str
    TSO_API_PWD: str
    LMPT2_API_KEY: str
    LOG_LEVEL: str = 'INFO'

    class Config:
        env_file = ".env"


settings = Settings()


def setup_logging(default_path='log_conf.yaml'):
    log_level = settings.LOG_LEVEL.upper()

    with open(default_path, 'r') as f:
        config = yaml.safe_load(f)

    # Apply log level override only to 'app' logger
    if 'loggers' in config and 'app' in config['loggers']:
        config['loggers']['app']['level'] = log_level

    logging.config.dictConfig(config)
