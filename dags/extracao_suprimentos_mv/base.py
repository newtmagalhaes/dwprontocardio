import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, sessionmaker

SCHEMA_POSTGRES = 'raw_mv'

BasePostgres = declarative_base(
    metadata=sa.MetaData(schema=SCHEMA_POSTGRES)
)

Session = sessionmaker()
