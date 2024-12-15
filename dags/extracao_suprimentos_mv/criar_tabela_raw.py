import sqlalchemy as sa

from dags.extracao_suprimentos_mv import models

from .base import BasePostgres
from .hooks import postgres_hook


def criar_tabela_raw():
    TABELAS_POSTGRES = [
        models.UniPro,
        models.MotCancel,
    ]

    engine = sa.create_engine(postgres_hook.get_uri())
    BasePostgres.metadata.create_all(engine, TABELAS_POSTGRES)
