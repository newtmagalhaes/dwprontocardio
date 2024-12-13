import sqlalchemy as sa

from dags.base import BasePostgres


class UniPro(BasePostgres):
    __tablename__ = 'uni_pro'

    _id_seq = sa.Sequence('id_uni_pro_seq', start=1, increment=1)

    id = sa.Column(sa.BigInteger, _id_seq, server_default=_id_seq.next_value(), primary_key=True),
    cd_uni_pro = sa.Column('CD_UNI_PRO', sa.String()),
    cd_unidade = sa.Column('CD_UNIDADE', sa.String()),
    cd_produto = sa.Column('CD_PRODUTO', sa.String()),
    vl_fator = sa.Column('VL_FATOR', sa.String()),
    tp_relatorios = sa.Column('TP_RELATORIOS', sa.String()),
    sn_ativo = sa.Column('SN_ATIVO', sa.String()),
    dt_extracao = sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
