import sqlalchemy as sa

from dags.extracao_suprimentos_mv.base import BasePostgres


class MotCancel(BasePostgres):
    __tablename__ = 'mot_cancel'

    _id_seq = sa.Sequence('id_mot_cancel_seq', start=1, increment=1)

    id = sa.Column(sa.BigInteger, _id_seq, server_default=_id_seq.next_value(), primary_key=True)
    cd_mot_cancel = sa.Column('CD_MOT_CANCEL', sa.String())
    ds_mot_cancel = sa.Column('DS_MOT_CANCEL', sa.String())
    tp_mot_cancel = sa.Column('TP_MOT_CANCEL', sa.String())
    dt_extracao = sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.func.now())
