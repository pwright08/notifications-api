"""

Revision ID: 124_index_noti_to_email_reply
Revises: 0123_add_noti_to_email_reply
Create Date: 2017-09-29 12:52:15.679231

"""
from alembic import op
import sqlalchemy as sa


revision = '124_index_noti_to_email_reply'
down_revision = '0123_add_noti_to_email_reply'


def upgrade():
    op.drop_index('ix_notification_to_email_sender_notification_id', table_name='notification_to_email_reply_to')
    op.create_index(op.f('ix_notification_to_email_reply_to_notification_id'), 'notification_to_email_reply_to', ['notification_id'], unique=True)


def downgrade():
    op.drop_index(op.f('ix_notification_to_email_reply_to_notification_id'), table_name='notification_to_email_reply_to')
    op.create_index('ix_notification_to_email_sender_notification_id', 'notification_to_email_reply_to', ['notification_id'], unique=False)
