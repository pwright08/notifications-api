"""

Revision ID: 0123_add_noti_to_sms_sender
Revises: 0122_add_service_letter_contact
Create Date: 2017-09-26 16:17:58.575158

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0123_add_noti_to_sms_sender'
down_revision = '0122_add_service_letter_contact'


def upgrade():
    op.create_table('notification_to_sms_sender',
    sa.Column('notification_id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('service_sms_sender_id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.ForeignKeyConstraint(['notification_id'], ['notifications.id'], ),
    sa.ForeignKeyConstraint(['service_sms_sender_id'], ['service_sms_senders.id'], ),
    sa.PrimaryKeyConstraint('notification_id', 'service_sms_sender_id')
    )
    op.create_index(op.f('ix_notification_to_sms_sender_notification_id'), 'notification_to_sms_sender', ['notification_id'], unique=False)
    op.create_index(op.f('ix_notification_to_sms_sender_service_sms_sender_id'), 'notification_to_sms_sender', ['service_sms_sender_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_notification_to_sms_sender_service_sms_sender_id'), table_name='notification_to_sms_sender')
    op.drop_index(op.f('ix_notification_to_sms_sender_notification_id'), table_name='notification_to_sms_sender')
    op.drop_table('notification_to_sms_sender')
