"""

Revision ID: 0214_returned_mail_status
Revises: 0213_brand_colour_domain
Create Date: 2018-08-21 14:44:04.203480

"""
from alembic import op


revision = '0214_returned_mail_status'
down_revision = '0213_brand_colour_domain'


def upgrade():
    op.execute("INSERT INTO notification_status_types (name) VALUES ('returned-mail')")


def downgrade():
    op.execute("DELETE FROM notification_status_types WHERE name='returned-mail'")
