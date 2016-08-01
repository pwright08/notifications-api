"""empty message

Revision ID: 0043_jobs_to_notification_hist
Revises: 0042_notification_history
Create Date: 2016-07-15 13:28:41.441009

"""

# revision identifiers, used by Alembic.
revision = '0043_jobs_to_notification_hist'
down_revision = '0042_notification_history'

from alembic import op

from sqlalchemy.orm.session import Session

import uuid
import datetime
from app.models import Job, Template, NotificationHistory


def upgrade():
    session = Session(bind=op.get_bind())

    go_live = datetime.datetime.strptime('2016-05-18', '%Y-%m-%d')
    notifications_history_start_date = datetime.datetime.strptime('2016-06-26 23:21:55', '%Y-%m-%d %H:%M:%S')
    jobs = session.query(Job).join(Template).filter(Job.service_id == '95316ff0-e555-462d-a6e7-95d26fbfd091',
                                                    Job.created_at >= go_live,
                                                    Job.created_at < notifications_history_start_date).all()

    for job in jobs:
        for i in range(0, job.notifications_delivered):
            notification = NotificationHistory(id=uuid.uuid4(),
                                               job_id=job.id,
                                               service_id=job.service_id,
                                               template_id=job.template.id,
                                               template_version=job.template_version,
                                               key_type='normal',
                                               content_char_count=len(job.template.content),
                                               notification_type=job.template.template_type,
                                               created_at=job.created_at,
                                               sent_at=job.processing_finished,
                                               sent_by='ses' if job.template.template_type == 'email' else 'mmg',
                                               status='delivered')

            session.add(notification)

        for i in range(0, job.notifications_failed):
            notification = NotificationHistory(id=uuid.uuid4(),
                                               job_id=job.id,
                                               service_id=job.service_id,
                                               template_id=job.template.id,
                                               template_version=job.template_version,
                                               key_type='normal',
                                               content_char_count=len(job.template.content),
                                               notification_type=job.template.template_type,
                                               created_at=job.created_at,
                                               sent_at=job.processing_finished,
                                               sent_by='ses' if job.template.template_type == 'email' else 'mmg',
                                               status='permanent-failure')
            session.add(notification)
        session.commit()


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    session = Session(bind=op.get_bind())

    go_live = datetime.datetime.strptime('2016-05-18', '%Y-%m-%d')
    notifications_history_start_date = datetime.datetime.strptime('2016-06-26 23:21:55', '%Y-%m-%d %H:%M:%S')

    session.query(NotificationHistory).filter(
        NotificationHistory.created_at >= go_live,
        NotificationHistory.service_id == '95316ff0-e555-462d-a6e7-95d26fbfd091',
        NotificationHistory.created_at < notifications_history_start_date).delete()

    session.commit()
    ### end Alembic commands ###
