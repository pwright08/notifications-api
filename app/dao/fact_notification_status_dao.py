from datetime import datetime, timedelta, time

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert

from app import db
from app.dao.dao_utils import transactional
from app.models import FactNotificationStatus, Notification, NotificationHistory
from app.utils import convert_bst_to_utc


def fetch_data_to_populate_ft_notification_status_day(process_day):
    '''
    This gets the data from Notification / NotificationHistory that will be used to populate a row in the
    ft_notification_status table
    '''
    start_date = convert_bst_to_utc(datetime.combine(process_day, time.min))
    end_date = convert_bst_to_utc(datetime.combine(process_day + timedelta(days=1), time.min))

    table = Notification if start_date >= datetime.utcnow() - timedelta(days=7) else NotificationHistory

    return db.session.query(
        table.template_id,
        table.service_id,
        func.coalesce(table.job_id, '00000000-0000-0000-0000-000000000000').label('job_id'),
        table.notification_type,
        table.key_type,
        table.status.label('notification_status'),
        func.count().label('notification_count'),
    ).filter(
        table.created_at >= start_date,
        table.created_at < end_date
    ).group_by(
        table.template_id,
        table.service_id,
        'job_id',
        table.notification_type,
        table.key_type,
        table.status,
    ).all()


@transactional
def update_fact_notification_status_for_day(data, process_day):
    '''
    This uses the Postgres upsert to avoid race conditions when two threads try and insert
    at the same row. The excluded object refers to values that we tried to insert but were
    rejected.
    http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#insert-on-conflict-upsert
    '''
    table = FactNotificationStatus.__table__
    stmt = insert(table).values(
        bst_date=process_day,
        template_id=data.template_id,
        service_id=data.service_id,
        job_id=data.job_id,
        notification_type=data.notification_type,
        key_type=data.key_type,
        notification_status=data.notification_status,
        notification_count=data.notification_count
    )
    stmt = stmt.on_conflict_do_update(
        constraint="ft_notification_status_pkey",
        set_={
            'notification_count': stmt.excluded.notification_count,
            'updated_at': datetime.utcnow()
        }
    )
    db.session.connection().execute(stmt)
