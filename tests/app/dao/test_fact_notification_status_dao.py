from datetime import date, datetime, timedelta
from uuid import UUID

from app.dao.fact_notification_status_dao import (
    fetch_data_to_populate_ft_notification_status_day,
    update_fact_notification_status_for_day,
)
from app.models import FactNotificationStatus
from tests.app.db import create_job, create_notification, create_template


def test_fetch_data_to_populate_ft_notification_status_day_when_no_notifications(notify_db_session):
    process_day = datetime(2018, 1, 15)

    assert fetch_data_to_populate_ft_notification_status_day(process_day) == []


def test_fetch_data_to_populate_ft_notification_status_day_returns_correct_fields(
    notify_db_session,
    sample_service,
):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)
    job = create_job(template=template)
    create_notification(template=template, job=job, created_at=process_day)

    results = fetch_data_to_populate_ft_notification_status_day(process_day)

    assert len(results) == 1
    assert results[0].template_id == template.id
    assert results[0].service_id == sample_service.id
    assert results[0].job_id == job.id
    assert results[0].notification_type == 'sms'
    assert results[0].key_type == 'normal'
    assert results[0].notification_status == 'created'
    assert results[0].notification_count == 1


def test_fetch_data_to_populate_ft_notification_status_day_aggregates_similar_results(
    notify_db_session,
    sample_service,
):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)

    for i in range(5):
        create_notification(template=template, created_at=process_day)

    results = fetch_data_to_populate_ft_notification_status_day(process_day)

    assert len(results) == 1
    assert results[0].notification_count == 5


def test_fetch_data_to_populate_ft_notification_status_day_gives_a_blank_uuid_for_notifications_with_no_job(
    notify_db_session,
    sample_service,
):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)
    create_notification(template=template, created_at=process_day)

    results = fetch_data_to_populate_ft_notification_status_day(process_day)

    assert results[0].job_id == UUID('00000000-0000-0000-0000-000000000000')


def test_fetch_data_to_populate_ft_notification_status_day_only_returns_data_for_given_day(
    notify_db_session,
    sample_service,
):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)
    create_notification(template=template, created_at=process_day - timedelta(days=1))
    create_notification(template=template, created_at=process_day)
    create_notification(template=template, created_at=process_day + timedelta(days=1))

    results = fetch_data_to_populate_ft_notification_status_day(process_day)

    assert len(results) == 1
    assert results[0].notification_count == 1


def test_fetch_data_to_populate_ft_notification_status_day_returns_results_for_bst(
    notify_db_session,
    sample_service,
):
    template = create_template(service=sample_service)
    create_notification(template=template, status='created', created_at=datetime(2017, 7, 29, 23, 59))
    create_notification(template=template, status='sending', created_at=datetime(2017, 7, 30, 23, 59))

    results = fetch_data_to_populate_ft_notification_status_day(datetime(2017, 7, 30))

    assert len(results) == 1
    assert results[0].notification_status == 'created'


def test_update_fact_notification_status_for_day_inserts_correct_fields(notify_db_session, sample_service):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)
    notification = create_notification(template=template, created_at=process_day)

    data_to_insert = fetch_data_to_populate_ft_notification_status_day(process_day)

    update_fact_notification_status_for_day(data_to_insert[0], process_day)

    result = FactNotificationStatus.query.one()

    assert result.bst_date == date(2018, 1, 15)
    assert result.template_id == template.id
    assert result.service_id == sample_service.id
    assert result.job_id == UUID('00000000-0000-0000-0000-000000000000')
    assert result.notification_type == template.template_type
    assert result.key_type == notification.key_type
    assert result.notification_status == notification.status
    assert result.notification_count == 1
    assert result.created_at
    assert not result.updated_at


def test_update_fact_notification_status_for_day_updates_row_when_notification_count_unchanged(
    notify_db_session,
    sample_service,
):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)
    create_notification(template=template, created_at=process_day)

    data_to_insert = fetch_data_to_populate_ft_notification_status_day(process_day)[0]

    for i in range(2):
        update_fact_notification_status_for_day(data_to_insert, process_day)

    result = FactNotificationStatus.query.one()

    assert result.updated_at


def test_update_fact_notification_status_for_day_updates_row_when_notification_count_is_changed(
    notify_db_session,
    sample_service,
):
    process_day = datetime(2018, 1, 15)
    template = create_template(service=sample_service)

    # create 1 ft_notification_status row with a notification_count of 1
    create_notification(template=template, created_at=process_day)
    data_to_insert = fetch_data_to_populate_ft_notification_status_day(process_day)[0]
    update_fact_notification_status_for_day(data_to_insert, process_day)

    result = FactNotificationStatus.query.one()
    assert result.notification_count == 1

    # update that row to have a different notification_count
    for i in range(9):
        create_notification(template=template, created_at=process_day)

    data_to_insert = fetch_data_to_populate_ft_notification_status_day(process_day)[0]
    update_fact_notification_status_for_day(data_to_insert, process_day)

    result = FactNotificationStatus.query.one()
    assert result.notification_count == 10
