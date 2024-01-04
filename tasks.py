"""This file contains the tasks that will be executed by celery."""
from celery_app import app
from sync.database_breach import DatabaseBridge
from sync.facilities import SyncFacilities
from sync.modalities import SyncModalities
from sync.practitioners import SyncPractitioners
from sync.studies import SyncStudies
from sync.sync_base import OrganizationData
from sync.sync_validator import SyncValidator
from utils import get_schema_name


@app.task
def sync_data_from_by_organization(
    organization_id: str, organization_slug: str
) -> None:
    """Sync data from source to destination database by organization.

    :param organization_id:
    :param organization_slug:
    :return: None
    """
    organization_data = OrganizationData(
        organization_id,
        get_schema_name(organization_slug),
    )
    bridge = DatabaseBridge()
    sync_facilities = SyncFacilities(organization_data, bridge)
    sync_facilities.retrieve_data()

    sync_modalities = SyncModalities(organization_data, bridge)
    sync_modalities.retrieve_data()

    sync_practitioners = SyncPractitioners(organization_data, bridge)
    sync_practitioners.retrieve_data()

    sync_studies = SyncStudies(organization_data, bridge)
    sync_studies.retrieve_data()

    # Pending to QA
    # sync_technicians = SyncTechnicians(organization_data, bridge)
    # sync_technicians.retrieve_data()

    bridge.close_connections()


@app.task
def sync_pending_data_by_organization(
    organization_id: str, organization_slug: str
) -> None:
    """Sync pending data from source to destination database by organization.

    :param organization_id:
    :param organization_slug:
    :return: None
    """
    organization_data = OrganizationData(
        organization_id,
        get_schema_name(organization_slug),
    )
    bridge = DatabaseBridge()
    SyncValidator(organization_data, bridge).retrieve_data()
