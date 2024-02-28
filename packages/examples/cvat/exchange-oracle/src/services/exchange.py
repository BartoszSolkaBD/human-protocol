from datetime import timedelta
from typing import Optional

import src.cvat.api_calls as cvat_api
import src.models.cvat as models
import src.services.cvat as cvat_service
from src.chain.escrow import get_escrow_manifest
from src.core.types import JobStatuses, Networks, ProjectStatuses
from src.db import SessionLocal
from src.utils.assignments import parse_manifest
from src.utils.requests import get_or_404
from src.utils.time import utcnow


class UserHasUnfinishedAssignmentError(Exception):
    pass


def create_assignment(
    escrow_address: str, chain_id: Networks, wallet_address: str
) -> Optional[str]:
    with SessionLocal.begin() as session:
        user = get_or_404(
            cvat_service.get_user_by_id(session, wallet_address, for_update=True),
            wallet_address,
            "user",
        )

        # There can be several projects with under one escrow, we need any
        project = cvat_service.get_project_by_escrow_address(
            session,
            escrow_address,
            status_in=[
                ProjectStatuses.annotation
            ],  # avoid unnecessary locking on completed projects
            for_update=True,
        )

        if not project:
            # Retry without a lock to check if the project doesn't exist
            get_or_404(
                cvat_service.get_project_by_escrow_address(
                    session, escrow_address, status_in=[ProjectStatuses.annotation]
                ),
                escrow_address,
                "job",
            )
            return None

        manifest = parse_manifest(get_escrow_manifest(project.chain_id, project.escrow_address))

        unassigned_job: Optional[models.Job] = None
        unfinished_assignments: list[models.Assignment] = []
        for job in project.jobs:
            job_assignment = job.latest_assignment
            if job_assignment and not job_assignment.is_finished:
                unfinished_assignments.append(job_assignment)

            if (
                not unassigned_job
                and job.status == JobStatuses.new
                and (not job_assignment or job_assignment.is_finished)
            ):
                unassigned_job = job

        now = utcnow()
        unfinished_user_assignments = [
            assignment
            for assignment in unfinished_assignments
            if assignment.user_wallet_address == wallet_address and now < assignment.expires_at
        ]
        if unfinished_user_assignments:
            raise UserHasUnfinishedAssignmentError(
                "The user already has an unfinished assignment in this project"
            )

        if not unassigned_job:
            return None

        assignment_id = cvat_service.create_assignment(
            session,
            wallet_address=user.wallet_address,
            cvat_job_id=unassigned_job.cvat_id,
            expires_at=now + timedelta(seconds=manifest.annotation.max_time),
        )

        cvat_api.clear_job_annotations(unassigned_job.cvat_id)
        cvat_api.restart_job(unassigned_job.cvat_id)
        cvat_api.update_job_assignee(unassigned_job.cvat_id, assignee_id=user.cvat_id)
        # rollback is automatic within the transaction

    return assignment_id
