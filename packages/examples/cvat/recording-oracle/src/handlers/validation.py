import io
import os
from logging import Logger
from typing import Dict, Optional

from sqlalchemy.orm import Session

import src.chain.escrow as escrow
import src.core.annotation_meta as annotation
import src.core.validation_meta as validation
import src.services.webhook as oracle_db_service
from src.core.config import Config
from src.core.manifest import TaskManifest
from src.core.oracle_events import (
    RecordingOracleEvent_JobCompleted,
    RecordingOracleEvent_SubmissionRejected,
)
from src.core.storage import (
    compose_results_bucket_filename as compose_annotation_results_bucket_filename,
)
from src.core.types import OracleWebhookTypes
from src.handlers.process_intermediate_results import (
    ValidationFailure,
    ValidationResult,
    ValidationSuccess,
    parse_annotation_metafile,
    process_intermediate_results,
    serialize_validation_meta,
)
from src.log import ROOT_LOGGER_NAME
from src.services.cloud import make_client as make_cloud_client
from src.services.cloud import s3
from src.services.cloud.utils import BucketAccessInfo
from src.utils.assignments import compute_resulting_annotations_hash, parse_manifest
from src.utils.logging import NullLogger, get_function_logger

module_logger_name = f"{ROOT_LOGGER_NAME}.cron.webhook"


class _TaskValidator:
    def __init__(
        self, escrow_address: str, chain_id: int, manifest: TaskManifest, db_session: Session
    ) -> None:
        self.escrow_address = escrow_address
        self.chain_id = chain_id
        self.manifest = manifest
        self.db_session = db_session
        self.logger: Logger = NullLogger()

        self.data_bucket = BucketAccessInfo.from_raw_url(
            Config.exchange_oracle_storage_config.bucket_url()
        )

        self.annotation_meta: Optional[annotation.AnnotationMeta] = None
        self.job_annotations: Optional[Dict[int, bytes]] = None
        self.merged_annotations: Optional[bytes] = None
        self.gt_data: Optional[bytes] = None

    def set_logger(self, logger: Logger):
        self.logger = logger

    def _download_results_meta(self):
        data_bucket_client = make_cloud_client(self.data_bucket)

        annotation_meta_path = compose_annotation_results_bucket_filename(
            self.escrow_address,
            self.chain_id,
            annotation.ANNOTATION_RESULTS_METAFILE_NAME,
        )
        annotation_metafile_data = data_bucket_client.download_file(
            self.data_bucket.url.bucket_name, annotation_meta_path
        )
        self.annotation_meta = parse_annotation_metafile(io.BytesIO(annotation_metafile_data))

    def _download_annotations(self):
        assert self.annotation_meta is not None

        data_bucket_client = make_cloud_client(self.data_bucket)

        job_annotations = {}
        for job_meta in self.annotation_meta.jobs:
            job_filename = compose_annotation_results_bucket_filename(
                self.escrow_address,
                self.chain_id,
                job_meta.annotation_filename,
            )
            job_annotations[job_meta.job_id] = data_bucket_client.download_file(
                self.data_bucket.url.bucket_name, job_filename
            )

        excor_merged_annotation_path = compose_annotation_results_bucket_filename(
            self.escrow_address,
            self.chain_id,
            annotation.RESULTING_ANNOTATIONS_FILE,
        )
        merged_annotations = data_bucket_client.download_file(
            self.data_bucket.url.bucket_name, excor_merged_annotation_path
        )

        self.job_annotations = job_annotations
        self.merged_annotations = merged_annotations

    def _download_gt(self):
        gt_bucket = BucketAccessInfo.from_raw_url(self.manifest.validation.gt_url)
        gt_bucket_client = make_cloud_client(gt_bucket)
        self.gt_data = gt_bucket_client.download_file(gt_bucket.url.bucket_name, gt_bucket.url.path)

    def _download_results(self):
        self._download_results_meta()
        self._download_annotations()
        self._download_gt()

    def _process_annotation_results(self) -> ValidationResult:
        assert self.annotation_meta is not None
        assert self.job_annotations is not None
        assert self.merged_annotations is not None
        assert self.gt_data is not None

        # TODO: refactor further
        return process_intermediate_results(
            session=self.db_session,
            escrow_address=self.escrow_address,
            chain_id=self.chain_id,
            meta=self.annotation_meta,
            job_annotations={k: io.BytesIO(v) for k, v in self.job_annotations.items()},
            merged_annotations=io.BytesIO(self.merged_annotations),
            gt_annotations=io.BytesIO(self.gt_data),
            manifest=self.manifest,
            logger=self.logger,
        )

    def validate(self):
        self._download_results()

        validation_result = self._process_annotation_results()

        self._handle_validation_result(validation_result)

    def _compose_validation_results_bucket_filename(self, filename: str) -> str:
        return f"{self.escrow_address}@{self.chain_id}/{filename}"

    _LOW_QUALITY_REASON_MESSAGE_TEMPLATE = (
        "Annotation quality ({}) is below the required threshold ({})"
    )

    def _handle_validation_result(self, validation_result: ValidationResult):
        logger = self.logger
        escrow_address = self.escrow_address
        chain_id = self.chain_id
        db_session = self.db_session

        if isinstance(validation_result, ValidationSuccess):
            logger.info(
                f"Validation for escrow_address={escrow_address} successful, "
                f"average annotation quality is {validation_result.average_quality:.2f}"
            )

            recor_merged_annotations_path = self._compose_validation_results_bucket_filename(
                validation.RESULTING_ANNOTATIONS_FILE,
            )

            recor_validation_meta_path = self._compose_validation_results_bucket_filename(
                validation.VALIDATION_METAFILE_NAME,
            )
            validation_metafile = serialize_validation_meta(validation_result.validation_meta)

            storage_client = s3.S3Client(
                Config.storage_config.provider_endpoint_url(),
                access_key=Config.storage_config.access_key,
                secret_key=Config.storage_config.secret_key,
            )

            # TODO: add encryption
            storage_client.create_file(
                Config.storage_config.data_bucket_name,
                recor_merged_annotations_path,
                validation_result.resulting_annotations,
            )
            storage_client.create_file(
                Config.storage_config.data_bucket_name,
                recor_validation_meta_path,
                validation_metafile,
            )

            escrow.store_results(
                chain_id,
                escrow_address,
                Config.storage_config.bucket_url() + os.path.dirname(recor_merged_annotations_path),
                compute_resulting_annotations_hash(validation_result.resulting_annotations),
            )

            oracle_db_service.outbox.create_webhook(
                db_session,
                escrow_address,
                chain_id,
                OracleWebhookTypes.reputation_oracle,
                event=RecordingOracleEvent_JobCompleted(),
            )
            oracle_db_service.outbox.create_webhook(
                db_session,
                escrow_address,
                chain_id,
                OracleWebhookTypes.exchange_oracle,
                event=RecordingOracleEvent_JobCompleted(),
            )
        elif isinstance(validation_result, ValidationFailure):
            logger.info(
                f"Validation for escrow_address={escrow_address} failed, "
                f"rejected {len(validation_result.rejected_jobs)} jobs"
            )

            job_id_to_assignment_id = {
                job_meta.job_id: job_meta.assignment_id for job_meta in self.annotation_meta.jobs
            }

            oracle_db_service.outbox.create_webhook(
                db_session,
                escrow_address,
                chain_id,
                OracleWebhookTypes.exchange_oracle,
                event=RecordingOracleEvent_SubmissionRejected(
                    rejected_tasks=[
                        RecordingOracleEvent_SubmissionRejected.RejectedTaskInfo(
                            task_id=job_id_to_assignment_id[rejected_job_id],
                            reason=self._LOW_QUALITY_REASON_MESSAGE_TEMPLATE.format(
                                validation_result.job_results[rejected_job_id],
                                self.manifest.validation.min_quality,
                            ),
                        )
                        for rejected_job_id in validation_result.rejected_jobs
                    ]
                ),
            )
        else:
            assert False


def validate_results(
    escrow_address: str,
    chain_id: int,
    db_session: Session,
):
    logger = get_function_logger(module_logger_name)

    escrow.validate_escrow(chain_id=chain_id, escrow_address=escrow_address)

    manifest = parse_manifest(escrow.get_escrow_manifest(chain_id, escrow_address))

    validator = _TaskValidator(
        escrow_address=escrow_address, chain_id=chain_id, manifest=manifest, db_session=db_session
    )
    validator.set_logger(logger)
    validator.validate()