from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from psycopg.rows import dict_row

from cube_web.routes.auth import Actor, require_admin
from cube_web.services.quality_contracts import Publication, WarnApproval
from cube_web.services.quality_repository import (
    OutputVersionNotFound,
    lock_dataset,
    lock_quality_run,
    require_open_gauss_domain_store,
)


class WarnApprovalRejected(RuntimeError):
    pass


class WarnApprovalConflict(RuntimeError):
    pass


class PublicationPolicyRejected(RuntimeError):
    pass


class PublicationWithdrawalConflict(RuntimeError):
    pass


class PublicationNotFound(LookupError):
    pass


@dataclass(frozen=True)
class PublishRequest:
    output_version: str | None = None
    quality_run_id: UUID | None = None
    targets: tuple[dict[str, str], ...] = ()


def _approval(row: dict) -> WarnApproval:
    return WarnApproval(**row)


def _publication(row: dict) -> Publication:
    return Publication(
        publication_id=row["publication_id"],
        dataset_id=row["dataset_id"],
        output_version=row["output_version"],
        quality_run_id=row["quality_run_id"],
        status=row["status"],
        service_version_id=row.get("service_version_id"),
        requested_by=row["requested_by"],
        requested_at=row["requested_at"],
        activated_at=row.get("activated_at"),
        failure=row.get("failure"),
        withdrawn_by=row.get("withdrawn_by"),
        withdrawn_at=row.get("withdrawn_at"),
        withdrawal_reason=row.get("withdrawal_reason"),
    )


def approve_warn(dataset_id: str, quality_run_id: UUID, reason: str, actor: Actor) -> WarnApproval:
    require_admin(actor)
    with require_open_gauss_domain_store().transaction() as tx:
        dataset = lock_dataset(tx, dataset_id)
        run = lock_quality_run(tx, quality_run_id)
        if run["dataset_id"] != dataset_id or run["output_version"] != dataset["current_output_version"]:
            raise WarnApprovalRejected("quality run does not bind the current dataset output")
        if run["quality_run_id"] != dataset["current_quality_run_id"] or run["status"] != "warn" or not run["result_complete"]:
            raise WarnApprovalRejected("only the exact complete current Warn run can be approved")
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM partition_quality_warn_approvals WHERE quality_run_id = %s", (quality_run_id,))
            existing = cur.fetchone()
            if existing is not None:
                if (
                    existing["approved_by"] == actor.username
                    and existing["reason"] == reason
                    and existing["rule_set_version"] == run["rule_set_version"]
                ):
                    return _approval(existing)
                raise WarnApprovalConflict(str(quality_run_id))
            cur.execute(
                "INSERT INTO partition_quality_warn_approvals "
                "(approval_id, dataset_id, output_version, quality_run_id, rule_set_version, approved_by, approved_at, reason) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
                (
                    uuid4(),
                    dataset_id,
                    run["output_version"],
                    quality_run_id,
                    run["rule_set_version"],
                    actor.username,
                    datetime.now(UTC),
                    reason,
                ),
            )
            return _approval(cur.fetchone())


def publish_dataset(dataset_id: str, request: PublishRequest, actor: Actor) -> Publication:
    require_admin(actor)
    with require_open_gauss_domain_store().transaction() as tx:
        dataset = lock_dataset(tx, dataset_id)
        output_version = request.output_version or dataset["current_output_version"]
        quality_run_id = request.quality_run_id or dataset["current_quality_run_id"]
        if output_version is None or quality_run_id is None:
            raise PublicationPolicyRejected("current output and quality run are required")
        if output_version != dataset["current_output_version"]:
            raise PublicationPolicyRejected("output version is not current")
        if quality_run_id != dataset["current_quality_run_id"]:
            raise PublicationPolicyRejected("quality run is not the current quality run")
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT status FROM partition_output_versions WHERE dataset_id = %s AND output_version = %s FOR UPDATE",
                (dataset_id, output_version),
            )
            output = cur.fetchone()
            if output is None or output["status"] != "completed":
                raise OutputVersionNotFound(output_version)
        run = lock_quality_run(tx, quality_run_id)
        if run["dataset_id"] != dataset_id or run["output_version"] != output_version or not run["result_complete"]:
            raise PublicationPolicyRejected("quality run does not bind the complete current output")
        if run["status"] == "warn":
            with tx.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT rule_set_version FROM partition_quality_warn_approvals WHERE quality_run_id = %s", (quality_run_id,))
                approval = cur.fetchone()
            if approval is None or approval["rule_set_version"] != run["rule_set_version"]:
                raise PublicationPolicyRejected("current Warn quality run requires approval")
        elif run["status"] != "pass":
            raise PublicationPolicyRejected("quality run status cannot authorize publication")
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT ir.ingest_run_id, ir.status FROM ingest_runs ir "
                "JOIN partition_runs pr ON pr.partition_run_id = ir.partition_run_id "
                "WHERE ir.dataset_id = %s AND pr.status = 'completed' "
                "ORDER BY ir.created_at DESC, ir.ingest_run_id DESC LIMIT 1 FOR UPDATE",
                (dataset_id,),
            )
            ingest_run = cur.fetchone()
            if ingest_run is None or ingest_run["status"] != "completed":
                raise PublicationPolicyRejected("publication requires completed ingest")
            cur.execute(
                "SELECT count(*) AS scene_count, "
                "COALESCE(sum(CASE WHEN status <> 'completed' OR output_version <> %s THEN 1 ELSE 0 END), 0) AS invalid_count "
                "FROM ingest_run_scenes WHERE ingest_run_id = %s",
                (output_version, ingest_run["ingest_run_id"]),
            )
            ingest_scenes = cur.fetchone()
            if int(ingest_scenes["scene_count"] or 0) == 0 or int(ingest_scenes["invalid_count"] or 0) != 0:
                raise PublicationPolicyRejected("publication requires completed ingest for the current output")
            cur.execute(
                "SELECT * FROM partition_publications WHERE dataset_id = %s AND output_version = %s AND quality_run_id = %s "
                "AND status = 'active' FOR UPDATE",
                (dataset_id, output_version, quality_run_id),
            )
            existing = cur.fetchone()
            if existing is not None:
                publication_id = existing["publication_id"]
            else:
                cur.execute(
                    "INSERT INTO partition_publications "
                    "(publication_id, dataset_id, output_version, quality_run_id, status, desired_action, service_version_id, requested_by, requested_at, activated_at) "
                    "VALUES (%s, %s, %s, %s, 'active', 'activate', %s, %s, %s, %s) RETURNING *",
                    (uuid4(), dataset_id, output_version, quality_run_id, output_version, actor.username, datetime.now(UTC), datetime.now(UTC)),
                )
                existing = cur.fetchone()
                publication_id = existing["publication_id"]
            if request.targets:
                for target in request.targets:
                    source_asset_id = str(target.get("source_asset_id") or "").strip()
                    band_code = str(target.get("band_code") or "").strip()
                    if not source_asset_id or not band_code:
                        raise PublicationPolicyRejected("publication target requires source_asset_id and band_code")
                    cur.execute(
                        "SELECT 1 FROM partition_dataset_bands WHERE dataset_id=%s AND source_asset_id=%s AND band_code=%s",
                        (dataset_id, source_asset_id, band_code),
                    )
                    if cur.fetchone() is None:
                        raise PublicationPolicyRejected(f"publication target does not belong to dataset: {source_asset_id}/{band_code}")
                    cur.execute(
                        "SELECT 1 FROM partition_indexes WHERE dataset_id=%s AND output_version=%s AND source_asset_id=%s AND band_code=%s LIMIT 1",
                        (dataset_id, output_version, source_asset_id, band_code),
                    )
                    if cur.fetchone() is None:
                        raise PublicationPolicyRejected(f"publication target is not partitioned: {source_asset_id}/{band_code}")
                    cur.execute(
                        "INSERT INTO partition_publication_targets "
                        "(publication_id,dataset_id,output_version,source_asset_id,band_code) "
                        "SELECT %s,%s,%s,%s,%s WHERE NOT EXISTS ("
                        "SELECT 1 FROM partition_publication_targets WHERE publication_id=%s AND source_asset_id=%s AND band_code=%s)",
                        (publication_id, dataset_id, output_version, source_asset_id, band_code, publication_id, source_asset_id, band_code),
                    )
            if existing is not None:
                return _publication(existing)


def withdraw_publication(dataset_id: str, publication_id: UUID, reason: str, actor: Actor) -> Publication:
    with require_open_gauss_domain_store().transaction() as tx:
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM partition_publications WHERE publication_id = %s AND dataset_id = %s FOR UPDATE",
                (publication_id, dataset_id),
            )
            publication = cur.fetchone()
            if publication is None:
                raise PublicationNotFound(str(publication_id))
            if publication["status"] in {"failed"} or publication["service_version_id"] is None:
                raise PublicationWithdrawalConflict("publication cannot be withdrawn")
            if publication["status"] == "withdrawn":
                if publication.get("withdrawn_by") == actor.username and publication.get("withdrawal_reason") == reason:
                    return _publication(publication)
                raise PublicationWithdrawalConflict(str(publication_id))
            cur.execute(
                "UPDATE partition_publications SET status = 'withdrawn', desired_action = 'withdraw', withdrawn_by = %s, "
                "withdrawn_at = now(), withdrawal_reason = %s, updated_at = now() "
                "WHERE publication_id = %s RETURNING *",
                (actor.username, reason, publication_id),
            )
            return _publication(cur.fetchone())
