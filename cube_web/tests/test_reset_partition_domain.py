import pytest

from scripts.reset_partition_domain import _bootstrap_scheduling, build_reset_plan, validate_reset_guards


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


class FakeConnection:
    current_database = "cube_dev"

    def __init__(self):
        self.inventory = [("r", "public", None, "partition_datasets")]

    def execute(self, query):
        if "pg_class" in query:
            return _Result(self.inventory)
        return _Result([])


def test_reset_requires_all_three_guards() -> None:
    connection = FakeConnection()
    with pytest.raises(RuntimeError, match="CUBE_WEB_ENV=development"):
        validate_reset_guards(connection, "cube_dev", True, "production")
    with pytest.raises(RuntimeError, match="dangerously-reset-partition-domain"):
        validate_reset_guards(connection, "cube_dev", False, "development")
    with pytest.raises(RuntimeError, match="actual database"):
        validate_reset_guards(connection, "wrong", True, "development")


def test_reset_order_and_unknown_object_refusal() -> None:
    connection = FakeConnection()
    plan = build_reset_plan(connection)
    drops = "\n".join(plan.drop_statements)
    assert drops.index("partition_domain_outbox") < drops.index("partition_datasets") < drops.index("partition_job_attempts")
    connection.inventory.append(("v", "public", None, "partition_old_view"))
    with pytest.raises(RuntimeError, match="partition_old_view"):
        build_reset_plan(connection)


def test_bootstrap_recreates_scheduling_before_domain_tables() -> None:
    class Cursor:
        def __init__(self, executed):
            self.executed = executed

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, statement):
            self.executed.append(statement)

    class Connection:
        def __init__(self):
            self.executed = []

        def cursor(self):
            return Cursor(self.executed)

    connection = Connection()
    _bootstrap_scheduling(connection)
    assert "CREATE TABLE partition_batches" in connection.executed[0]
    assert "CREATE TABLE partition_job_attempts" in connection.executed[2]
