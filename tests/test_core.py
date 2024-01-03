"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import shutil
import typing as t
import uuid
from pathlib import Path

import pytest
from singer_sdk.testing import get_target_test_class

from target_parquet.target import TargetParquet

TEST_OUTPUT_DIR = Path(f".output/test_{uuid.uuid4()}/")
SAMPLE_CONFIG: dict[str, t.Any] = {
    'destination_path': str(TEST_OUTPUT_DIR),
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetParquet,
    config=SAMPLE_CONFIG,
)


class TestTargetParquet(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def test_output_dir(self):
        return TEST_OUTPUT_DIR

    @pytest.fixture(scope="class")
    def resource(self, test_output_dir):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        test_output_dir.mkdir(parents=True, exist_ok=True)
        yield test_output_dir
        shutil.rmtree(test_output_dir)


# TODO: Create additional tests as appropriate for your target.
