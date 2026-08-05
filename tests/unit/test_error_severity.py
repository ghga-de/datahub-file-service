# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for picking the error to surface when concurrent parts or files fail.

Parts run inside nested TaskGroups (one per file, one per part for the overlapped
verify/upload pair), so failures can reach the handler wrapped several layers deep.
Severity has to survive that nesting: a CriticalError stops the whole batch and a
ConclusiveError fails the file outright, so neither may be masked by an
InconclusiveError that would merely schedule a retry.
"""

from unittest.mock import MagicMock

import pytest

from dhfs.core.interrogator import Interrogator, _flatten_exception_group
from tests.fixtures.config import get_config
from tests.fixtures.utils import DHFS_CRYPT4GH_PRIVATE_KEY_PATH

CRITICAL = Interrogator.CriticalError("critical")
CONCLUSIVE = Interrogator.DecryptionError()
INCONCLUSIVE = Interrogator.InconclusiveError("inconclusive")


@pytest.fixture(name="interrogator")
def interrogator_fixture() -> Interrogator:
    """An Interrogator with stubbed-out collaborators."""
    config = get_config(
        data_hub_crypt4gh_private_key_path=DHFS_CRYPT4GH_PRIVATE_KEY_PATH
    )
    return Interrogator(
        config=config, central_client=MagicMock(), s3_client=MagicMock()
    )


def test_flatten_nested_groups():
    """Leaves are collected from arbitrarily deep ExceptionGroups."""
    group = ExceptionGroup(
        "outer",
        [
            ExceptionGroup("inner", [INCONCLUSIVE, CRITICAL]),
            ExceptionGroup("also inner", [ExceptionGroup("deep", [CONCLUSIVE])]),
        ],
    )
    assert _flatten_exception_group(group) == [INCONCLUSIVE, CRITICAL, CONCLUSIVE]


@pytest.mark.parametrize(
    "errors, expected",
    [
        ([INCONCLUSIVE, CRITICAL], CRITICAL),
        ([INCONCLUSIVE, CONCLUSIVE], CONCLUSIVE),
        ([CONCLUSIVE, CRITICAL], CRITICAL),
        ([INCONCLUSIVE, CONCLUSIVE, CRITICAL], CRITICAL),
        ([INCONCLUSIVE], INCONCLUSIVE),
    ],
    ids=[
        "critical beats inconclusive",
        "conclusive beats inconclusive",
        "critical beats conclusive",
        "critical beats both",
        "lone inconclusive survives",
    ],
)
def test_severity_wins_regardless_of_position(interrogator, errors, expected):
    """The most severe error is surfaced no matter what order the parts failed in."""
    for ordering in (errors, list(reversed(errors))):
        group = ExceptionGroup("parts failed", ordering)
        assert interrogator._most_significant_error(group) is expected


def test_severity_survives_the_nested_part_group(interrogator):
    """A CriticalError still wins when it is raised a level deeper than its rival.

    This is the shape the overlapped verify/upload pair produces: the per-part group
    is nested inside the per-file group.
    """
    group = ExceptionGroup(
        "file",
        [
            INCONCLUSIVE,
            ExceptionGroup("part", [CRITICAL]),
        ],
    )
    assert interrogator._most_significant_error(group) is CRITICAL
