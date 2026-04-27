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
"""Service configuration and execution"""

import logging
from asyncio import sleep
from contextlib import suppress

from hexkit.log import configure_logging
from hexkit.utils import now_utc_ms_prec

from dhfs import __version__
from dhfs.config import Config
from dhfs.core.interrogator import InterrogatorPort
from dhfs.inject import prepare_interrogation_bucket_cleaner, prepare_interrogator

log = logging.getLogger(__name__)


def _configure_logging(config: Config):
    """Silence log messages from libraries and set up structured logging.

    This will silence logs from blacklisted libraries (unless 'critical') when the
    log level is configured to anything above DEBUG. When log level is DEBUG for DHFS,
    library logs with level INFO or higher will be emitted. DEBUG-level library logs
    will still be suppressed to avoid too much noise.
    """
    configure_logging(config=config)
    for logger in config.library_logger_names:
        logging.getLogger(logger).setLevel(config.library_log_level)


async def run_interrogator(forever: bool = True):
    """Run the file interrogation and re-encryption process."""
    config = Config()  # type: ignore
    _configure_logging(config=config)
    log.info("DHFS version %s starting.", __version__)
    async with prepare_interrogator(config=config) as interrogator:
        if forever:
            while True:
                try:
                    start = now_utc_ms_prec()
                    with suppress(
                        InterrogatorPort.CantCompleteError,
                        InterrogatorPort.InterrogationError,
                    ):
                        await interrogator.interrogate_new_files()
                except Exception:
                    log.warning(
                        "An unhandled exception caused the current round of file"
                        + " processing to fail. If this keeps occurring, run"
                        + " with log_level set to DEBUG.",
                    )
                    if config.log_level == "DEBUG":
                        raise
                finally:
                    stop = now_utc_ms_prec()
                    if (
                        timediff := (stop - start).seconds
                    ) < config.min_run_interval_seconds:
                        sleep_duration = config.min_run_interval_seconds - timediff
                        log.info(
                            "Waiting %i seconds before beginning the next round of file"
                            + " processing because the minimum run interval is set to"
                            + " %i seconds.",
                            sleep_duration,
                            config.min_run_interval_seconds,
                        )
                        await sleep(sleep_duration)
        else:
            await interrogator.interrogate_new_files()


async def perform_cleanup():
    """Run the S3 'interrogation' bucket cleanup routine."""
    config = Config()  # type: ignore
    _configure_logging(config=config)
    log.info("Cleanup routine starting. Current DHFS version is %s.", __version__)
    async with prepare_interrogation_bucket_cleaner(config=config) as cleaner:
        await cleaner.scan_and_clean()
