#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

# Standard Library Imports
from datetime import datetime
import logging
from typing import List

# Third-Party Imports
# Application Imports
from cdp_backend.pipeline.ingestion_models import EventIngestionModel
from cdp_scrapers.legistar_utils import LegistarScraper

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


def get_events(
    from_dt: datetime,
    to_dt: datetime,
    **kwargs,
) -> List[EventIngestionModel]:
    """
    Get all events for the provided timespan.

    Parameters
    ----------
    from_dt: datetime
        Datetime to start event gather from.
    to_dt: datetime
        Datetime to end event gather at.

    Returns
    -------
    events: List[EventIngestionModel]
        All events gathered that occured in the provided time range.

    Notes
    -----
    As the implimenter of the get_events function, you can choose to ignore the from_dt
    and to_dt parameters. However, they are useful for manually kicking off pipelines
    from GitHub Actions UI.
    """
    scraper = LegistarScraper(
        client="olympia",
        timezone="America/Los_Angeles",
    )

    return scraper.get_events(begin=from_dt, end=to_dt)
