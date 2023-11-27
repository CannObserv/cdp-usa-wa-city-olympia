#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

# Standard Library Imports
from datetime import datetime
import logging
import re
from typing import List

# Third-Party Imports
from bs4 import BeautifulSoup
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import urlopen

# Application Imports
from cdp_backend.pipeline.ingestion_models import EventIngestionModel
from cdp_scrapers.legistar_utils import (
    LEGISTAR_SESSION_VIDEO_URI,
    LEGISTAR_EV_SITE_URL,
    LegistarScraper,
    ContentUriScrapeResult,
    parse_video_page_url,
)
from cdp_scrapers.scraper_utils import str_simplified
from cdp_scrapers.types import ContentURIs

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


def get_legistar_content_uris(client: str, legistar_ev: dict) -> ContentUriScrapeResult:
    """
    Return URLs for videos and captions from a Legistar/Granicus-hosted video web page.

    Parameters
    ----------
    client: str
        Which legistar client to target. Ex: "seattle"
    legistar_ev: Dict
        Data for one Legistar Event.

    Returns
    -------
    ContentUriScrapeResult
        status: ContentUriScrapeResult.Status
            Status code describing the scraping process. Use uris only if status is Ok
        uris: Optional[List[ContentURIs]]
            URIs for video and optional caption

    Raises
    ------
    NotImplementedError
        Means the content structure of the web page hosting session video has changed.
        We need explicit review and update the scraping code.
    ConnectionError
        When the Legistar site (e.g. *.legistar.com) itself may be down.

    See Also
    --------
    LegistarScraper.get_content_uris
    cdp_scrapers.legistar_content_parsers
    """
    global video_page_parser

    # prefer video file path in legistar Event.EventVideoPath
    if legistar_ev[LEGISTAR_SESSION_VIDEO_URI]:
        return (
            ContentUriScrapeResult.Status.Ok,
            [
                ContentURIs(
                    video_uri=str_simplified(legistar_ev[LEGISTAR_SESSION_VIDEO_URI]),
                    caption_uri=None,
                )
            ],
        )
    if not legistar_ev[LEGISTAR_EV_SITE_URL]:
        return (ContentUriScrapeResult.Status.UnrecognizedPatternError, None)

    try:
        # a td tag with a certain id pattern.
        # this is usually something like
        # https://somewhere.legistar.com/MeetingDetail.aspx...
        # that is a summary-like page for a meeting
        with urlopen(legistar_ev[LEGISTAR_EV_SITE_URL]) as resp:
            soup = BeautifulSoup(resp.read(), "html.parser")

            if "server error" in soup.text.lower():
                try:
                    url_attrs = urlsplit(legistar_ev[LEGISTAR_EV_SITE_URL])
                    netloc = url_attrs.netloc
                except ValueError:
                    netloc = legistar_ev[LEGISTAR_EV_SITE_URL]
                raise ConnectionError(
                    f"{netloc} appears to be down: {str_simplified(soup.text)}"
                )

    except (URLError, HTTPError) as e:
        log.debug(f"{legistar_ev[LEGISTAR_EV_SITE_URL]}: {str(e)}")
        return (ContentUriScrapeResult.Status.ResourceAccessError, None)

    # this gets us the url for the web PAGE containing the video
    # video link is provided in the window.open()command inside onclick event
    # <a id="ctl00_ContentPlaceHolder1_hypVideo"
    # data-event-id="75f1e143-6756-496f-911b-d3abe61d64a5"
    # data-running-text="In&amp;nbsp;progress" class="videolink"
    # onclick="window.open('Video.aspx?
    # Mode=Granicus&amp;ID1=8844&amp;G=D64&amp;Mode2=Video','video');
    # return false;"
    # href="#" style="color:Blue;font-family:Tahoma;font-size:10pt;">Video</a>
    extract_url = soup.find(
        "a", id=re.compile(r"ct\S*_ContentPlaceHolder\S*_hypVideo"), onclick=True
    )
    if extract_url is None:
        return (ContentUriScrapeResult.Status.UnrecognizedPatternError, None)

    # NOTE: after this point, failing to scrape video url should raise an exception.
    # We need to be alerted that we probabaly have a new web page structure.

    extract_url = extract_url["onclick"]
    start = extract_url.find("'") + len("'")
    end = extract_url.find("',")
    video_page_url = f"https://{client}.legistar.com/{extract_url[start:end]}"

    log.debug(f"{legistar_ev[LEGISTAR_EV_SITE_URL]} -> {video_page_url}")
    try:
        uris = parse_video_page_url(video_page_url, client)
    except HTTPError as e:
        log.debug(f"Error opening {video_page_url}:\n{str(e)}")
        return (ContentUriScrapeResult.Status.ResourceAccessError, None)

    if uris is None:
        raise NotImplementedError(
            "get_legistar_content_uris() needs attention. "
            f"Unrecognized video web page HTML structure: {video_page_url}"
        )
    return (ContentUriScrapeResult.Status.Ok, uris)
