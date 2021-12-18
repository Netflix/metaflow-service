from typing import Callable, List

from services.ui_backend_service.data.cache.utils import streamed_errors

from .get_data_action import GetData

from metaflow import Task
from metaflow.plugins.cards.card_client import get_cards


class GetCards(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        """
        Cache Action to fetch Cards HTML content for a pathspec

        Parameters
        ----------
        pathspec : str
            Task pathspec:
                ["FlowId/RunNumber/StepName/TaskId"]
        invalidate_cache : bool
            Force cache invalidation, defaults to False
        """
        return super().format_request(targets=pathspecs, invalidate_cache=invalidate_cache)

    @classmethod
    def fetch_data(cls, pathspec: str, stream_output: Callable[[str], None]):
        """
        Fetch data using Metaflow Client.

        Parameters
        ----------
        pathspec : str
            Task pathspec
                "FlowId/RunNumber/StepName/TaskId"
        stream_output : Callable[[object], None]
            Stream output callable from execute() that accepts a JSON serializable object.
            Used for generic messaging.

        Errors can be streamed to cache client using `stream_output` in combination with
        the error_event_msg helper. This way failures won't be cached for individual artifacts,
        thus making it necessary to retry fetching during next attempt.
        (Will add significant overhead/delay).

        Stream error example:
            stream_output(error_event_msg(str(ex), "s3-not-found", get_traceback_str()))
        """
        def _card_item(card):
            return {
                "type": card.type,
                "html": card.get()
            }
        try:
            with streamed_errors(stream_output):
                task = Task("{}".format(pathspec))
                cards = {index: _card_item(card) for index, card in enumerate(get_cards(task))}
        except Exception:
            # NOTE: return false in order not to cache this
            # since parameters might be available later
            return False

        return [True, cards]
