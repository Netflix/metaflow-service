from typing import Callable, List

from services.ui_backend_service.data.cache.utils import streamed_errors

from .get_data_action import GetData

from metaflow import Task
from metaflow.cards import get_cards
from metaflow.plugins.cards.exception import CardNotPresentException
import os


class GetCardData(GetData):
    @classmethod
    def format_request(cls, pathspec: str, card_hash: str, invalidate_cache=False):
        """
        Cache Action to fetch Cards HTML content for a pathspec

        Parameters
        ----------
        pathspec : str
            Task pathspec:
                ["FlowId/RunNumber/StepName/TaskId"]
        card_hash : str
            Card hash
        """
        targets = "{}/{}".format(pathspec, card_hash)
        # We set the targets to have the card hash so that we can find the card data for a particular hash
        return super().format_request(
            targets=[targets], invalidate_cache=invalidate_cache
        )

    @classmethod
    def fetch_data(
        cls, pathspec_with_card_hash: str, stream_output: Callable[[str], None]
    ):
        """
        Fetch data using Metaflow Client.

        Parameters
        ----------
        pathspec_with_card_hash : str
            Task pathspec with card hash
                "FlowId/RunNumber/StepName/TaskId/CardHash"
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
            card_load_policy = os.environ.get("MF_CARD_LOAD_POLICY", "full")
            card_data = None
            if card_load_policy == "full":
                card_data = card.get_data()
            if card_data is None:
                return None
            return {"id": card.id, "data": card_data, "hash": card.hash}

        try:
            pathspec = "/".join(pathspec_with_card_hash.split("/")[:-1])
            card_hash = pathspec_with_card_hash.split("/")[-1]
            with streamed_errors(stream_output):
                task = Task("{}".format(pathspec))
                cards_datas = [
                    _card_item(card)
                    for card in get_cards(task)
                    if card.hash == card_hash
                ]
                if len(cards_datas) == 0:
                    # This means there is no card with the given hash
                    return [True, None]
                elif cards_datas[0] is None:
                    # This means the card data is not available
                    return [True, None]
        except CardNotPresentException as e:
            return [False, "card-not-present", str(e)]
        except AttributeError as e:
            # AttributeError is raised when a Task May not have been found and we try to access the
            # task's properties in the card fetch. This is a workaround for the fact that we don't
            # want to throw scary looking 500 errors when we may have intermittent issues.
            return [False, "cannot-fetch-card", str(e)]
        except Exception:
            # NOTE: return false in order not to cache this
            # since parameters might be available later
            return False

        return [True, cards_datas[0]]
