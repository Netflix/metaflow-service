from typing import Callable, List

from services.ui_backend_service.data.cache.utils import streamed_errors

from .get_data_action import GetData

from metaflow import Task
from metaflow.cards import get_cards
import os


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
        def _card_item(card, card_idx):
            card_load_policy = os.environ.get('MF_CARD_LOAD_POLICY', 'full')
            if card_load_policy == 'full':
                card_html = card.get()
            elif card_load_policy == 'blurb_only':
                card_html = f"""<html>
                <body>
Your organization has disabled cards viewing from the Metaflow UI. Here is a code snippet to retrieve cards using the Metaflow client library:

<pre>
<code>
from metaflow import Task, namespace
from metaflow.cards import get_cards

namespace(None)
task = Task("{pathspec}")
card = get_cards(task)[{card_idx}]

# Uncomment block below to view card in a web browser.
# import tempfile
# import webbrowser
# html_file = tempfile.mktemp(".html")
# with open(html_file, 'w') as f:
#    f.write(card.get())
# webbrowser.open_new('file://' + html_file)

</code>
</pre>

Please visit <a href="https://docs.metaflow.org/api/client" target="_blank">https://docs.metaflow.org/api/client</a> for detailed documentation.
                </body>
                </html>"""
            else:
                raise ValueError(f"Invalid value for MF_CARD_LOAD_POLICY ({card_load_policy}) - must be 'full' or 'blurb_only'")
            return {
                "id": card.id,
                "type": card.type,
                "html": card_html
            }
        try:
            with streamed_errors(stream_output):
                task = Task("{}".format(pathspec))
                cards = {card.hash: _card_item(card, i) for i, card in enumerate(get_cards(task))}
        except Exception:
            # NOTE: return false in order not to cache this
            # since parameters might be available later
            return False

        return [True, cards]
