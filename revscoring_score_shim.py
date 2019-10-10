"""
``revscoring score -h``
::

    Scores a set of revisions.

    Usage:
        score (-h | --help)
        score <model-file> --host=<uri> [<rev_id>...]
              [--rev-ids=<path>] [--cache=<json>] [--caches=<json>]
              [--batch-size=<num>] [--io-workers=<num>] [--cpu-workers=<num>]
              [--debug] [--verbose]

    Options:
        -h --help           Print this documentation
        <model-file>        Path to a model file
        --host=<url>        The url pointing to a MediaWiki API to use for
                            extracting features
        <rev_id>            A revision identifier to score.
        --rev-ids=<path>    The path to a file containing revision identifiers
                            to score (expects a column called 'rev_id').  If
                            any <rev_id> are provided, this argument is
                            ignored. [default: <stdin>]
        --cache=<json>      A JSON blob of cache values to use during
                            extraction for every call.
        --caches=<json>     A JSON blob of rev_id-->cache value pairs to use
                            during extraction
        --batch-size=<num>  The size of the revisions to batch when requesting
                            data from the API [default: 50]
        --io-workers=<num>  The number of worker processes to use for
                            requesting data from the API [default: <auto>]
        --cpu-workers=<num>  The number of worker processes to use for
                             extraction and scoring [default: <cpu-count>]
        --debug             Print debug logging
        --verbose           Print feature extraction debug logging
"""

# monkey-patch score processor so that it never errors
from revscoring.utilities import score
import sys
import time
class MyScoreProcessor(score.ScoreProcessor):

    @classmethod
    def _process_score(cls, e_r_caches):
        try:
            pair = super()._process_score(e_r_caches)
            rev_id = pair[0]
            error = pair[1]
            return rev_id, error
        except Exception as error:
            rev_id, scoring_model, extractor, cache, _ = e_r_caches
#            raise error
            return rev_id, error_score(error)

score.ScoreProcessor = MyScoreProcessor

if __name__ == "__main__":
    from threading import Timer

    class RepeatingTimer(Timer):
        def run(self):
            while not self.finished.is_set():
                self.function(*self.args, **self.kwargs)
                self.finished.wait(self.interval)


    t = RepeatingTimer(30.0, lambda: sys.stderr("{0}:ScoreProcessor is running".format(time.now())))
    score.main()
