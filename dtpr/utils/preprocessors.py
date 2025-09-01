from dtpr.base import Event
from dtpr.utils.functions import deltaR
from typing import Optional


def test_preprocessor(event: Event, dummy_val: Optional[int] = -999) -> None:
    try:
        p1, p2 = event.genmuons
        event.dR = deltaR(p1, p2)
    except ValueError:
        event.dR = dummy_val
