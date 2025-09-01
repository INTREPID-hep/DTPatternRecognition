from dtpr.base import Event


def test_selector(event: Event) -> bool:
    genMuons = getattr(event, "genmuons", [])
    AtLeastTwoMuons = len(genMuons) > 1

    return AtLeastTwoMuons
