from orchestrator.identifiers import new_id


def test_new_id_is_non_empty_and_stable_as_text():
    value = new_id()
    assert isinstance(value, str)
    assert len(value) == 36
