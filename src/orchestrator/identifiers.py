from uuid import UUID, uuid4


def new_id() -> str:
    value: UUID = uuid4()
    return str(value)
