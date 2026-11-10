"""Prospective arithmetic only; callers must persist reservations under lock."""
MAX_ACTORS = 128
MAX_RECORD_BYTES = 16384
MAX_EVENTS = 2 + 3 * MAX_ACTORS + 1
MAX_HISTORY_BYTES = MAX_EVENTS * MAX_RECORD_BYTES


def fits_capacity(*, events: int, payload_bytes: int, outstanding_acks: int,
                  outstanding_stops: int, closed: bool) -> bool:
    if type(closed) is not bool:
        return False
    values = (events, payload_bytes, outstanding_acks, outstanding_stops)
    if any(type(value) is not int or value < 0 for value in values):
        return False
    if outstanding_acks > MAX_ACTORS or outstanding_stops > MAX_ACTORS:
        return False
    remaining = outstanding_acks + outstanding_stops + 1 + (not closed)
    return (events <= MAX_EVENTS - remaining
            and payload_bytes <= events * MAX_RECORD_BYTES
            and payload_bytes <= MAX_HISTORY_BYTES - remaining * MAX_RECORD_BYTES)
