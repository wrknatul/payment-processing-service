from faststream.rabbit import Channel, RabbitBroker


def build_broker(url: str, *, prefetch_count: int | None = None) -> RabbitBroker:
    # Publisher confirms + on_return_raises: a publish the broker did not confirm or could not
    # route to any queue raises instead of being lost silently.
    channel = Channel(
        prefetch_count=prefetch_count,
        publisher_confirms=True,
        on_return_raises=True,
    )
    return RabbitBroker(url, default_channel=channel)
