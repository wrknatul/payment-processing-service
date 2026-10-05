"""RabbitMQ topology.

    payments (direct) --payments.new--> [payments.new]  --reject--> payments.dlx
    payments.retry (direct) --payments.retry.<ttl>--> [payments.retry.<ttl>]  --expired--> payments
    payments.dlx (direct) --payments.dead--> [payments.dlq]

Retries are delayed by parking a message in a queue with x-message-ttl whose dead letter exchange
routes it back to payments.new. Each delay has its own queue: a single queue with per-message TTL
would hold a short delay behind a longer one at its head.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.messaging.events import PAYMENTS_NEW_ROUTING_KEY
from app.messaging.retry import RetryPolicy

PAYMENTS_EXCHANGE = "payments"
RETRY_EXCHANGE = "payments.retry"
DEAD_LETTER_EXCHANGE = "payments.dlx"
DEAD_LETTER_QUEUE = "payments.dlq"
DEAD_LETTER_ROUTING_KEY = "payments.dead"

ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"


@dataclass(frozen=True)
class Topology:
    exchange: RabbitExchange
    queue: RabbitQueue
    retry_exchange: RabbitExchange
    retry_queues: Mapping[int, RabbitQueue]
    dead_letter_exchange: RabbitExchange
    dead_letter_queue: RabbitQueue

    def retry_queue(self, delay_ms: int) -> RabbitQueue:
        return self.retry_queues[delay_ms]


def build_topology(retry_policy: RetryPolicy, prefix: str = "") -> Topology:
    """Describe all exchanges and queues.

    `prefix` namespaces every name so tests can share a broker; production uses an empty one.
    """
    exchange = _direct_exchange(f"{prefix}{PAYMENTS_EXCHANGE}")
    retry_exchange = _direct_exchange(f"{prefix}{RETRY_EXCHANGE}")
    dead_letter_exchange = _direct_exchange(f"{prefix}{DEAD_LETTER_EXCHANGE}")

    queue_name = f"{prefix}{PAYMENTS_NEW_ROUTING_KEY}"
    dead_routing_key = f"{prefix}{DEAD_LETTER_ROUTING_KEY}"

    queue = RabbitQueue(
        queue_name,
        durable=True,
        routing_key=queue_name,
        arguments={
            "x-dead-letter-exchange": dead_letter_exchange.name,
            "x-dead-letter-routing-key": dead_routing_key,
        },
    )

    retry_queues: dict[int, RabbitQueue] = {}
    for delay_ms in retry_policy.delays_ms:
        # The TTL is part of the name: a queue cannot be redeclared with different arguments,
        # so changing the delays creates new queues instead of failing with PRECONDITION_FAILED.
        name = f"{retry_exchange.name}.{delay_ms}"
        retry_queues[delay_ms] = RabbitQueue(
            name,
            durable=True,
            routing_key=name,
            arguments={
                "x-message-ttl": delay_ms,
                "x-dead-letter-exchange": exchange.name,
                "x-dead-letter-routing-key": queue_name,
            },
        )

    dead_letter_queue = RabbitQueue(
        f"{prefix}{DEAD_LETTER_QUEUE}", durable=True, routing_key=dead_routing_key
    )

    return Topology(
        exchange=exchange,
        queue=queue,
        retry_exchange=retry_exchange,
        retry_queues=retry_queues,
        dead_letter_exchange=dead_letter_exchange,
        dead_letter_queue=dead_letter_queue,
    )


async def declare_topology(broker: RabbitBroker, topology: Topology) -> None:
    """Declare everything idempotently; the broker must be connected.

    Both the outbox relay and the consumer call this on startup, so neither depends on the other
    having started first.
    """
    bindings = [
        (topology.exchange, topology.queue),
        *((topology.retry_exchange, queue) for queue in topology.retry_queues.values()),
        (topology.dead_letter_exchange, topology.dead_letter_queue),
    ]
    for exchange, queue in bindings:
        declared_exchange = await broker.declare_exchange(exchange)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing_key)


def _direct_exchange(name: str) -> RabbitExchange:
    return RabbitExchange(name, type=ExchangeType.DIRECT, durable=True)
