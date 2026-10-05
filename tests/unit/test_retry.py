from app.messaging.retry import RetryPolicy
from app.messaging.topology import build_topology


def test_default_policy_backs_off_exponentially_and_stops_after_third_attempt() -> None:
    policy = RetryPolicy()

    assert policy.delay_ms_after(1) == 5_000
    assert policy.delay_ms_after(2) == 10_000
    assert policy.delay_ms_after(3) is None
    assert policy.delays_ms == (5_000, 10_000)


def test_policy_respects_custom_settings() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay_seconds=0.5, multiplier=3)

    assert policy.delays_ms == (500, 1_500, 4_500)
    assert policy.delay_ms_after(4) is None


def test_single_attempt_policy_never_retries() -> None:
    policy = RetryPolicy(max_attempts=1)

    assert policy.delay_ms_after(1) is None
    assert policy.delays_ms == ()


def test_topology_names_and_dead_lettering() -> None:
    topology = build_topology(RetryPolicy())

    assert topology.exchange.name == "payments"
    assert topology.queue.name == "payments.new"
    assert topology.queue.routing_key == "payments.new"
    assert topology.queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert topology.queue.arguments["x-dead-letter-routing-key"] == "payments.dead"
    assert topology.dead_letter_exchange.name == "payments.dlx"
    assert topology.dead_letter_queue.name == "payments.dlq"
    assert topology.dead_letter_queue.routing_key == "payments.dead"


def test_each_delay_has_a_ttl_queue_that_routes_back_to_the_main_queue() -> None:
    topology = build_topology(RetryPolicy())

    assert topology.retry_exchange.name == "payments.retry"
    assert sorted(topology.retry_queues) == [5_000, 10_000]
    for delay_ms, queue in topology.retry_queues.items():
        assert queue.name == f"payments.retry.{delay_ms}"
        assert queue.routing_key == queue.name
        assert queue.arguments["x-message-ttl"] == delay_ms
        assert queue.arguments["x-dead-letter-exchange"] == "payments"
        assert queue.arguments["x-dead-letter-routing-key"] == "payments.new"


def test_prefix_namespaces_every_name() -> None:
    topology = build_topology(RetryPolicy(), prefix="t1.")

    names = [
        topology.exchange.name,
        topology.queue.name,
        topology.queue.routing_key,
        topology.queue.arguments["x-dead-letter-exchange"],
        topology.queue.arguments["x-dead-letter-routing-key"],
        topology.retry_exchange.name,
        topology.dead_letter_exchange.name,
        topology.dead_letter_queue.name,
        topology.dead_letter_queue.routing_key,
    ]
    for queue in topology.retry_queues.values():
        names += [
            queue.name,
            queue.arguments["x-dead-letter-exchange"],
            queue.arguments["x-dead-letter-routing-key"],
        ]

    assert all(name.startswith("t1.") for name in names)
