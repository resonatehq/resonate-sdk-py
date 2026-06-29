default:
    just --list


examples:
    uv run python examples/hello-world
    uv run python examples/fibonacci --mode run --n 12
    uv run python examples/fibonacci --mode rpc --n 12
    uv run python examples/fibonacci --mode mix --n 12
    uv run python examples/error-handling --mode run --error none
    uv run python examples/error-handling --mode run --error taken
    uv run python examples/error-handling --mode run --error value
    uv run python examples/error-handling --mode rpc --error none
    uv run python examples/error-handling --mode rpc --error taken
    uv run python examples/error-handling --mode rpc --error value
    uv run python examples/pipeline
    uv run python examples/rpc
    uv run python examples/saga
    uv run python examples/saga --fail hotel
    uv run python examples/saga --fail charge
    uv run python examples/versioning
    uv run python examples/human-in-the-loop --decision approve
    uv run python examples/human-in-the-loop --decision reject
    uv run python examples/recovery
    uv run python examples/detached
    uv run python examples/polling
    uv run python examples/structured-concurrency
    uv run python examples/retries


examples-nats:
    uv run python examples/hello-world-nats
    uv run python examples/fibonacci-nats --mode run --n 12
    uv run python examples/fibonacci-nats --mode rpc --n 12
    uv run python examples/fibonacci-nats --mode mix --n 12
    uv run python examples/error-handling-nats --mode run --error none
    uv run python examples/error-handling-nats --mode run --error taken
    uv run python examples/error-handling-nats --mode run --error value
    uv run python examples/error-handling-nats --mode rpc --error none
    uv run python examples/error-handling-nats --mode rpc --error taken
    uv run python examples/error-handling-nats --mode rpc --error value
    uv run python examples/pipeline-nats
    uv run python examples/rpc-nats
    uv run python examples/saga-nats
    uv run python examples/saga-nats --fail hotel
    uv run python examples/saga-nats --fail charge
    uv run python examples/versioning-nats
    uv run python examples/human-in-the-loop-nats --decision approve
    uv run python examples/human-in-the-loop-nats --decision reject
    uv run python examples/recovery-nats
    uv run python examples/detached-nats
    uv run python examples/polling-nats
    uv run python examples/structured-concurrency-nats
    uv run python examples/retries-nats
