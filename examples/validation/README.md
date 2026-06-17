# validation — check a function's output before it is persisted

Some leaves are non-deterministic — the textbook case is a call to an LLM, which
can return malformed or incomplete output. You often want to check that output
*before* it becomes a durable result:

> `ctx.options(validate=fn).run(...)` runs `fn` on the return value before
> Resonate persists it. A falsy verdict — or a raised exception — discards the
> value and retries the leaf, exactly like an outright failure.

The validator is `Callable[[value], bool]` (sync or `async`). Only once the
output validates is it stored as the `ctx.run` child's durable result, so an
invalid value is **never** written.

## It rides on the retry machinery

Validation feeds the rejection through the same retry loop as
[`retries`](../retries), so it inherits that example's rule:

> **only a leaf is ever re-run** — a function that performs no `ctx.run` /
> `ctx.rpc` / `ctx.sleep` / `ctx.promise` / `ctx.detached`.

A leaf (e.g. your LLM call) has no durable footprint, so re-running it is safe. A
**workflow**'s failed validation cannot be re-run and settles as a rejection.

When retries are exhausted, the rejection that settles is a `ValidationError`
(when the last verdict was falsy) or the exception the validator raised (e.g. a
`json.JSONDecodeError`). The original type survives the durability boundary, so a
caller can `except ValidationError` to tell "output never validated" apart from
an ordinary domain failure.

## One validator, two inputs

A single `onboard` workflow validates the same flaky `extract_contact` leaf with
one validator (wants a `name` + `email`). The fake `Model` behaves differently
per prompt:

| Call | Outcome |
|------|---------|
| `onboard("ada")` | model warms up (prose → missing email → valid) and validates on the 3rd try → **success** |
| `onboard("grace")` | model never returns an email, so retries exhaust → **`ValidationError`** |

`validate` applies to **local `ctx.run` children** — the place a return value is
produced and persisted in this process. It is a per-call option, set the same way
as a per-call retry policy:

```python
await ctx.options(retry_policy=POLICY, validate=is_valid_contact).run(fn, ...)
```

## Run it

Start a Resonate server on `localhost:8001` (`resonate dev`), then:

```sh
uv run python examples/validation
```

Expected output:

```
onboard('ada') -- output validated before it is stored:
  model call 1 for 'ada'
  model call 2 for 'ada'
    validator: incomplete {'name': 'Ada Lovelace'}
  model call 3 for 'ada'
    validator: ok
  -> Ada Lovelace <ada@example.com>
onboard('grace') -- output never validates:
  model call 1 for 'grace'
    validator: incomplete {'name': 'Grace Hopper'}
  model call 2 for 'grace'
    validator: incomplete {'name': 'Grace Hopper'}
  model call 3 for 'grace'
    validator: incomplete {'name': 'Grace Hopper'}
  model call 4 for 'grace'
    validator: incomplete {'name': 'Grace Hopper'}
  model call 5 for 'grace'
    validator: incomplete {'name': 'Grace Hopper'}
  model call 6 for 'grace'
    validator: incomplete {'name': 'Grace Hopper'}
  -> rejected (ValidationError): validation failed for result of extract_contact
```

`onboard("ada")`'s first reply is prose (the `json.loads` in the validator
raises, so it retries) and its second is missing the email (a falsy verdict, so
it retries); the third validates. `onboard("grace")` parses every time but never
carries an email, so the `Constant(max_retries=5, delay=0)` policy gives up after
6 attempts and the `ValidationError` settles as the rejection.
