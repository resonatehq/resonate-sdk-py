# Task Queue

Just like Celery, you can use Resonate as a task queue. `resonate.rpc` allows you to invoke function in other nodes that might have a different logical group. And sync, by using `handle.result()` awaiting for the return value in other node.
