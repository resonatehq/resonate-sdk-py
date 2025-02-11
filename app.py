# from __future__ import annotations

# import queue
# import random
# from concurrent.futures import Future

# from resonate.context import Context
# from resonate.registry import Registry
# from resonate.scheduler import Command, Invoke, Scheduler
# from resonate.stores import LocalStore

# __all__ = ["Invoke"]


# def foo(ctx: Context):
#     p1 = yield ctx.rfi(bar)
#     p2 = yield ctx.rfi(baz)

#     v1 = yield p1
#     v2 = yield p2

#     return v1 + v2


# def bar(ctx: Context):
#     return (yield ctx.rfc(baz))


# def baz():
#     return "baz"


# pid = "default"

# interrupted = False

# # store is non volatile
# store = LocalStore()

# while not interrupted:
#     # everything else is volatile

#     cmds: list[Command | tuple[Command, Future]] = []

#     cq = queue.Queue[Command | tuple[Command, Future] | None]()

#     registry = Registry()
#     registry.add("foo", foo)
#     registry.add("bar", bar)
#     registry.add("baz", baz)

#     scheduler = Scheduler(pid=pid, cq=cq, registry=registry, store=store)

#     # now connect to the store
#     # store.connect(pid, LocalSender(scheduler, store, scheduler.registry))

#     while True:
#         # Phase 1: Enqueue command
#         try:
#             if cmd := input("❯ "):
#                 scheduler.enqueue(eval(cmd))
#             elif cmds:
#                 cmd = random.choice(cmds)
#                 cmds.remove(cmd)

#                 match cmd:
#                     case (cmd, future):
#                         scheduler.enqueue(cmd, future)
#                     case cmd:
#                         scheduler.enqueue(cmd)
#         except EOFError:
#             print("⚡⚡⚡")
#             # store.disconnect(pid)
#             break
#         except KeyboardInterrupt:
#             interrupted = True
#             break
#         except Exception as e:
#             print(e)
#             continue

#         # Phase 2: Step scheduler
#         try:
#             scheduler.step()
#         except queue.Empty:
#             pass

#         # Phase 3: Step processor
#         while True:
#             try:
#                 scheduler.processor.step()
#             except queue.Empty:
#                 break

#         # Phase 4: Grab commands
#         while True:
#             try:
#                 if cmd := cq.get_nowait():
#                     cmds.append(cmd)
#             except queue.Empty:
#                 break

#     for computation in scheduler.computations.values():
#         computation.print()

# # tmp: print all promises and tasks
# print("\nPromises:")
# for promise in store.promises._promises.values():
#     print(promise)

# print("\nTasks:")
# for task in store.tasks._tasks.values():
#     print(task)
