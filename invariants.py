# from typing import Callable


# def invariant_replay_r1(func: Callable) -> None:
#     reg = Registry()
#     reg.register("func", func)
#     core, effects, root = _setup(reg)

#     # The outcome carries the tree on both arms (``_ExecFulfilled.tree`` /
#     # ``_ExecSuspended.tree``), so one inner call yields one ``Tree`` snapshot.
#     outcome1 = await core.execute_until_blocked_inner(root, effects)
#     tree1 = outcome1.tree

#     outcome2 = await core.execute_until_blocked_inner(root, effects)
#     tree2 = outcome2.tree
#     assert tree2.is_prune_of(tree1)
#     assert not tree2.is_equal(tree1)  # strictly fewer nodes -- the grandchild

#     outcome3 = await core.execute_until_blocked_inner(root, effects)
#     tree3 = outcome3.tree
#     assert tree3.is_equal(tree2)


# async def invariant_replay_r2() -> None:
#     """Settle the blocking rpc between runs: replay prunes *and* extends.

#     ``tree1`` suspends on rpc ``a`` (``f.2``); ``ctx.rpc("b")`` is never reached,
#     so ``f.3`` does not yet exist. After ``a`` settles in the shared cache,
#     ``tree2``:

#     * **prunes** ``f.1.1`` -- child is settled, its body is skipped; and
#     * **extends** with ``f.3`` -- the body runs past ``await a`` and spawns
#       rpc ``b``, a node ``tree1`` never had.

#     Neither node set contains the other, so neither ``is_prune_of`` nor
#     ``is_extension_of`` holds directly -- the replay factors into a prune of
#     ``tree1`` followed by an extension (``tree.md`` §6/§8).
#     """

#     reg = Registry()
#     reg.register("func", func)
#     core, effects, root = _setup(reg)

#     outcome1 = await core.execute_until_blocked_inner(root, effects)
#     tree1 = outcome1.tree

#     # Settle rpc "a" (``f.2``) in place in the shared cache -- modeling the
#     # server (or a remote worker) settling the promise the suspended task was
#     # blocked on -- so the next inner replay sees it terminal and progresses
#     # past the await.
#     outcome2 = await core.execute_until_blocked_inner(root, effects)
#     tree2 = outcome2.tree
#     assert tree2.is_prune_of(tree1)

#     for suspended in outcome1:
#         _settle_external(effects, suspended)

#         outcome3 = await core.execute_until_blocked_inner(root, effects)
#         tree3 = outcome3.tree
#         # Prune (f.1.1 gone) AND extend (f.3 is new) -- neither contains the other,
#         # so neither atom holds directly; it factors into a prune then an extension
#         # (see test_tree.test_mixed_replay_factors_into_prune_then_extension).

#         assert tree3.is_extension_of(tree2)  # f.1.1 was pruned away
