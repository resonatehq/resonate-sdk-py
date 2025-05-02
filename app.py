from resonate import Resonate, Context

resonate = Resonate()

@resonate.register
def foo(ctx: Context) -> str:
    return "Hello, world!"

# ok
handle = resonate.run("a", "foo")
print("a:", handle.result())

# ko
handle = resonate.run("b", foo)
print("b", handle.result())
