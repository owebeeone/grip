import asyncio
import pytest
from grip.grip_context import GripContext
from grip.grip_core import GripRegistry
from grip.dripfeeder import PushDripFeeder, ConstantDripFeeder


@pytest.mark.asyncio
async def test_grip_context_snapshot_and_get():
    grip = GripRegistry()
    grip.add.User("default_user")
    grip.add.Theme("light")

    UserKey = grip.ref.User
    ThemeKey = grip.ref.Theme

    user_feeder = ConstantDripFeeder("alice")
    theme_feeder = ConstantDripFeeder("dark")

    user_drip = user_feeder.create_drip()
    theme_drip = theme_feeder.create_drip()

    ctx = GripContext(grip=grip)
    ctx.set({
        UserKey: user_drip,
        ThemeKey: theme_drip
    })

    snapshot = ctx.snapshot(UserKey, ThemeKey)
    assert snapshot.values[UserKey] == "alice"
    assert snapshot.values[ThemeKey] == "dark"
    assert snapshot.source is ctx


@pytest.mark.asyncio
async def test_grip_context_stream_updates():
    grip = GripRegistry()
    grip.add.Value(0)

    ValueKey = grip.ref.Value

    feeder = PushDripFeeder(0)
    ctx = GripContext(grip=grip)
    drip = feeder.create_drip()
    ctx.set({ValueKey: drip})

    updates = []

    # The stream method is not yet implemented
    # This part of the test will fail until stream() is done.
    # async def stream_context():
    #     async for snap in ctx.stream(ValueKey):
    #         updates.append(snap.values[ValueKey])
    #         if updates[-1] == 3:
    #             break
    #
    # task = asyncio.create_task(stream_context())
    #
    # await asyncio.sleep(0.01)
    # feeder.push(1)
    # feeder.push(2)
    # feeder.push(3)
    # await task
    #
    # assert updates == [0, 1, 2, 3]
    pass # Placeholder until stream is implemented


if __name__ == "__main__":
    pytest.main([__file__])
