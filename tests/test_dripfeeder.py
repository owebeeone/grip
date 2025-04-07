import asyncio
import random
from typing import Any
import pytest
from datatrees import datatree, dtfield
from grip.dripfeeder import ConstantDripFeeder, PushDripFeeder, new_controlled_drip

VERBOSE = False


@datatree
class CollectResult:
    values: list[Any] = dtfield(default_factory=list)
    stop_future: asyncio.Future = dtfield(default_factory=lambda: asyncio.get_event_loop().create_future())
    collect_task: asyncio.Task = dtfield(default=None, init=False)

    async def timed_collect(self, drip):
        # Add initial value
        self.values.append(drip.snapshot())
        
        # Wait for either stop signal or value changes
        while not self.stop_future.done():
            fut = asyncio.ensure_future(drip.on_change())
            done, _ = await asyncio.wait(
                [fut, self.stop_future],
                return_when=asyncio.FIRST_COMPLETED
            )
            if fut in done:
                val = await fut
                self.values.append(val)
                
        return 
    
    def start_collect(self, drip):
        self.collect_task = asyncio.create_task(self.timed_collect(drip))
        
    def stop_collect(self):
        if not self.stop_future.done():
            self.stop_future.set_result(None)
    
    async def await_collect(self):
        await self.collect_task
        return self.values
    

@pytest.mark.asyncio
async def test_constant_dripfeed_snapshot_and_stream():
    dripfeeder = ConstantDripFeeder(42)
    drip = dripfeeder.create_drip()

    # Test snapshot
    assert drip.snapshot() == 42

    # Test async stream yields the static value
    collector = CollectResult()
    collector.start_collect(drip)
    
    # Add a timeout since we expect ConstantDripFeeder to not change
    await asyncio.sleep(0.1)
    collector.stop_collect()
    
    values = await collector.await_collect()
    assert 42 in values


@pytest.mark.asyncio
async def test_push_drip_feed_behavior():
    dripfeeder = PushDripFeeder("initial")
    drip = dripfeeder.create_drip()

    # Create collector to gather values
    collector = CollectResult()
    collector.start_collect(drip)

    # Wait a bit for the initial value to be collected
    await asyncio.sleep(0.01)

    # Push values
    dripfeeder.push("update1")
    await asyncio.sleep(0.01)
    dripfeeder.push("update2")
    await asyncio.sleep(0.01)
    dripfeeder.push("stop")
    
    # Wait for the final value to be processed
    await asyncio.sleep(0.01)
    collector.stop_collect()
    
    # Get collected values
    received = await collector.await_collect()

    print(received)
    assert "initial" in received
    assert "update1" in received
    assert "update2" in received
    assert "stop" in received
    assert drip.snapshot() == "stop"

@pytest.mark.asyncio
async def test_drip_stress_monte_carlo():
    N = 10  # PushDripFeeders
    M = 5   # ConstantDripFeeders
    O = 20  # Total Drips
    T = 3.0  # Duration in seconds
    P = O - (N + M)  # Remaining random drips

    stop_future = asyncio.get_event_loop().create_future()

    # Create feeders
    push_feeders = [PushDripFeeder(0) for _ in range(N)]
    const_feeders = [ConstantDripFeeder("const") for _ in range(M)]

    # Create initial drips
    all_drips = [f.create_drip() for f in const_feeders] + [f.create_drip() for f in push_feeders]

    # Add P more randomly
    feeders = push_feeders + const_feeders
    for _ in range(P):
        f = random.choice(feeders)
        all_drips.append(f.create_drip())

    results: list[list[tuple[int, int]]] = [[] for _ in range(len(all_drips))]

    # Producers
    async def run_producer(idx, feeder):
        counter = 0
        while not stop_future.done():
            await asyncio.sleep(random.uniform(0.0, 0.02))
            feeder.push((idx, counter))
            counter += 1

    # Consumers
    async def run_consumer(idx, drip):
        try:
            while not stop_future.done():
                fut = asyncio.ensure_future(drip.on_change())
                done, _ = await asyncio.wait([fut, stop_future], return_when=asyncio.FIRST_COMPLETED)
                if fut in done:
                    val = await fut
                    results[idx].append(val)
                    
                if stop_future in done:
                    await stop_future
        except Exception as e:
            print(f"Consumer {idx} error: {e}")

    # Launch tasks
    tasks = []
    for i, f in enumerate(push_feeders):
        tasks.append(asyncio.create_task(run_producer(i, f)))

    for i, drip in enumerate(all_drips):
        tasks.append(asyncio.create_task(run_consumer(i, drip)))
        
        # Drip switcher task
    async def run_drifter():
        while not stop_future.done():
            await asyncio.sleep(random.uniform(0.0, 0.02))
            drip_idx = random.randint(0, len(all_drips) - 1)
            feeder = random.choice(feeders)
            all_drips[drip_idx].attach(feeder)  # simulate feeder swap

    tasks.append(asyncio.create_task(run_drifter()))

    # Let it run T seconds
    await asyncio.sleep(T)
    stop_future.set_result(None)
    await asyncio.gather(*tasks)

    # Sanity checks
    for i, out in enumerate(results):
        assert isinstance(out, list), f"Drip {i} did not collect data"
        # Push drips should receive values from their assigned producer index
        producer_ids = {v[0] for v in out if isinstance(v, tuple)}
        assert len(producer_ids) > 0, f"Drip {i} never received push values"
        # Optional: ensure at least one count value > 0
        assert any(v[1] > 0 for v in out if isinstance(v, tuple)), f"Drip {i} values seem static"

    if VERBOSE:
        print("Test completed with:")
        for i, r in enumerate(results):
            print(f"Drip {i}: {len(r)} values")

@pytest.mark.asyncio
async def test_drip_controller_switching():
    f1 = PushDripFeeder("f1-0")
    f2 = PushDripFeeder("f2-0")

    parent_drip = f2.create_drip()
    # Create initial drip and controller
    drip = new_controlled_drip(parent_drip)

    # Collect values
    values = []
    stop_future = asyncio.get_event_loop().create_future()

    async def collect():
        # First get the initial value
        values.append(drip.snapshot())
        
        # Collect values until timeout or we have enough values
        while not stop_future.done():
            fut = asyncio.ensure_future(drip.on_change())
            done, _ = await asyncio.wait(
                [fut, stop_future], 
                return_when=asyncio.FIRST_COMPLETED
            )
            if fut in done:
                val = await fut
                values.append(val)
        print(f"Collect done - {stop_future.done()}")

    collect_task = asyncio.create_task(collect())

    # Push 2 values from f1
    f1.push("f1-1")
    await asyncio.sleep(0.01)
    f2.push("f2-1")
    await asyncio.sleep(0.01)

    # Switch to f2
    parent_drip.attach(f1)
    f2.push("f2-2") # should be ignored
    f1.push("f1-2")
    f2.push("f2-3") # should be ignored
    f1.push("f1-3")
    f2.push("f2-4") # should be ignored
    f1.push("f1-5")
    
    await asyncio.sleep(0.01)

    stop_future.set_result(None)
    await collect_task

    assert "f1-5" in values, "Drip did not update after controller switch"

if __name__ == "__main__":
    
    async def main():
        await test_drip_controller_switching()
        await test_constant_dripfeed_snapshot_and_stream()
        await test_push_drip_feed_behavior()
        await test_drip_stress_monte_carlo()

    asyncio.run(main())
