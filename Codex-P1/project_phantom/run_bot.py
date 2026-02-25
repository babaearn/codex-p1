from __future__ import annotations

import argparse
import asyncio
import contextlib

from project_phantom.config import Layer0Config, Layer1Config, Layer2Config, Layer3Config
from project_phantom.core.types import AbsorptionEvent, ExecutionEvent, PrePumpEvent, TrapSetupEvent
from project_phantom.layer0.trap_detector import run_layer0
from project_phantom.layer1.absorption_engine import run_layer1
from project_phantom.layer2.ignition_engine import run_layer2
from project_phantom.layer3.executor import run_layer3


async def _execution_printer(queue: asyncio.Queue[ExecutionEvent], stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        print(
            f"[EXECUTION] symbol={event.symbol} direction={event.direction} "
            f"entry={event.plan.entry} sl={event.plan.sl} tp1={event.plan.tp1} tp2={event.plan.tp2} "
            f"rr=1:{event.plan.rr} mode={event.raw.get('execution_mode')}"
        )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Project PHANTOM 4-layer pipeline.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    stop_event = asyncio.Event()
    queue_l0: asyncio.Queue[TrapSetupEvent] = asyncio.Queue(maxsize=200)
    queue_l1: asyncio.Queue[AbsorptionEvent] = asyncio.Queue(maxsize=200)
    queue_l2: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=200)
    queue_l3: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=200)

    layer0 = Layer0Config(symbol=args.symbol)
    layer1 = Layer1Config(symbol=args.symbol)
    layer2 = Layer2Config(symbol=args.symbol)
    layer3 = Layer3Config(symbol=args.symbol, execution_mode=args.mode)
    if args.no_telegram:
        layer3.telegram.enabled = False

    tasks = [
        asyncio.create_task(run_layer0(layer0, queue_l0, stop_event=stop_event), name="layer0"),
        asyncio.create_task(run_layer1(layer1, queue_l0, queue_l1, stop_event=stop_event), name="layer1"),
        asyncio.create_task(run_layer2(layer2, queue_l1, queue_l2, stop_event=stop_event), name="layer2"),
        asyncio.create_task(
            run_layer3(layer3, queue_l2, out_queue=queue_l3, stop_event=stop_event),
            name="layer3",
        ),
        asyncio.create_task(_execution_printer(queue_l3, stop_event), name="execution-printer"),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        raise
    finally:
        stop_event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
