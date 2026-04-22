from contextlib import asynccontextmanager
from functools import cached_property
import logging
from typing import Any

import anyio

logger = logging.getLogger(__name__)


class HidIoError(OSError): ...


class HidNotOpenError(RuntimeError): ...


class HidFailedToOpenError(RuntimeError): ...


def descriptors_for_path(path: bytes) -> list[dict[str, Any]]:
    import hid

    descriptors = list(hid.enumerate())
    return [d for d in descriptors if d["path"] == path]


class BlockingHidTransport:
    def __init__(self, path: bytes):
        self.path = path
        self._dev: object | None = None

    def _require(self):
        if self._dev is None:
            raise HidNotOpenError("device not open")
        return self._dev

    def open(self) -> None:
        import hid

        d = hid.device()
        d.open_path(self.path)
        d.set_nonblocking(0)
        self._dev = d

    def close(self) -> None:
        d, self._dev = self._dev, None
        if d is not None:
            d.close()

    def read(self, timeout_ms: int, *, read_size: int = 64) -> bytes:
        d = self._require()
        try:
            data = bytes(d.read(read_size, timeout_ms))
            return data
        except Exception as e:
            raise HidIoError(f"hid read failed (result={e})") from e

    def write(self, payload: bytes) -> int:
        d = self._require()
        try:
            if logger.isEnabledFor(logging.DEBUG):
                msg = f"Writing: {payload[:8].hex(' ', 1)}    {payload[8:16].hex(' ', 1)} ({len(payload)} bytes)"
                logger.debug(msg)
            n = d.write(payload)
        except Exception as e:
            raise HidIoError("write failed") from e
        if n < 0:
            raise HidIoError(f"write failed (n={n})")
        return n

    def get_input_report(self, report_id: int, *, read_size: int = 64) -> bytes:
        d = self._require()
        try:
            return bytes(d.get_input_report(report_id, read_size))
        except Exception as e:
            raise HidIoError(f"get_input_report failed (result={e})") from e

    def get_descriptor(self) -> dict[str, Any]:
        descriptors = descriptors_for_path(self.path)
        if len(descriptors) == 0:
            raise HidIoError("device not found")
        return descriptors[0]

    def get_hid(self) -> str:
        descriptor = self.get_descriptor()
        return f"{descriptor['vendor_id']:04X}:{descriptor['product_id']:04X}:{descriptor['serial_number']}"


class _AsyncHidTransport:
    def __init__(
        self,
        path: bytes,
        *,
        read_size: int = 64,
        read_timeout_ms: int = 200,
    ):
        self._dev = BlockingHidTransport(path)
        self._write_lock = anyio.Lock()
        self.read_timeout_ms = read_timeout_ms
        self._started = anyio.Event()
        self._reader_exited = anyio.Event()
        self._send_stream, self._receive_stream = anyio.create_memory_object_stream[
            bytes
        ](max_buffer_size=100)
        self._senders: set[anyio.abc.ObjectSendStream[bytes]] = set()
        self._senders_lock = anyio.Lock()

    @property
    def started(self) -> anyio.Event:
        return self._started

    @cached_property
    def descriptor(self) -> dict[str, Any]:
        return self._dev.get_descriptor()

    @cached_property
    def hid(self) -> str:
        return self._dev.get_hid()

    async def write(self, payload: bytes) -> None:
        async with self._write_lock:
            return await anyio.to_thread.run_sync(self._dev.write, payload)

    async def write_chunks(self, chunks: list[bytes]) -> None:
        async with self._write_lock:
            for chunk in chunks:
                await anyio.to_thread.run_sync(self._dev.write, chunk)

    async def get_input_report(self, report_id: int) -> bytes:
        return await anyio.to_thread.run_sync(self._dev.get_input_report, report_id)

    @asynccontextmanager
    async def subscribe(self) -> anyio.abc.ObjectReceiveStream[bytes]:
        send, receive = anyio.create_memory_object_stream[bytes](max_buffer_size=100)
        async with self._senders_lock:
            self._senders.add(send)
        try:
            yield receive
        finally:
            async with self._senders_lock:
                self._senders.remove(send)

    @asynccontextmanager
    async def _run(self):
        cancelled = anyio.get_cancelled_exc_class()
        try:
            await anyio.to_thread.run_sync(self._dev.open, cancellable=True)
        except cancelled:
            raise
        except BaseException as e:
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(self._dev.close, cancellable=False)
            raise HidFailedToOpenError(f"Failed to open device: {e}") from e

        async with anyio.create_task_group() as tg:
            self._started.set()
            tg.start_soon(self._reader_wrapper, tg.cancel_scope)
            tg.start_soon(self._broadcast_loop)
            try:
                yield self
            finally:
                logger.info("Closing device")
                # Cancel the reader loop and wait for it to exit
                tg.cancel_scope.cancel()

                # Now close the device
                with anyio.CancelScope(shield=True):
                    await self._reader_exited.wait()
                    await anyio.to_thread.run_sync(self._dev.close, cancellable=False)

    async def _reader_wrapper(self, cancel_scope: anyio.CancelScope):
        try:
            await self._reader_loop()
        except HidIoError:
            logger.info("Read error from event loop. Closing device.")
            cancel_scope.cancel()
        finally:
            self._reader_exited.set()

    async def _reader_loop(self) -> None:
        while True:
            data = await anyio.to_thread.run_sync(
                self._dev.read, self.read_timeout_ms, cancellable=True
            )

            if data:
                await self._send_stream.send(data)

    async def _broadcast_loop(self) -> None:
        while True:
            data = await self._receive_stream.receive()

            async with self._senders_lock:
                senders = list(self._senders)

            dead_senders = []
            for sender in senders:
                try:
                    sender.send_nowait(data)
                except anyio.WouldBlock:
                    dead_senders.append(sender)

            async with self._senders_lock:
                for sender in dead_senders:
                    self._senders.remove(sender)
                    await sender.aclose()


@asynccontextmanager
async def AsyncHidTransport(path: bytes) -> _AsyncHidTransport:
    transport = _AsyncHidTransport(path)
    async with transport._run() as transport:
        yield transport
    logger.info("Transport closed")
