"""Concurrent ClientState operations from simulated Web API workers.

Run with: python -m test.test_thread_safety
"""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from core.state import ClientState
from test.test_report import run_test_module


class ClientStateThreadSafetyTest(unittest.TestCase):
    def setUp(self):
        self.state = ClientState()

    def test_01_parallel_outbox_producers_do_not_lose_or_duplicate_messages(self):
        worker_count = 20
        messages_per_worker = 50

        def enqueue(worker):
            for index in range(messages_per_worker):
                self.state.queue_outbox_message(
                    worker,
                    "127.0.0.1",
                    30000 + worker,
                    f"{worker}:{index}",
                )

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            list(pool.map(enqueue, range(worker_count)))

        queued = self.state.fetch_outbox_messages()
        expected = {
            f"{worker}:{index}"
            for worker in range(worker_count)
            for index in range(messages_per_worker)
        }
        self.assertEqual(len(queued), worker_count * messages_per_worker)
        self.assertEqual({message["content"] for message in queued}, expected)

    def test_02_parallel_add_message_calls_preserve_all_shared_stream_entries(self):
        worker_count = 20
        messages_per_worker = 50

        def add_messages(worker):
            for index in range(messages_per_worker):
                self.state.add_message(1, f"worker-{worker}", f"{worker}:{index}")

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            list(pool.map(add_messages, range(worker_count)))

        messages = self.state.streams[1]
        self.assertEqual(len(messages), worker_count * messages_per_worker)
        self.assertEqual(
            {message["text"] for message in messages},
            {
                f"{worker}:{index}"
                for worker in range(worker_count)
                for index in range(messages_per_worker)
            },
        )

    def test_03_parallel_add_message_calls_keep_separate_streams(self):
        worker_count = 20
        messages_per_worker = 30

        def add_to_stream(stream_id):
            for index in range(messages_per_worker):
                self.state.add_message(stream_id, "sender", f"{stream_id}:{index}")

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            list(pool.map(add_to_stream, range(1, worker_count + 1)))

        for stream_id in range(1, worker_count + 1):
            messages = self.state.streams[stream_id]
            self.assertEqual(len(messages), messages_per_worker)
            self.assertTrue(all(
                message["text"].startswith(f"{stream_id}:")
                for message in messages
            ))

    def test_04_simultaneous_outbox_fetches_drain_each_message_once(self):
        message_count = 1000
        for index in range(message_count):
            self.state.queue_outbox_message(0, "127.0.0.1", 30000, str(index))

        with ThreadPoolExecutor(max_workers=20) as pool:
            batches = list(pool.map(
                lambda _: self.state.fetch_outbox_messages(),
                range(20),
            ))

        drained = [message["content"] for batch in batches for message in batch]
        self.assertEqual(len(drained), message_count)
        self.assertEqual(set(drained), {str(index) for index in range(message_count)})

    def test_05_simultaneous_outbox_producers_and_consumers_lose_no_messages(self):
        producer_count = 10
        consumer_count = 5
        messages_per_producer = 100
        producers_done = threading.Event()
        drained = []
        drained_lock = threading.Lock()

        def produce(worker):
            for index in range(messages_per_producer):
                self.state.queue_outbox_message(
                    0, "127.0.0.1", 31000, f"{worker}:{index}"
                )

        def consume():
            while not producers_done.is_set():
                batch = self.state.fetch_outbox_messages()
                if batch:
                    with drained_lock:
                        drained.extend(message["content"] for message in batch)
                else:
                    time.sleep(0.001)
            while True:
                batch = self.state.fetch_outbox_messages()
                if not batch:
                    return
                with drained_lock:
                    drained.extend(message["content"] for message in batch)

        with ThreadPoolExecutor(
            max_workers=producer_count + consumer_count
        ) as pool:
            consumers = [pool.submit(consume) for _ in range(consumer_count)]
            producers = [
                pool.submit(produce, worker)
                for worker in range(producer_count)
            ]
            for producer in producers:
                producer.result()
            producers_done.set()
            for consumer in consumers:
                consumer.result()

        expected = {
            f"{worker}:{index}"
            for worker in range(producer_count)
            for index in range(messages_per_producer)
        }
        self.assertEqual(len(drained), len(expected))
        self.assertEqual(set(drained), expected)

    def test_06_parallel_web_api_workers_can_update_queue_and_stream_together(self):
        worker_count = 20
        operations_per_worker = 40

        def update_state(worker):
            for index in range(operations_per_worker):
                content = f"{worker}:{index}"
                self.state.queue_outbox_message(
                    worker, "127.0.0.1", 32000 + worker, content
                )
                self.state.add_message(worker, f"worker-{worker}", content)

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            list(pool.map(update_state, range(worker_count)))

        expected_count = worker_count * operations_per_worker
        queued = self.state.fetch_outbox_messages()
        messages = [
            message
            for stream in self.state.streams.values()
            for message in stream
        ]
        self.assertEqual(len(queued), expected_count)
        self.assertEqual(len(messages), expected_count)
        self.assertEqual(
            {item["content"] for item in queued},
            {item["text"] for item in messages},
        )

    def test_07_snapshot_reads_remain_well_formed_during_concurrent_writes(self):
        writer_count = 8
        writes_per_worker = 50

        def write_messages(worker):
            for index in range(writes_per_worker):
                self.state.add_message(worker + 1, f"worker-{worker}", str(index))

        def read_snapshots(_):
            for _ in range(100):
                snapshot = self.state.to_json_snapshot()
                self.assertIsInstance(snapshot["streams"], dict)
                for stream_messages in snapshot["streams"].values():
                    self.assertIsInstance(stream_messages, list)
                    for message in stream_messages:
                        self.assertEqual(
                            set(message),
                            {"sender", "text", "timestamp"},
                        )

        with ThreadPoolExecutor(max_workers=writer_count + 4) as pool:
            writers = [
                pool.submit(write_messages, worker)
                for worker in range(writer_count)
            ]
            readers = [pool.submit(read_snapshots, index) for index in range(4)]
            for future in writers + readers:
                future.result()

        snapshot = self.state.to_json_snapshot()
        self.assertEqual(
            sum(map(len, snapshot["streams"].values())),
            writer_count * writes_per_worker,
        )

    def test_08_outbox_fetch_during_message_writes_keeps_both_collections_consistent(self):
        worker_count = 12
        writes_per_worker = 60

        def update(worker):
            for index in range(writes_per_worker):
                content = f"{worker}:{index}"
                self.state.add_message(worker % 3, f"worker-{worker}", content)
                self.state.queue_outbox_message(
                    worker % 3, "127.0.0.1", 33000 + worker, content
                )

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(update, worker) for worker in range(worker_count)]
            for future in futures:
                future.result()

        snapshot = self.state.to_json_snapshot()
        queued = self.state.fetch_outbox_messages()
        saved_messages = [
            message
            for stream in snapshot["streams"].values()
            for message in stream
        ]
        expected_count = worker_count * writes_per_worker
        self.assertEqual(len(queued), expected_count)
        self.assertEqual(len(saved_messages), expected_count)
        self.assertEqual(
            {message["content"] for message in queued},
            {message["text"] for message in saved_messages},
        )

    def test_09_concurrent_snapshot_and_outbox_drain_preserve_final_state(self):
        worker_count = 10
        operations_per_worker = 50
        start = threading.Barrier(worker_count + 2)
        drained = []
        drained_lock = threading.Lock()

        def update(worker):
            start.wait()
            for index in range(operations_per_worker):
                content = f"{worker}:{index}"
                self.state.queue_outbox_message(
                    worker, "127.0.0.1", 34000 + worker, content
                )
                self.state.add_message(worker, f"worker-{worker}", content)

        def drain():
            start.wait()
            while True:
                batch = self.state.fetch_outbox_messages()
                if not batch:
                    if all(future.done() for future in writers):
                        break
                    time.sleep(0.001)
                    continue
                with drained_lock:
                    drained.extend(message["content"] for message in batch)

        def snapshot_repeatedly():
            start.wait()
            while not all(future.done() for future in writers):
                self.state.to_json_snapshot()

        with ThreadPoolExecutor(max_workers=worker_count + 2) as pool:
            writers = [
                pool.submit(update, worker)
                for worker in range(worker_count)
            ]
            consumer = pool.submit(drain)
            reader = pool.submit(snapshot_repeatedly)
            for writer in writers:
                writer.result()
            consumer.result()
            reader.result()

        expected = {
            f"{worker}:{index}"
            for worker in range(worker_count)
            for index in range(operations_per_worker)
        }
        messages = [
            message
            for stream in self.state.streams.values()
            for message in stream
        ]
        self.assertEqual(set(drained), expected)
        self.assertEqual(len(drained), len(expected))
        self.assertEqual(len(messages), len(expected))


if __name__ == "__main__":
    run_test_module(__name__)
