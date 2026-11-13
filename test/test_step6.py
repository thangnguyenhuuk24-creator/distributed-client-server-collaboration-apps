"""Step 6: UDP file chunking, reassembly, and integrity checks.

Run with: python -m test.test_step6
"""

import hashlib
import json
import os
import shutil
import tempfile
import unittest

from network.streaming import (
    MAX_CHUNK_SIZE,
    FileTransferManager,
    StreamManager,
)
from test.net_helpers import Client, start_server, wait_until
from test.test_report import run_step_tests


class TestStep6ReliableFileTransfer(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="step6_")
        self.output_dir = os.path.join(self.tmp, "assembled")
        self.streams = StreamManager(self.output_dir)
        self.server = start_server()
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.stop()
        self.server.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_source(self, filename, content):
        path = os.path.join(self.tmp, filename)
        with open(path, "wb") as file:
            file.write(content)
        return path

    def make_pair(self, suffix=""):
        alice_name = f"alice{suffix}"
        bob_name = f"bob{suffix}"
        alice = Client(
            self.server, alice_name, os.path.join(self.tmp, alice_name)
        )
        bob = Client(self.server, bob_name, os.path.join(self.tmp, bob_name))
        self.clients.extend((alice, bob))
        self.assertTrue(wait_until(
            lambda: alice.peer(bob_name) and bob.peer(alice_name)
        ))
        return alice, bob

    def transfer(self, content, filename="payload.bin", stream_id=600, suffix=""):
        alice, bob = self.make_pair(suffix)
        source = self.make_source(filename, content)
        peer = alice.peer(bob.username)
        transfer_id = f"transfer-{stream_id}"
        alice.engine.send_file(
            peer["ip"],
            peer["port"],
            stream_id,
            source,
            transfer_id=transfer_id,
            target_session_id=peer["session_id"],
        )
        self.assertTrue(wait_until(
            lambda: bob.state.file_transfers.get(transfer_id, {}).get("status")
            == "COMPLETED"
        ))
        transfer = bob.engine.stream_manager.incoming_transfers[stream_id]
        with open(transfer["saved_path"], "rb") as file:
            return file.read(), bob.state.file_transfers[transfer_id]

    def test_01_file_is_split_into_chunks_within_udp_payload_limit(self):
        source = self.make_source("large.bin", bytes(range(256)) * 30)

        metadata, chunks = FileTransferManager.prepare_file_for_sending(source)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(0 < len(chunk) <= MAX_CHUNK_SIZE for chunk in chunks))
        self.assertEqual(sum(map(len, chunks)), metadata["filesize"])

    def test_02_file_metadata_contains_size_chunk_count_and_sha256(self):
        content = b"metadata test" * 300
        source = self.make_source("metadata.bin", content)

        metadata, chunks = FileTransferManager.prepare_file_for_sending(source)

        self.assertEqual(metadata["filesize"], len(content))
        self.assertEqual(metadata["total_chunks"], len(chunks))
        self.assertEqual(metadata["sha256"], hashlib.sha256(content).hexdigest())

    def test_03_file_metadata_serializes_and_decodes_without_data_loss(self):
        metadata = {
            "filename": "báo_cáo.bin",
            "filesize": 42,
            "total_chunks": 1,
            "sha256": "a" * 64,
        }

        decoded = FileTransferManager.decode_file_init(
            FileTransferManager.encode_file_init(metadata)
        )

        self.assertEqual(decoded, metadata)
        self.assertEqual(json.loads(FileTransferManager.encode_file_init(metadata)), metadata)

    def test_04_stream_manager_reassembles_multiple_chunks_in_order(self):
        content = bytes(range(256)) * 15
        chunks = [
            content[offset:offset + MAX_CHUNK_SIZE]
            for offset in range(0, len(content), MAX_CHUNK_SIZE)
        ]
        metadata = {
            "filename": "ordered.bin",
            "filesize": len(content),
            "total_chunks": len(chunks),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        self.streams.register_incoming_file(604, metadata)

        for chunk in chunks:
            received, total, completed = self.streams.append_chunk(604, chunk)

        saved_path = self.streams.incoming_transfers[604]["saved_path"]
        with open(saved_path, "rb") as file:
            self.assertEqual(file.read(), content)
        self.assertEqual((received, total, completed), (len(chunks), len(chunks), True))

    def test_05_hash_mismatch_prevents_corrupted_file_from_being_saved(self):
        self.streams.register_incoming_file(605, {
            "filename": "corrupt.bin",
            "filesize": 3,
            "total_chunks": 1,
            "sha256": hashlib.sha256(b"good").hexdigest(),
        })

        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            self.streams.append_chunk(605, b"bad")
        self.assertFalse(os.path.exists(os.path.join(self.output_dir, "corrupt.bin")))

    def test_06_empty_file_metadata_has_no_data_chunks(self):
        source = self.make_source("empty.bin", b"")

        metadata, chunks = FileTransferManager.prepare_file_for_sending(source)

        self.assertEqual(metadata["filesize"], 0)
        self.assertEqual(metadata["total_chunks"], 0)
        self.assertEqual(chunks, [])

    def test_07_same_filename_is_saved_without_overwriting_existing_file(self):
        content = b"same name"
        metadata = {
            "filename": "duplicate.bin",
            "filesize": len(content),
            "total_chunks": 1,
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        first_path = os.path.join(self.output_dir, "duplicate.bin")
        with open(first_path, "wb") as file:
            file.write(b"existing")

        self.streams.register_incoming_file(607, dict(metadata))
        self.streams.append_chunk(607, content)

        saved_path = self.streams.incoming_transfers[607]["saved_path"]
        self.assertEqual(saved_path, os.path.join(self.output_dir, "duplicate_1.bin"))
        with open(first_path, "rb") as file:
            self.assertEqual(file.read(), b"existing")

    def test_08_small_file_is_transferred_over_the_udp_relay_intact(self):
        content = b"small transfer over UDP"

        received, progress = self.transfer(content, "small.bin", 608)

        self.assertEqual(received, content)
        self.assertEqual(progress["progress_percent"], 100.0)

    def test_09_large_multichunk_file_is_reassembled_over_the_udp_relay(self):
        content = bytes(range(256)) * 40

        received, progress = self.transfer(content, "multi.bin", 609)

        self.assertEqual(received, content)
        self.assertGreater(progress["total_chunks"], 1)
        self.assertEqual(progress["received_chunks"], progress["total_chunks"])

    def test_10_empty_and_binary_files_transfer_without_corruption(self):
        empty, _ = self.transfer(b"", "empty-transfer.bin", 610)
        binary_content = b"\x00\xff\x10" + bytes(range(256)) * 8
        binary, _ = self.transfer(
            binary_content, "binary-transfer.bin", 611, suffix="2"
        )

        self.assertEqual(empty, b"")
        self.assertEqual(binary, binary_content)


if __name__ == "__main__":
    run_step_tests(__name__)
