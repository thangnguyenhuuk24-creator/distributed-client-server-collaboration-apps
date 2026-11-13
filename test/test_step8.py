"""Step 8: beginner-friendly checks for payload encryption.

Run this file with:
    python -m test.test_step8

Example result:
    Result: 10/10 tests passed

When the ``cryptography`` package is installed, the app uses AES-CBC.
Otherwise these checks exercise the app's documented XOR fallback.
"""

import unittest

from core.crypto import CryptoEngine, HAS_CRYPTOGRAPHY


class TestStep8Encryption(unittest.TestCase):
    """Check that encrypted bytes can be safely recovered by the app."""

    def setUp(self) -> None:
        self.crypto = CryptoEngine()

    def test_01_empty_message_stays_empty(self) -> None:
        """Encrypting an empty message returns an empty result."""
        self.assertEqual(self.crypto.encrypt(b""), b"")
        self.assertEqual(self.crypto.decrypt(b""), b"")

    def test_02_encrypting_text_does_not_expose_the_original_text(self) -> None:
        """The bytes sent over the network differ from the original message."""
        message = b"Hello from the LAN"

        encrypted = self.crypto.encrypt(message)

        self.assertNotEqual(encrypted, message)
        self.assertNotIn(message, encrypted)

    def test_03_encrypted_text_can_be_read_again(self) -> None:
        """A recipient with the same key can recover the original text."""
        message = "Tin nhắn tiếng Việt: xin chào".encode("utf-8")

        recovered = self.crypto.decrypt(self.crypto.encrypt(message))

        self.assertEqual(recovered, message)

    def test_04_binary_file_bytes_survive_encryption(self) -> None:
        """Encryption also preserves bytes that are not readable text."""
        file_chunk = bytes(range(256)) + b"\x00\xff\x10"

        recovered = self.crypto.decrypt(self.crypto.encrypt(file_chunk))

        self.assertEqual(recovered, file_chunk)

    def test_05_large_payload_can_be_encrypted_and_recovered(self) -> None:
        """A larger payload is not limited to one cipher block."""
        large_payload = (b"LAN collaboration data-" * 500) + bytes(range(128))

        recovered = self.crypto.decrypt(self.crypto.encrypt(large_payload))

        self.assertEqual(recovered, large_payload)

    def test_06_encrypting_the_same_data_twice_uses_random_bytes(self) -> None:
        """A random IV or salt makes repeated ciphertexts different."""
        message = b"same content, two encryption attempts"

        first_encryption = self.crypto.encrypt(message)
        second_encryption = self.crypto.encrypt(message)

        self.assertNotEqual(first_encryption, second_encryption)
        self.assertEqual(self.crypto.decrypt(first_encryption), message)
        self.assertEqual(self.crypto.decrypt(second_encryption), message)

    def test_07_another_engine_with_the_same_key_can_decrypt(self) -> None:
        """Two clients configured with the same key can exchange encrypted data."""
        sender = CryptoEngine(b"shared-LAN-test-key")
        receiver = CryptoEngine(b"shared-LAN-test-key")
        message = b"Message from one client to another"

        recovered = receiver.decrypt(sender.encrypt(message))

        self.assertEqual(recovered, message)

    def test_08_short_keys_are_padded_to_aes_key_length(self) -> None:
        """The app converts short configured keys into a 16-byte key."""
        crypto = CryptoEngine(b"short")

        self.assertEqual(len(crypto.key), 16)
        self.assertEqual(crypto.key, b"short###########")

    def test_09_long_keys_are_trimmed_to_aes_key_length(self) -> None:
        """The app uses the first 16 bytes of a longer configured key."""
        crypto = CryptoEngine(b"0123456789abcdefghijklmnop")

        self.assertEqual(len(crypto.key), 16)
        self.assertEqual(crypto.key, b"0123456789abcdef")

    def test_10_ciphertext_matches_the_active_crypto_mode(self) -> None:
        """AES output has a random IV and blocks; fallback output has its marker."""
        encrypted = self.crypto.encrypt(b"check the encryption mode")

        if HAS_CRYPTOGRAPHY:
            ciphertext = encrypted[16:]
            self.assertGreaterEqual(len(encrypted), 32)
            self.assertEqual(len(ciphertext) % 16, 0)
        else:
            self.assertTrue(encrypted.startswith(b"XOR:"))
            self.assertEqual(self.crypto.decrypt(encrypted), b"check the encryption mode")


if __name__ == "__main__":
    from test.test_report import run_step_tests

    run_step_tests(__name__)
