import os
import base64
from typing import Optional

# Thử import thư viện cryptography. Nếu thiếu, mặc định BÁO LỖI để không âm thầm
# rơi về XOR (không an toàn). Chỉ cho phép XOR khi đặt biến môi trường
# ALLOW_INSECURE_XOR=1 (dùng cho thử nghiệm).
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False

ALLOW_INSECURE_FALLBACK = os.environ.get("ALLOW_INSECURE_XOR") == "1"
DEFAULT_SECRET_KEY = b"HCMUT_CNET_2026_KEY!"


class CryptoEngine:


    def __init__(self, secret_key: Optional[bytes] = None):
        if not HAS_CRYPTOGRAPHY and not ALLOW_INSECURE_FALLBACK:
            raise RuntimeError(
                "Thiếu thư viện 'cryptography'. Chạy: python -m pip install -r requirements.txt"
            )
        if not HAS_CRYPTOGRAPHY:
            print("[!] CẢNH BÁO: đang dùng XOR dự phòng, KHÔNG an toàn.")

        # Khóa lấy từ tham số, nếu không có thì từ biến môi trường CNET_SECRET_KEY,
        # cuối cùng mới dùng khóa mặc định. Mọi client phải dùng CÙNG một khóa.
        if secret_key is None:
            env_key = os.environ.get("CNET_SECRET_KEY")
            secret_key = env_key.encode("utf-8") if env_key else DEFAULT_SECRET_KEY

        # Chuẩn hóa khóa về độ dài 16 bytes (128 bits) cho AES-128
        if len(secret_key) < 16:
            self.key = secret_key.ljust(16, b"#")
        else:
            self.key = secret_key[:16]

    def encrypt(self, plain_bytes: bytes) -> bytes:
        """Mã hóa mảng byte dữ liệu trước khi đóng gói vào Packet Header."""
        if not plain_bytes:
            return b""

        if HAS_CRYPTOGRAPHY:
            # Tạo Initialization Vector (IV) ngẫu nhiên 16 bytes
            iv = os.urandom(16)
            cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
            encryptor = cipher.encryptor()

            # PKCS7 Padding
            padder = padding.PKCS7(128).padder()
            padded_data = padder.update(plain_bytes) + padder.finalize()

            ciphertext = encryptor.update(padded_data) + encryptor.finalize()
            # Gói IV ở đầu ciphertext để bên nhận giải mã
            return iv + ciphertext
        else:
            # Fallback: Thuật toán đối xứng Multi-byte XOR kèm Salt 4 bytes
            salt = os.urandom(4)
            full_key = salt + self.key
            cipher_bytes = bytes([b ^ full_key[i % len(full_key)] for i, b in enumerate(plain_bytes)])
            return b"XOR:" + salt + cipher_bytes

    def decrypt(self, cipher_bytes: bytes) -> bytes:
        """Giải mã mảng byte Payload nhận được từ Packet sang dữ liệu gốc."""
        if not cipher_bytes:
            return b""

        if HAS_CRYPTOGRAPHY and not cipher_bytes.startswith(b"XOR:"):
            if len(cipher_bytes) < 16:
                raise ValueError("Ciphertext quá ngắn, thiếu IV của AES")

            iv = cipher_bytes[:16]
            encrypted_payload = cipher_bytes[16:]

            cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
            decryptor = cipher.decryptor()
            padded_data = decryptor.update(encrypted_payload) + decryptor.finalize()

            # Gỡ PKCS7 Padding
            unpadder = padding.PKCS7(128).unpadder()
            plain_data = unpadder.update(padded_data) + unpadder.finalize()
            return plain_data
        else:
            # Fallback decryption
            raw = cipher_bytes[4:] if cipher_bytes.startswith(b"XOR:") else cipher_bytes
            salt = raw[:4]
            actual_cipher = raw[4:]
            full_key = salt + self.key
            return bytes([b ^ full_key[i % len(full_key)] for i, b in enumerate(actual_cipher)])