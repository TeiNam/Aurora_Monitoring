import os
import base64
from cryptography.hazmat.primitives.ciphers import algorithms, modes, Cipher
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from dotenv import load_dotenv

load_dotenv()

# 암복호화에서 사용하는 .env에 저장된 AES_KEY, AES_IV
# AES_KEY 32바이트의 문자를 base64로 인코딩해서 저장해야 읽어올 수 있다.
# AES_IV 16바이트의 문자를 base64로 인코딩해서 저장해야 읽어올 수 있다.
AES_KEY = base64.urlsafe_b64decode(os.getenv("AES_KEY"))
AES_IV = base64.urlsafe_b64decode(os.getenv("AES_IV"))

# 복호화 된 AES_KEY, IV의 길이 확인
AES_KEY_LENGTH = 32
AES_IV_LENGTH = 16

if len(AES_KEY) != AES_KEY_LENGTH:
    raise ValueError(f"AES_KEY must be {AES_KEY_LENGTH} bytes long.")
elif len(AES_IV) != AES_IV_LENGTH:
    raise ValueError(f"AES_IV must be {AES_IV_LENGTH} bytes long.")

cipher_backend = default_backend()


def encrypt_password(password: str) -> str:
    padder = padding.PKCS7(128).padder()
    password = padder.update(password.encode()) + padder.finalize()

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=cipher_backend)
    encrypt = cipher.encryptor()
    encrypted_password = encrypt.update(password) + encrypt.finalize()

    encrypted_password_base64 = base64.urlsafe_b64encode(encrypted_password).decode()
    return encrypted_password_base64


def decrypt_password(encrypted_password_str: str) -> str:
    encrypted_password_bytes = base64.urlsafe_b64decode(encrypted_password_str)

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=cipher_backend)
    decrypt = cipher.decryptor()
    decrypted_password = decrypt.update(encrypted_password_bytes) + decrypt.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    decrypted_password = unpadder.update(decrypted_password) + unpadder.finalize()

    return decrypted_password.decode()
