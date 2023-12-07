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


# 암호화 함수
# 평문을 받아 AES_KEY와 IV를 이용해 암호화 후, base64로 한번 더 암호화
def encrypt_password(password: str) -> str:
    padder = padding.PKCS7(128).padder()
    password = padder.update(password.encode()) + padder.finalize()

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=cipher_backend)
    encryptor = cipher.encryptor()
    encrypted_password = encryptor.update(password) + encryptor.finalize()

    encrypted_password_base64 = base64.urlsafe_b64encode(encrypted_password).decode()
    return encrypted_password_base64


# 복호화 함수
# encrypt 후 base64로 저장된 데이터를 MySQL로 보내기 위해 평문으로 복호화
def decrypt_password(encrypted_password_str: str) -> str:
    try:
        encrypted_password_bytes = base64.urlsafe_b64decode(encrypted_password_str)

        cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=cipher_backend)
        decryptor = cipher.decryptor()
        decrypted_password = decryptor.update(encrypted_password_bytes) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        decrypted_password = unpadder.update(decrypted_password) + unpadder.finalize()

        return decrypted_password.decode()
    except Exception as e:
        print(f"Error in decrypt_password: {e}")
        # 에러 발생 시 빈 문자열 반환 대신, None 반환으로 변경하거나 적절한 기본값 설정
        return None


