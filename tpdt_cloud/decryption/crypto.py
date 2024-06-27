from Crypto.Cipher import AES
import base64
import hashlib


class AESCrypt(object):
    def __init__(self):
        secret_key = 'a4454d3f-c202-4c4d-8839-f58317841129'
        self.BLOCK_SIZE = 16
        self.key = hashlib.sha256(secret_key.encode()).digest()

    def pkcs7_pad(self, s):
        length = self.BLOCK_SIZE - (len(s) % self.BLOCK_SIZE)
        s += bytes([length]) * length
        return s

    def encrypt(self, plain_text):
        if (plain_text is None) or (len(plain_text) == 0):
            raise ValueError('input text cannot be null or empty set')

        plain_bytes = plain_text.encode('utf-8')
        raw = self.pkcs7_pad(plain_bytes)
        cipher = AES.new(self.key, AES.MODE_ECB)
        cipher_bytes = cipher.encrypt(raw)
        cipher_text = self.base64_encode(cipher_bytes)
        return cipher_text

    def base64_encode(self, bytes_data):
        """
        加base64
        :type bytes_data: byte
        :rtype 返回类型: string
        """
        return (base64.urlsafe_b64encode(bytes_data)).decode()




