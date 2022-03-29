import base64
import html
import typing
import logging

import Crypto.Signature.PKCS1_v1_5 as sign_PKCS1_v1_5  # 用于签名/验签

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5  # 用于加密
from Crypto import Random
from Crypto.Hash import SHA256

logger = logging.getLogger('ras')

"""
PKCS#1、PKCS#5、PKCS#7区别
PKCS5：PKCS5是8字节填充的，即填充一定数量的内容，使得成为8的整数倍，而填充的内容取决于需要填充的数目。例如，串0x56在经过PKCS5填充之后会成为0x56 0x07 0x07 0x07 0x07 0x07 0x07 0x07因为需要填充7字节，因此填充的内容就是7。当然特殊情况下，如果已经满足了8的整倍数，按照PKCS5的规则，仍然需要在尾部填充8个字节，并且内容是0x08,目的是为了加解密时统一处理填充。
PKCS7：PKCS7与PKCS5的区别在于PKCS5只填充到8字节，而PKCS7可以在1-255之间任意填充。
PKCS1：在进行RSA运算时需要将源数据D转化为Encryption block（EB）。其中pkcs1padding V1.5的填充模式按照以下方式进行
(1) EB = 00+BT+PS+00+D
EB：为填充后的16进制加密数据块，长度为1024/8 = 128字节（密钥长度1024位的情况下）
00：开头为00，是一个保留位
BT：用一个字节表示，在目前的版本上，有三个值00、01、02，如果使用公钥操作，BT为02，如果用私钥操作则可能为00或01
PS：填充位，PS = k － 3 － D 个字节，k表示密钥的字节长度，如果我们用1024bit的RSA密钥，k=1024/8=128字节，D表示明文数据D的字节长度，如果BT为00，则PS全部为00，如果BT为01，则PS全部为FF，如果BT为02，PS为随机产生的非0x00的字节数据。
00：在源数据D前一个字节用00表示
D：实际源数据
公式(1)整个EB的长度等于密钥的长度。
注意：对于BT为00的，数据D中的数据就不能以00字节开头，要不然会有歧义，因为这时候你PS填充的也是00，就分不清哪些是填充数据哪些是明文数据了，但如果你的明文数据就是以00字节开头怎么办呢？对于私钥操作，你可以把BT的值设为01，这时PS填充的FF，那么用00字节就可以区分填充数据和明文数据，对于公钥操作，填充的都是非00字节，也能够用00字节区分开。如果你使用私钥加密，建议你BT使用01，保证了安全性。
对于BT为02和01的，要保证PS至少要有八个字节长度
因为EB = 00+BT+PS+00+D = k
所以D <= k － 3 － 8，所以当我们使用128字节密钥对数据进行加密时，明文数据的长度不能超过128 － 11 = 117字节
当RSA要加密数据大于(k－11)字节时怎么办呢？把明文数据按照D的最大长度分块，然后逐块加密，最后把密文拼起来就行。
"""


class RSACtrl:
    def __init__(self, privatekey_path='tmp_privatekey.pem', publickey_path="tmp_publickey.pem", load_key_file=True):
        # 实现RSA 非对称加解密
        self.private_key = None  # 私钥
        self.public_key = None  # 公钥

        self.pri_obj = None  # 私钥obj
        self.pub_obj = None  # 公钥obj
        self.cert = None  # 证书

        self.private_key_max_handle_msg_len = 0
        self.public_key_max_handle_msg_len = 0

        self.privatekey_path = privatekey_path  # 私钥
        self.publickey_path = publickey_path  # 公钥
        if load_key_file:
            self.load_key_file()

    @staticmethod
    def generate_key(bit=2048):
        # 手动生成一个密钥对(项目中的密钥对一般由开发来生成)，生成密钥对的时候，可以指定生成的长度，一般推荐使用1024bit，
        # 1024bit的rsa公钥，最多只能加密117byte的数据，数据流超过这个数则需要对数据分段加密，
        # 目前1024bit长度的密钥已经被证明了不够安全，尽量使用2048bit长度的密钥，
        # 2048bit长度密钥最多能加密245byte长度的数据计算长度公式：密钥长度 / 8 - 11 = 最大加密量(单位bytes)下面生成一对2048bit的密钥：
        # x = RSA.generate(2048)
        x = RSA.generate(bit, Random.new().read)  # 也可以使用伪随机数来辅助生成
        privatekey = x.export_key()  # 私钥
        publickey = x.publickey().export_key()  # 公钥
        logger.debug(f"生成公私钥对：{type(privatekey)} {privatekey} {type(publickey)} {publickey}")
        return privatekey, publickey

    def generate_key_and_load(self, bit=2048):
        self.private_key, self.public_key = self.generate_key(bit)
        return self

    def import_public_key(self, publickey):
        self.public_key = publickey
        self.pub_obj = PKCS1_v1_5.new(RSA.importKey(self.public_key))
        self.public_key_max_handle_msg_len = getattr(self.pub_obj, '_key').size_in_bytes()
        return publickey

    def import_private_key(self, private_key, auto_public=True):
        pri_obj = RSA.importKey(private_key)
        self.cert = pri_obj.export_key("DER")  # 生成证书 -- 它和私钥是唯一对应的
        self.private_key = pri_obj.export_key()
        self.pri_obj = PKCS1_v1_5.new(pri_obj)
        # self.pri_obj = PKCS1_v1_5.new(pri_obj)
        self.private_key_max_handle_msg_len = getattr(self.pri_obj, '_key').size_in_bytes() - 11
        if auto_public:
            # 通过私钥生成公钥  (公钥不会变 -- 用于只知道私钥的情况)
            self.import_public_key(pri_obj.publickey().export_key())

        return pri_obj

    @staticmethod
    def get_data_form_file(path):
        with open(path, 'rb') as x:
            return x.read()

    def import_public_key_from_file(self, path="publickey.pem"):
        publickey = self.get_data_form_file(path)
        self.import_public_key(publickey)
        return publickey

    def import_private_key_obj_from_file(self, path='privatekey.pem', auto_public=True):
        # 从文件导入密钥
        private_key = self.get_data_form_file(path)
        self.privatekey_path = path
        return self.import_private_key(private_key, auto_public)

    def generate_key_and_save_file_and_load(self, bit=2048):
        """
        2048位RSA密钥生成需要2秒，4096需要15秒
        """
        self.generate_key_and_load(bit)
        self.save_key_file()
        self.load_key_file()
        return self

    def save_key_file(self):
        # 写入文件
        with open(self.privatekey_path, "wb") as x:
            x.write(self.private_key)
        with open(self.publickey_path, "wb") as x:
            x.write(self.public_key)
        return self

    def load_key_file(self):
        if self.publickey_path:
            publickey = self.import_public_key_from_file()
        else:
            publickey = None
        if self.privatekey_path:
            self.import_private_key_obj_from_file()
        if publickey != self.public_key:
            raise ValueError(f'公钥文件【{self.publickey_path}】和私钥文件【{self.privatekey_path}】不匹配')
        return self

    """
    ① 使用公钥 - 私钥对信息进行"加密" + “解密”
    作用：对信息进行公钥加密，私钥解密。 应用场景：
    A想要加密传输一份数据给B，担心使用对称加密算法易被他人破解（密钥只有一份，
    一旦泄露，则数据泄露），故使用非对称加密。
    信息接收方可以生成自己的秘钥对，即公私钥各一个，然后将公钥发给他人，
    私钥自己保留。

    A使用公钥加密数据，然后将加密后的密文发送给B，B再使用自己的私钥进行解密，
    这样即使A的公钥和密文均被第三方得到，
    第三方也要知晓私钥和加密算法才能解密密文，大大降低数据泄露风险。
    """

    def encrypt_with_rsa(self, plain_text):
        # 先公钥加密
        secret_byte_obj = self.pub_obj.encrypt(plain_text.encode())
        return secret_byte_obj

    def decrypt_with_rsa(self, secret_byte_obj):
        if isinstance(secret_byte_obj, str):
            secret_byte_obj = secret_byte_obj.encode()
        # 后私钥解密
        _byte_obj = self.pri_obj.decrypt(secret_byte_obj, Random.new().read)
        plain_text = _byte_obj.decode()
        return plain_text

    def encrypt_with_rsa_split_base64(self, msg, b64=True) -> str:
        """
        公钥加密 base64进行编码
        """
        # 分段加密
        encrypt_text = []
        if isinstance(msg, str):
            msg = msg.encode("utf-8")
        # 对数据进行分段加密
        for i in range(0, len(msg), self.private_key_max_handle_msg_len):
            cont = msg[i:i + self.private_key_max_handle_msg_len]
            encrypt_text.append(self.pub_obj.encrypt(cont))
        # 分段加密完进行拼接
        cipher_text = b''.join(encrypt_text)
        if b64:
            # base64进行编码
            cipher_text = base64.b64encode(cipher_text)
        return cipher_text.decode()

    def decrypt_with_rsa_split_base64(self, secret_byte_obj, encoding='utf-8', b64=True) -> str:
        if isinstance(secret_byte_obj, str):
            secret_byte_obj = secret_byte_obj.encode()
        if b64:
            secret_byte_obj = base64.b64decode(secret_byte_obj)

        # 分段加密
        encrypt_bytes = []
        msg_len = self.public_key_max_handle_msg_len
        # 对数据进行分段加密
        for i in range(0, len(secret_byte_obj), msg_len):
            cont = secret_byte_obj[i:i + msg_len]
            # 后私钥解密
            _byte_obj = self.pri_obj.decrypt(cont, Random.new().read)

            encrypt_bytes.append(_byte_obj)
        # 分段加密完进行拼接
        encrypt_byte = b''.join(encrypt_bytes)
        try:
            plain_text = encrypt_byte.decode(encoding)
        except Exception as ex:
            logger.warning(f'私钥解密 [ERROR] {ex}: {encoding.__repr__()}')
            plain_text = encrypt_byte.decode(encoding, errors='xmlcharrefreplace')
            plain_text = html.unescape(plain_text)
        return plain_text

    def to_sign_with_private_key(self, msg: typing.Union[str, bytes], b64=True) -> str:
        # 私钥签名
        signer_pri_obj = sign_PKCS1_v1_5.new(RSA.importKey(self.private_key))
        rand_hash = SHA256.new()
        if isinstance(msg, str):
            msg = msg.encode()
        rand_hash.update(msg)
        signature = signer_pri_obj.sign(rand_hash)
        if b64:
            signature = base64.b64encode(signature)
        return signature.decode()

    def to_verify_with_public_key(
            self, signature: typing.Union[str, bytes], msg: typing.Union[str, bytes], b64=True) -> bool:
        # 公钥验签
        verifier = sign_PKCS1_v1_5.new(RSA.importKey(self.public_key))
        _rand_hash = SHA256.new()
        if isinstance(msg, str):
            msg = msg.encode()
        _rand_hash.update(msg)
        try:
            if isinstance(signature, str):
                signature = signature.encode()
            if b64:
                signature = base64.b64decode(signature)
            verifier.verify(_rand_hash, signature)
            verify = True
        except ValueError as ex:
            if str(ex) == 'Invalid signature':
                verify = False
            else:
                raise ex
        return verify  # true / false

    def encode(self, msg: typing.Union[str, bytes], b64=True) -> str:
        """
        公钥加密
         str分段后 加密 join base64编码 返回str
        """
        return self.encrypt_with_rsa_split_base64(msg, b64=b64)

    def decode(self, secret: typing.Union[str, bytes], encoding='utf-8', b64=True) -> str:
        """
        私钥解密
         str base64解码 分段解密 join 返回str
        """
        return self.decrypt_with_rsa_split_base64(secret, encoding=encoding, b64=b64)

    def sign(self, msg: typing.Union[str, bytes], b64=True) -> str:
        """
        私钥签名
        """
        return self.to_sign_with_private_key(msg, b64=b64)

    def verify(self, signature: typing.Union[str, bytes], msg: typing.Union[str, bytes], b64=True) -> bool:
        """
        公钥验签
        """
        return self.to_verify_with_public_key(signature, msg, b64=b64)

    def check(self):
        """
        检测类函数方法以及公私密钥对可用性
        """
        self.verifier_with_signature(print)
        self.verifier_without_signature(print)

    def verifier_without_signature(self, logger_info: typing.Callable = logging.info):
        # 加解密验证
        text = "I love CA!"
        assert text == self.decrypt_with_rsa(self.encrypt_with_rsa(text))
        logger_info("rsa 加/解密验证 success！")

    def verifier_with_signature(self, logger_info: typing.Callable = logging.info):
        # 签名/验签
        text = "I love CA!"
        assert self.to_verify_with_public_key(self.to_sign_with_private_key(text), text)
        logger_info("rsa 签名/验签 success!")


if __name__ == '__main__':
    # __data = ''.join(str(i)+' ' for i in range(10000))
    __data = """
    ① 使用 公钥 - 私钥 对 信息进行 "加密" + “解密”
    作用：对信息进行公钥加密，私钥解密。 应用场景：
    A想要加密传输一份数据给B，担心使用对称加密算法易被他人破解（密钥只有一份，
    一旦泄露，则数据泄露），故使用非对称加密。
    信息接收方可以生成自己的秘钥对，即公私钥各一个，然后将公钥发给他人，
    私钥自己保留。

    A使用公钥加密数据，然后将加密后的密文发送给B，B再使用自己的私钥进行解密，
    这样即使A的公钥和密文均被第三方得到，
    第三方也要知晓私钥和加密算法才能解密密文，大大降低数据泄露风险。

    ② 使用 私钥 - 公钥 对 信息进行 "签名" + “验签”

    作用：对解密后的文件的完整性、真实性进行验证（繁琐但更加保险的做法，很少用到）
    应用场景： A有一私密文件欲加密后发送给B，又担心因各种原因导致B收到并解密后的文件并非完整、真实的原文件（可能被篡改或丢失一部分），所以A在发送前对原文件进行签名，将[签名和密文]一同发送给B让B收到后用做一下文件的 [解密 + 验签],
    均通过后-方可证明收到的原文件的真实性、完整性。

    如果是加密的同时又要签名，这个时候稍微有点复杂。
    1、发送者和接收者需要各持有一对公私钥，也就是4个钥匙。
    2、接收者的公私钥用于机密信息的加解密
    3、发送者的公私钥用于机密信息的签名/验签
    4、接收者和发送者都要提前将各自的[公钥]告知对方。
    """
    rsa_ctrl = RSACtrl()
    rsa_ctrl.check()

    __mi_wen = rsa_ctrl.encode(__data)
    print(__mi_wen)

    __ming_wen = rsa_ctrl.decode(__mi_wen)
    print(__ming_wen)

    __signature = rsa_ctrl.sign(__mi_wen)
    print(__signature)

    __is_ming_wen = rsa_ctrl.verify(__signature, __ming_wen)
    print(__is_ming_wen)
