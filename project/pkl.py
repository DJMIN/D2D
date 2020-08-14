import hashlib
import time


def md5s(s):
    md5 = hashlib.md5()
    md5.update(s.encode())
    return md5.hexdigest()


def encode(s):
    s = s + 'password' + str(time.time()//10)
    check = md5s(s)
    print(check)
    qianming = md5s(check)
    return check, qianming


# 客户端
flag_o = 'ECB'
res = encode(flag_o)


# 服务端
for flag in ["ECB", "GCM"]:
    print(encode(flag) == res)
