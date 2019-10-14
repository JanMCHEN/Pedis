"""
RESP协议部分实现
"""


class Resp:
    @staticmethod
    def encode(text, opt='*'):
        """
        主要的编码方法，用于数据传送打包
        :param text: 可以未多种类型，bool表示操作成功和错误发生，list表示需要打包的数据
        :param opt: 默认‘*’即已数组形式发送数据，‘$'表示传送单个字符串，仅在text未list有效
        :return: 字节数据
        """
        if isinstance(text, bool):
            if text:
                return Resp.ok()
            return Resp.error('Operation against a key holding the wrong kind of value', 'WRONGTYPE')

        if isinstance(text, int):
            return Resp.integer(text)

        if isinstance(text, str):
            text = [text]

        if not text:
            return Resp.ok('(empty list or set)')
        res = []
        if opt == '*':
            res.append(f'*{len(text)}\r\n')

        for tt in text:
            if tt is None:
                res.append('$-1\r\n')
                continue
            res.append(f'${len(tt.encode())}\r\n')
            res.append(tt+'\r\n')
        return ''.join(res).encode()

    @staticmethod
    def decode(byte: bytes):
        """解码"""
        return byte.decode().split('\r\n')

    @staticmethod
    def error(info, op='ERR'):
        """错误信息"""
        return f'-{op} {info}\r\n'.encode()

    @staticmethod
    def ok(s='OK'):
        """用于传输简单字符串"""
        return f'+{s}\r\n'.encode()

    @staticmethod
    def integer(num):
        """传输整数"""
        return f':{num}\r\n'.encode()


if __name__ == '__main__':
    a = Resp.encode('set\n a b'.split(' '))
    print(a, Resp.decode(a))


