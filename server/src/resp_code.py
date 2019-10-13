class Resp:
    @staticmethod
    def encode(text, opt='*'):
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
        return byte.decode().split('\r\n')

    @staticmethod
    def error(info, op='ERR'):
        return f'-{op} {info}\r\n'.encode()

    @staticmethod
    def ok(s='OK'):
        return f'+{s}\r\n'.encode()

    @staticmethod
    def integer(num):
        return f':{num}\r\n'.encode()


if __name__ == '__main__':
    a = Resp.encode('set\n a b'.split(' '))
    print(a, Resp.decode(a))


