import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


if __name__ == '__main__':
    from server.src.pedis_server import main

    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        print('exit')
