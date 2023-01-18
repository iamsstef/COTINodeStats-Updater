class COTI:
    def __init__(self):
        self.TESTNET = self.TESTNET_()
        self.MAINNET = self.MAINNET_()

    class TESTNET_:
        def __init__(self):
            return None
        def __str__(self):
            return 'TESTNET'

    class MAINNET_:
        def __init__(self):
            return None
        def __str__(self):
            return 'MAINNET'

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class SyncStatus:
    Unsync = 0
    Sync = 1
    Unkown = 2

class CustomSSLError(Exception):
    pass