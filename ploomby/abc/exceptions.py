class UnregisteredHandler(Exception):
    pass


class NoConnectionError(Exception):
    pass


class NoModelProvidedError(Exception):
    pass


class NoMessageKeyError(Exception):
    pass


class ConsumerAlreadyRegistered(Exception):
    pass
