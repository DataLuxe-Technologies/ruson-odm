class Session:
    def __init__(self, binding_session):
        self.__binding_session = binding_session

    def _get_session(self):
        return self.__binding_session
