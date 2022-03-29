class Error:

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def to_json(self):
        return {
            "code": self.code,
            "message": self.message
        }
