class GridCoreError(Exception):
    def __init__(self, message: str, code: str = "GRID_CORE_ERROR"):
        self.message = message
        self.code = code
        super().__init__(message)


class ValidationError(GridCoreError):
    def __init__(self, message: str):
        super().__init__(message, code="VALIDATION_ERROR")


class NotImplementedCapabilityError(GridCoreError):
    def __init__(self, message: str):
        super().__init__(message, code="NOT_IMPLEMENTED_CAPABILITY")


class ParseError(GridCoreError):
    def __init__(self, message: str):
        super().__init__(message, code="PARSE_ERROR")
