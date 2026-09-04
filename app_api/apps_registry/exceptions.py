"""
Custom exceptions for the applications registry domain.

These exceptions provide semantic meaning and make error handling in services
more explicit and testable, following Open/Closed Principle.
"""


class ApplicationException(Exception):
    """
    Base exception for all application-related errors.
    
    Provides a clear semantic root for domain exceptions.
    """
    pass


class ApplicationAlreadyExistsException(ApplicationException):
    """
    Raised when attempting to create an application with an existing package_name.
    
    This is a predictable, domain-specific error (not a generic ValueError).
    """
    def __init__(self, package_name: str):
        self.package_name = package_name
        super().__init__(
            f"Application with package name '{package_name}' already exists."
        )


class ApplicationNotFoundException(ApplicationException):
    """
    Raised when attempting to access an application that doesn't exist.
    
    Maps to HTTP 404 Not Found.
    """
    def __init__(self, app_id: int = None, package_name: str = None):
        self.app_id = app_id
        self.package_name = package_name
        
        if app_id:
            message = f"Application with ID {app_id} not found."
        elif package_name:
            message = f"Application with package name '{package_name}' not found."
        else:
            message = "Application not found."
        
        super().__init__(message)


class InvalidApplicationDataException(ApplicationException):
    """
    Raised when provided application data is semantically invalid.
    
    Different from serializer validation errors (which are caught earlier).
    Use this for business-logic validation that depends on system state.
    """
    def __init__(self, message: str):
        super().__init__(message)


class ApplicationInactiveException(ApplicationException):
    """
    Raised when attempting to operate on an inactive application.
    
    Useful for operations that should only apply to active applications.
    """
    def __init__(self, package_name: str = None):
        if package_name:
            message = f"Application '{package_name}' is inactive."
        else:
            message = "Application is inactive."
        
        super().__init__(message)
