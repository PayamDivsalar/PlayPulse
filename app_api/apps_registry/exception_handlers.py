"""
Centralized exception handling for the API.

Custom exception handler that maps domain exceptions to appropriate HTTP status codes.
This follows DRF best practices for consistent error responses across all endpoints.
"""

from rest_framework.views import exception_handler
from rest_framework.response import Response
from rest_framework import status

from .exceptions import (
    ApplicationAlreadyExistsException,
    ApplicationNotFoundException,
    InvalidApplicationDataException,
)


def custom_exception_handler(exc, context):
    """
    Custom exception handler for domain-specific exceptions.
    
    Maps application domain exceptions to appropriate HTTP status codes
    and delegates standard exceptions to DRF's default handler.
    
    Exception mappings:
    - ApplicationAlreadyExistsException → 409 Conflict
    - ApplicationNotFoundException → 404 Not Found
    - InvalidApplicationDataException → 400 Bad Request
    - All others → passed to DRF's default handler
    
    Args:
        exc: The exception that was raised
        context: The context dict passed to the exception handler
        
    Returns:
        Response object with appropriate status code and error message
    """
    
    # Handle application-specific exceptions
    if isinstance(exc, ApplicationAlreadyExistsException):
        return Response(
            {'error': str(exc)},
            status=status.HTTP_409_CONFLICT,
        )
    
    if isinstance(exc, ApplicationNotFoundException):
        return Response(
            {'error': str(exc)},
            status=status.HTTP_404_NOT_FOUND,
        )
    
    if isinstance(exc, InvalidApplicationDataException):
        return Response(
            {'error': str(exc)},
            status=status.HTTP_400_BAD_REQUEST,
        )
    
    # Fall back to DRF's default exception handler for standard exceptions
    # (ValidationError, PermissionDenied, NotFound, etc.)
    response = exception_handler(exc, context)
    
    return response
