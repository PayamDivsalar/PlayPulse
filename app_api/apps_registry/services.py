"""
Business logic layer for Application management.

This module contains ApplicationService which encapsulates all business rules
and operations related to applications. It's independent of HTTP/views,
making it reusable from any delivery mechanism (HTTP, CLI, Celery tasks, etc.).

Following Single Responsibility Principle: Views only handle HTTP orchestration,
Services handle business decisions and domain logic.
"""

from typing import Dict, Any, Optional
from django.core.exceptions import FieldError
from django.db.models import QuerySet
from django.db import IntegrityError

from .models import Application
from .exceptions import (
    ApplicationAlreadyExistsException,
    ApplicationNotFoundException,
    InvalidApplicationDataException,
)


class ApplicationService:
    """
    Service layer for Application management.
    
    Provides business operations with clear error handling and domain logic.
    All methods raise specific ApplicationException subclasses for semantic error handling.
    """

    @staticmethod
    def create_application(
        package_name: str,
        display_name: Optional[str] = None,
        category: Optional[str] = None,
        is_messaging_app: bool = False,
        is_iranian_app: bool = False,
    ) -> Application:
        """
        Create a new application in the registry.
        
        Business rules:
        - package_name must be unique (check before DB hit for better error message)
        - display_name is optional (can be filled by crawler later)
        - is_messaging_app defaults to False
        - is_active defaults to True
        
        Args:
            package_name: Unique application identifier (e.g., com.whatsapp)
            display_name: Human-readable app name (optional, can be auto-filled)
            category: Play Store category (optional)
            is_messaging_app: Flag for network analysis requirement
            is_iranian_app: Flag for crawling with the Iranian region/locale
            
        Returns:
            The created Application instance
            
        Raises:
            ApplicationAlreadyExistsException: If package_name already exists
            InvalidApplicationDataException: If data fails business validation
            
        Example:
            >>> app = ApplicationService.create_application(
            ...     package_name="com.example.app",
            ...     display_name="Example App",
            ...     is_messaging_app=True
            ... )
        """
        # Check for duplicates before trying to create
        # This provides better error messaging than letting DB constraint fail
        if Application.objects.filter(package_name=package_name).exists():
            raise ApplicationAlreadyExistsException(package_name)

        # Normalize whitespace in optional fields
        if display_name:
            display_name = display_name.strip() or None

        if category:
            category = category.strip() or None

        try:
            application = Application.objects.create(
                package_name=package_name,
                display_name=display_name,
                category=category,
                is_messaging_app=is_messaging_app,
                is_iranian_app=is_iranian_app,
                is_active=True,  # Always create as active
            )
            return application
        except IntegrityError as e:
            # Catch DB integrity errors (e.g., unique constraint violations)
            raise InvalidApplicationDataException(
                f"Failed to create application: {str(e)}"
            )

    @staticmethod
    def list_applications(
        filters: Optional[Dict[str, Any]] = None,
        paginate: bool = False,
    ) -> QuerySet:
        """
        Retrieve applications with optional filtering.
        
        Business rules:
        - By default, returns all applications (including inactive)
        - Can filter by is_active, is_messaging_app, category, etc.
        - Returns in descending order by created_at (newest first)
        
        Args:
            filters: Dictionary of filter conditions (Django ORM style)
                     e.g., {'is_active': True, 'is_messaging_app': True}
            paginate: If True, return paginated queryset (caller handles pagination)
            
        Returns:
            Django QuerySet of Application objects
            
        Example:
            >>> apps = ApplicationService.list_applications(
            ...     filters={'is_active': True, 'is_messaging_app': True}
            ... )
            >>> # Pagination is handled by views, not here (separation of concerns)
        """
        queryset = Application.objects.all()

        if filters:
            # Safely apply filters using Django ORM
            try:
                queryset = queryset.filter(**filters)
            except FieldError as e:
                raise InvalidApplicationDataException(
                    f"Invalid filter criteria: {str(e)}"
                )

        # Always order by newest first
        return queryset.order_by('-created_at')

    @staticmethod
    def get_application(app_id: int) -> Application:
        """
        Retrieve a single application by ID.
        
        Args:
            app_id: Application primary key
            
        Returns:
            The Application instance
            
        Raises:
            ApplicationNotFoundException: If app with given ID doesn't exist
        """
        try:
            return Application.objects.get(id=app_id)
        except Application.DoesNotExist:
            raise ApplicationNotFoundException(app_id=app_id)

    @staticmethod
    def get_application_by_package_name(package_name: str) -> Application:
        """
        Retrieve a single application by package name.
        
        Args:
            package_name: The unique package identifier
            
        Returns:
            The Application instance
            
        Raises:
            ApplicationNotFoundException: If app with given package_name doesn't exist
        """
        try:
            return Application.objects.get(package_name=package_name)
        except Application.DoesNotExist:
            raise ApplicationNotFoundException(package_name=package_name)

    @staticmethod
    def update_application(
        app_id: int,
        update_data: Dict[str, Any],
    ) -> Application:
        """
        Update an existing application.
        
        Business rules:
        - Cannot change package_name (immutable key)
        - Can update: display_name, category, is_messaging_app, is_iranian_app, is_active
        - Use deactivate_application() instead of setting is_active=False for clarity
        
        Args:
            app_id: Application ID to update
            update_data: Dictionary of fields to update
            
        Returns:
            Updated Application instance
            
        Raises:
            ApplicationNotFoundException: If app doesn't exist
            InvalidApplicationDataException: If trying to change immutable fields
            
        Example:
            >>> app = ApplicationService.update_application(
            ...     app_id=1,
            ...     update_data={'display_name': 'New Name', 'category': 'Social'}
            ... )
        """
        application = ApplicationService.get_application(app_id)

        # Prevent changing the immutable package_name key
        if 'package_name' in update_data:
            raise InvalidApplicationDataException(
                "Cannot change package_name of an existing application."
            )

        # Allowed updateable fields
        allowed_fields = {
            'display_name',
            'category',
            'is_messaging_app',
            'is_iranian_app',
            'is_active',
        }

        # Filter to only allowed fields
        filtered_data = {
            k: v for k, v in update_data.items() if k in allowed_fields
        }

        # Normalize whitespace in string fields
        if 'display_name' in filtered_data and filtered_data['display_name']:
            filtered_data['display_name'] = (
                filtered_data['display_name'].strip() or None
            )

        if 'category' in filtered_data and filtered_data['category']:
            filtered_data['category'] = filtered_data['category'].strip() or None

        # Update and save
        try:
            for field, value in filtered_data.items():
                setattr(application, field, value)
            application.save()
            return application
        except IntegrityError as e:
            raise InvalidApplicationDataException(
                f"Failed to update application: {str(e)}"
            )

    @staticmethod
    def deactivate_application(app_id: int) -> Application:
        """
        Deactivate an application (soft delete).
        
        Instead of actually deleting the record, we mark it as inactive.
        The crawler will skip inactive applications.
        
        The explicit method name (not delete_application) makes intent clear
        and prevents accidentally using hard delete.
        
        Business rules:
        - Only active applications can be deactivated
        - Deactivated applications can be reactivated
        - No data is actually lost
        
        Args:
            app_id: Application ID to deactivate
            
        Returns:
            The deactivated Application instance
            
        Raises:
            ApplicationNotFoundException: If app doesn't exist
            InvalidApplicationDataException: If app is already inactive
            
        Example:
            >>> deactivated_app = ApplicationService.deactivate_application(app_id=1)
        """
        application = ApplicationService.get_application(app_id)

        application.is_active = False
        application.save()
        return application

    @staticmethod
    def reactivate_application(app_id: int) -> Application:
        """
        Reactivate a previously deactivated application.
        
        This is the inverse of deactivate_application().
        
        Args:
            app_id: Application ID to reactivate
            
        Returns:
            The reactivated Application instance
            
        Raises:
            ApplicationNotFoundException: If app doesn't exist
            InvalidApplicationDataException: If app is already active
        """
        application = ApplicationService.get_application(app_id)

        application.is_active = True
        application.save()
        return application
