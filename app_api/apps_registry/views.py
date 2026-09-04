"""
HTTP API Views for Application management.

Views are purely responsible for:
1. Accepting HTTP requests
2. Calling appropriate service methods
3. Returning HTTP responses

All business logic is delegated to the service layer.
This keeps views thin and focused on HTTP concerns only.

Using DRF Generic Views instead of ViewSets because we have distinct,
meaningful operations that deserve explicit view classes.
"""

from rest_framework import generics, status
from rest_framework.response import Response
from drf_yasg.utils import swagger_auto_schema, no_body
from drf_yasg import openapi

from .models import Application
from .serializers import ApplicationSerializer, ApplicationUpdateSerializer
from .services import ApplicationService
from .exceptions import InvalidApplicationDataException


error_response_schema = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    properties={
        'error': openapi.Schema(type=openapi.TYPE_STRING),
    },
    required=['error'],
)

list_is_active_param = openapi.Parameter(
    'is_active',
    openapi.IN_QUERY,
    description='Only active applications when true; inactive applications when false.',
    type=openapi.TYPE_BOOLEAN,
)

list_is_messaging_app_param = openapi.Parameter(
    'is_messaging_app',
    openapi.IN_QUERY,
    description='Only messaging apps when true; non-messaging apps when false.',
    type=openapi.TYPE_BOOLEAN,
)

list_category_param = openapi.Parameter(
    'category',
    openapi.IN_QUERY,
    description='Filter applications by Play Store category.',
    type=openapi.TYPE_STRING,
)


class ApplicationListCreateView(generics.ListCreateAPIView):
    """
    List all applications or create a new one.
    
    GET /api/applications/ — List all applications
    POST /api/applications/ — Create new application
    
    Supports filtering via query parameters:
    - ?is_active=true — Only active applications
    - ?is_messaging_app=true — Only messaging apps
    - ?category=Social — Filter by category
    """

    queryset = Application.objects.all()
    serializer_class = ApplicationSerializer

    def get_queryset(self):
        """Apply filters from query parameters to the queryset."""
        # Extract filter parameters from query string
        is_active = self.request.query_params.get('is_active')
        is_messaging_app = self.request.query_params.get('is_messaging_app')
        category = self.request.query_params.get('category')

        filters = {}
        if is_active is not None:
            # Convert string 'true'/'false' to boolean
            filters['is_active'] = is_active.lower() in ('true', '1', 'yes')

        if is_messaging_app is not None:
            filters['is_messaging_app'] = is_messaging_app.lower() in (
                'true',
                '1',
                'yes',
            )

        if category:
            filters['category'] = category

        # Pass filters to service layer (filtering happens there, not in view)
        return ApplicationService.list_applications(filters=filters if filters else None)

    @swagger_auto_schema(
        manual_parameters=[
            list_is_active_param,
            list_is_messaging_app_param,
            list_category_param,
        ],
        responses={
            200: ApplicationSerializer(many=True),
            400: openapi.Response('Invalid filter criteria', error_response_schema),
        },
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    @swagger_auto_schema(
        request_body=ApplicationSerializer,
        responses={
            201: ApplicationSerializer,
            400: openapi.Response('Invalid data', error_response_schema),
            409: openapi.Response('Duplicate package_name', error_response_schema),
        },
    )
    def create(self, request, *args, **kwargs):
        """
        Create a new application.
        
        Domain exceptions are handled by the custom exception handler.
        No try/except needed here—let exceptions bubble up.
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        application = ApplicationService.create_application(
            package_name=serializer.validated_data['package_name'],
            display_name=serializer.validated_data.get('display_name'),
            category=serializer.validated_data.get('category'),
            is_messaging_app=serializer.validated_data.get(
                'is_messaging_app', False
            ),
        )
        return Response(
            ApplicationSerializer(application).data,
            status=status.HTTP_201_CREATED,
        )


class ApplicationDetailView(generics.RetrieveUpdateAPIView):
    """
    Retrieve or update an application.
    
    GET /api/applications/{id}/ — Get application details
    PUT /api/applications/{id}/ — Replace application (full update)
    PATCH /api/applications/{id}/ — Partial update
    
    Note: DELETE is not provided here. Use the separate Deactivate endpoint
    to maintain intention clarity (soft delete vs hard delete).
    """

    queryset = Application.objects.all()
    serializer_class = ApplicationSerializer

    @swagger_auto_schema(
        request_body=None,
        responses={
            200: ApplicationSerializer,
            404: openapi.Response('Application not found', error_response_schema),
        },
    )
    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve a single application.
        
        Returns 404 if application doesn't exist.
        Domain exception is handled by custom exception handler.
        """
        application = ApplicationService.get_application(
            app_id=self.kwargs['pk']
        )
        return Response(
            ApplicationSerializer(application).data,
            status=status.HTTP_200_OK,
        )

    @swagger_auto_schema(
        request_body=ApplicationUpdateSerializer,
        responses={
            200: ApplicationSerializer,
            400: openapi.Response('Invalid data', error_response_schema),
            404: openapi.Response('Application not found', error_response_schema),
        },
    )
    def update(self, request, *args, **kwargs):
        """
        Update an application (PUT or PATCH).
        
        Delegates to service layer for business logic.
        Domain exceptions are handled by the custom exception handler.
        """
        application = ApplicationService.get_application(
            app_id=self.kwargs['pk']
        )

        if 'package_name' in request.data:
            raise InvalidApplicationDataException(
                'package_name is read-only and cannot be changed.'
            )

        # Use partial=True for PATCH, False for PUT
        partial = self.request.method == 'PATCH'
        serializer = ApplicationUpdateSerializer(
            application,
            data=request.data,
            partial=partial,
        )
        serializer.is_valid(raise_exception=True)

        updated_app = ApplicationService.update_application(
            app_id=self.kwargs['pk'],
            update_data=serializer.validated_data,
        )
        return Response(
            ApplicationSerializer(updated_app).data,
            status=status.HTTP_200_OK,
        )


class ApplicationDeactivateView(generics.GenericAPIView):
    """
    Deactivate (soft delete) an application.
    
    POST /api/applications/{id}/deactivate/ — Mark as inactive
    
    This is a separate endpoint from PUT/PATCH to make intent explicit:
    - We're not using HTTP DELETE (which suggests permanent removal)
    - We're not using a generic update (which could be ambiguous)
    - We're explicitly deactivating, which matches business intent
    
    The crawler will skip inactive applications.
    Users can reactivate if needed.
    """

    queryset = Application.objects.all()
    serializer_class = ApplicationSerializer

    @swagger_auto_schema(
        request_body=no_body,
        responses={
            200: ApplicationSerializer,
            404: openapi.Response('Application not found', error_response_schema),
        },
    )
    def post(self, request, *args, **kwargs):
        """Mark application as inactive (idempotent)."""
        application = ApplicationService.deactivate_application(
            app_id=self.kwargs['pk']
        )
        return Response(
            ApplicationSerializer(application).data,
            status=status.HTTP_200_OK,
        )


class ApplicationReactivateView(generics.GenericAPIView):
    """
    Reactivate a previously deactivated application.
    
    POST /api/applications/{id}/reactivate/ — Mark as active
    
    Inverse of deactivate. Used to restore apps that were previously disabled.
    Idempotent: calling multiple times has the same effect as calling once.
    """

    queryset = Application.objects.all()
    serializer_class = ApplicationSerializer

    @swagger_auto_schema(
        request_body=no_body,
        responses={
            200: ApplicationSerializer,
            404: openapi.Response('Application not found', error_response_schema),
        },
    )
    def post(self, request, *args, **kwargs):
        """Mark application as active (idempotent)."""
        application = ApplicationService.reactivate_application(
            app_id=self.kwargs['pk']
        )
        return Response(
            ApplicationSerializer(application).data,
            status=status.HTTP_200_OK,
        )
