"""
URL configuration for Applications Registry API.

Defines the API endpoints for CRUD operations on applications.
This is imported into config/urls.py to be included in the main URL routing.
"""

from django.urls import path
from . import views

app_name = 'apps_registry'

urlpatterns = [
    # List and Create
    path(
        'applications/',
        views.ApplicationListCreateView.as_view(),
        name='application-list-create',
    ),
    # Retrieve and Update
    path(
        'applications/<int:pk>/',
        views.ApplicationDetailView.as_view(),
        name='application-detail',
    ),
    # Deactivate (Soft Delete)
    path(
        'applications/<int:pk>/deactivate/',
        views.ApplicationDeactivateView.as_view(),
        name='application-deactivate',
    ),
    # Reactivate
    path(
        'applications/<int:pk>/reactivate/',
        views.ApplicationReactivateView.as_view(),
        name='application-reactivate',
    ),
]
