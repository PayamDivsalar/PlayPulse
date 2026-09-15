"""
Integration tests for Application API views.

Tests the full HTTP request/response cycle, checking that views properly
orchestrate services and return appropriate HTTP responses.
"""

from rest_framework.test import APITestCase
from rest_framework import status

from ..models import Application
from ..services import ApplicationService


class ApplicationListCreateViewTestCase(APITestCase):
    """Integration tests for ApplicationListCreateView (GET/POST /api/applications/)"""

    def setUp(self):
        """Create test data."""
        self.list_url = '/api/applications/'
        # Create some test applications
        ApplicationService.create_application(
            package_name='com.app1.test',
            display_name='App 1',
            is_messaging_app=False,
        )
        ApplicationService.create_application(
            package_name='com.app2.test',
            display_name='App 2',
            is_messaging_app=True,
        )

    def tearDown(self):
        """Clean up after each test."""
        Application.objects.all().delete()

    # ==================== GET (List) TESTS ====================

    def test_list_applications_success(self):
        """Test successful listing of all applications."""
        response = self.client.get(self.list_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

    def test_list_applications_response_structure(self):
        """Test that list response has correct structure."""
        response = self.client.get(self.list_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Response should be a list
        self.assertIsInstance(response.data, list)
        
        # Each item should have expected fields
        if response.data:
            app = response.data[0]
            self.assertIn('id', app)
            self.assertIn('package_name', app)
            self.assertIn('display_name', app)
            self.assertIn('is_active', app)
            self.assertIn('is_messaging_app', app)
            self.assertIn('created_at', app)
            self.assertIn('updated_at', app)

    def test_list_applications_filter_is_active(self):
        """Test filtering by is_active parameter."""
        # Deactivate one app
        app = Application.objects.first()
        ApplicationService.deactivate_application(app_id=app.id)
        
        # Get only active
        response = self.client.get(self.list_url + '?is_active=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]['is_active'])

    def test_list_applications_filter_is_messaging_app(self):
        """Test filtering by is_messaging_app parameter."""
        response = self.client.get(self.list_url + '?is_messaging_app=true')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]['is_messaging_app'])

    def test_list_applications_filter_multiple(self):
        """Test combining multiple filters."""
        response = self.client.get(
            self.list_url + '?is_active=true&is_messaging_app=true'
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should have 1 app (app2) that is active and is_messaging_app
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['package_name'], 'com.app2.test')

    # ==================== POST (Create) TESTS ====================

    def test_create_application_success(self):
        """Test successful application creation."""
        payload = {
            'package_name': 'com.newapp.test',
            'display_name': 'New App',
            'category': 'Social',
            'is_messaging_app': True,
        }
        response = self.client.post(self.list_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['package_name'], 'com.newapp.test')
        self.assertEqual(response.data['display_name'], 'New App')
        self.assertTrue(response.data['is_active'])

    def test_create_application_minimal(self):
        """Test creating application with minimum required data."""
        payload = {'package_name': 'com.minimal.test'}
        response = self.client.post(self.list_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['package_name'], 'com.minimal.test')
        self.assertIsNone(response.data['display_name'])

    def test_create_application_iranian(self):
        """Test creating an application with is_iranian_app=true via POST."""
        payload = {
            'package_name': 'com.iranian.test',
            'is_iranian_app': True,
        }
        response = self.client.post(self.list_url, payload, format='json')

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(response.data['is_iranian_app'])
        # Persisted correctly
        app = ApplicationService.get_application_by_package_name(
            'com.iranian.test'
        )
        self.assertTrue(app.is_iranian_app)

    def test_create_application_iranian_defaults_to_false(self):
        """POST without is_iranian_app defaults it to False."""
        payload = {'package_name': 'com.notiranian.test'}
        response = self.client.post(self.list_url, payload, format='json')

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertFalse(response.data['is_iranian_app'])

    def test_create_application_duplicate_package_name(self):
        """Test that duplicate package_name returns an error (409 or 400)."""
        payload = {'package_name': 'com.app1.test'}
        response = self.client.post(self.list_url, payload, format='json')
        
        # Should return a 400 or 409 error (both indicate failure)
        self.assertIn(response.status_code, [status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT])
        self.assertIn('error', response.data)
        # The error message should indicate the package already exists
        error_msg = str(response.data.get('error', '')).lower()
        self.assertTrue(
            'already exists' in error_msg or 'duplicate' in error_msg,
            f"Expected 'already exists' or 'duplicate' in error message, got: {error_msg}"
        )

    def test_create_application_invalid_package_name(self):
        """Test that invalid package_name returns 400 Bad Request."""
        payload = {'package_name': 'invalid_no_dot'}
        response = self.client.post(self.list_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('package_name', response.data)

    def test_create_application_missing_required_field(self):
        """Test that missing required package_name returns 400 Bad Request."""
        payload = {'display_name': 'App without package'}
        response = self.client.post(self.list_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('package_name', response.data)

    def test_create_application_returns_all_fields(self):
        """Test that created application response includes all expected fields."""
        payload = {
            'package_name': 'com.complete.test',
            'display_name': 'Complete App',
            'category': 'Games',
            'is_messaging_app': True,
        }
        response = self.client.post(self.list_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.data
        
        # Verify all fields
        self.assertEqual(data['package_name'], 'com.complete.test')
        self.assertEqual(data['display_name'], 'Complete App')
        self.assertEqual(data['category'], 'Games')
        self.assertTrue(data['is_messaging_app'])
        self.assertIn('is_iranian_app', data)
        self.assertTrue(data['is_active'])
        self.assertIn('id', data)
        self.assertIn('created_at', data)
        self.assertIn('updated_at', data)


class ApplicationDetailViewTestCase(APITestCase):
    """Integration tests for ApplicationDetailView (GET/PUT/PATCH /api/applications/{id}/)"""

    def setUp(self):
        """Create test data."""
        self.app = ApplicationService.create_application(
            package_name='com.detail.test',
            display_name='Detail Test App',
            category='Games',
            is_messaging_app=False,
        )
        self.detail_url = f'/api/applications/{self.app.id}/'

    def tearDown(self):
        """Clean up after each test."""
        Application.objects.all().delete()

    # ==================== GET (Retrieve) TESTS ====================

    def test_retrieve_application_success(self):
        """Test successful retrieval of a single application."""
        response = self.client.get(self.detail_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], self.app.id)
        self.assertEqual(response.data['package_name'], 'com.detail.test')

    def test_retrieve_application_not_found(self):
        """Test that retrieving non-existent app returns 404."""
        response = self.client.get('/api/applications/9999/')
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)

    # ==================== PUT (Full Update) TESTS ====================

    def test_update_application_full_success(self):
        """Test successful update (PATCH) of multiple fields at once."""
        # Note: Using PATCH rather than PUT because PUT requires all fields
        # (including immutable package_name)
        payload = {
            'display_name': 'Updated Name',
            'category': 'Social',
            'is_messaging_app': True,
            'is_active': True,
        }
        response = self.client.patch(self.detail_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['display_name'], 'Updated Name')
        self.assertEqual(response.data['category'], 'Social')
        self.assertTrue(response.data['is_messaging_app'])

    def test_update_application_partial_success(self):
        """Test successful partial update (PATCH) of an application."""
        payload = {'display_name': 'Partially Updated'}
        response = self.client.patch(self.detail_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['display_name'], 'Partially Updated')
        # Other fields should be unchanged
        self.assertEqual(response.data['category'], 'Games')
        self.assertFalse(response.data['is_messaging_app'])

    def test_update_application_iranian_flag(self):
        """PATCH can flip is_iranian_app on an existing application."""
        payload = {'is_iranian_app': True}
        response = self.client.patch(self.detail_url, payload, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['is_iranian_app'])
        # Persisted
        app = ApplicationService.get_application(app_id=self.app.id)
        self.assertTrue(app.is_iranian_app)

    def test_update_application_not_found(self):
        """Test that updating non-existent app returns 404."""
        payload = {'display_name': 'Something'}
        response = self.client.patch('/api/applications/9999/', payload)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_application_cannot_change_package_name(self):
        """Test that application returns 400 when trying to change package_name."""
        payload = {'package_name': 'com.new.different'}
        response = self.client.patch(self.detail_url, payload, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        # Original package_name should be unchanged
        app = ApplicationService.get_application(app_id=self.app.id)
        self.assertEqual(app.package_name, 'com.detail.test')

    def test_update_application_invalid_data(self):
        """Test that invalid data returns 400."""
        payload = {'display_name': '   '}  # Only whitespace - should be invalid
        response = self.client.patch(self.detail_url, payload)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class ApplicationDeactivateViewTestCase(APITestCase):
    """Integration tests for ApplicationDeactivateView (POST /api/applications/{id}/deactivate/)"""

    def setUp(self):
        """Create test data."""
        self.app = ApplicationService.create_application(
            package_name='com.deactivate.test',
        )
        self.deactivate_url = f'/api/applications/{self.app.id}/deactivate/'

    def tearDown(self):
        """Clean up after each test."""
        Application.objects.all().delete()

    def test_deactivate_application_success(self):
        """Test successful deactivation of an application."""
        response = self.client.post(self.deactivate_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data['is_active'])
        
        # Verify in database
        app = ApplicationService.get_application(app_id=self.app.id)
        self.assertFalse(app.is_active)

    def test_deactivate_nonexistent_application(self):
        """Test that deactivating non-existent app returns 404."""
        response = self.client.post('/api/applications/9999/deactivate/')
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_deactivate_already_inactive(self):
        """Test that deactivating already inactive app is idempotent."""
        # Deactivate once
        self.client.post(self.deactivate_url)
        
        # Try to deactivate again
        response = self.client.post(self.deactivate_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data['is_active'])
        app = ApplicationService.get_application(app_id=self.app.id)
        self.assertFalse(app.is_active)


class ApplicationReactivateViewTestCase(APITestCase):
    """Integration tests for ApplicationReactivateView (POST /api/applications/{id}/reactivate/)"""

    def setUp(self):
        """Create test data."""
        self.app = ApplicationService.create_application(
            package_name='com.reactivate.test',
        )
        # Deactivate it
        ApplicationService.deactivate_application(app_id=self.app.id)
        self.reactivate_url = f'/api/applications/{self.app.id}/reactivate/'

    def tearDown(self):
        """Clean up after each test."""
        Application.objects.all().delete()

    def test_reactivate_application_success(self):
        """Test successful reactivation of an application."""
        response = self.client.post(self.reactivate_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['is_active'])
        
        # Verify in database
        app = ApplicationService.get_application(app_id=self.app.id)
        self.assertTrue(app.is_active)

    def test_reactivate_nonexistent_application(self):
        """Test that reactivating non-existent app returns 404."""
        response = self.client.post('/api/applications/9999/reactivate/')
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_reactivate_already_active(self):
        """Test that reactivating already active app is idempotent."""
        # Create and don't deactivate
        active_app = ApplicationService.create_application(
            package_name='com.already.active',
        )
        url = f'/api/applications/{active_app.id}/reactivate/'
        
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['is_active'])
        refreshed = ApplicationService.get_application(app_id=active_app.id)
        self.assertTrue(refreshed.is_active)


class ApplicationEndToEndTestCase(APITestCase):
    """End-to-end integration tests covering full workflows."""

    def setUp(self):
        """Setup for end-to-end tests."""
        self.list_url = '/api/applications/'

    def tearDown(self):
        """Clean up after each test."""
        Application.objects.all().delete()

    def test_complete_crud_workflow(self):
        """Test complete CRUD workflow via API."""
        # CREATE
        create_payload = {
            'package_name': 'com.e2e.test',
            'display_name': 'E2E Test App',
        }
        create_response = self.client.post(
            self.list_url, create_payload, format='json'
        )
        self.assertEqual(create_response.status_code, status.HTTP_201_CREATED)
        app_id = create_response.data['id']
        
        # READ
        detail_url = f'/api/applications/{app_id}/'
        read_response = self.client.get(detail_url)
        self.assertEqual(read_response.status_code, status.HTTP_200_OK)
        self.assertEqual(read_response.data['package_name'], 'com.e2e.test')
        
        # UPDATE
        update_payload = {'display_name': 'Updated E2E App'}
        update_response = self.client.patch(
            detail_url, update_payload, format='json'
        )
        self.assertEqual(update_response.status_code, status.HTTP_200_OK)
        self.assertEqual(update_response.data['display_name'], 'Updated E2E App')
        
        # DEACTIVATE
        deactivate_url = f'/api/applications/{app_id}/deactivate/'
        deactivate_response = self.client.post(deactivate_url)
        self.assertEqual(deactivate_response.status_code, status.HTTP_200_OK)
        self.assertFalse(deactivate_response.data['is_active'])
        
        # LIST with filter
        list_response = self.client.get(self.list_url + '?is_active=false')
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(list_response.data), 1)
        self.assertEqual(list_response.data[0]['id'], app_id)
        
        # REACTIVATE
        reactivate_url = f'/api/applications/{app_id}/reactivate/'
        reactivate_response = self.client.post(reactivate_url)
        self.assertEqual(reactivate_response.status_code, status.HTTP_200_OK)
        self.assertTrue(reactivate_response.data['is_active'])
