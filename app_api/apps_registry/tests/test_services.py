"""
Unit tests for ApplicationService.

Tests business logic in isolation, independent of HTTP/views.
This demonstrates the value of separating business logic from delivery layer.
"""

from django.test import TestCase

from ..models import Application
from ..services import ApplicationService
from ..exceptions import (
    ApplicationAlreadyExistsException,
    ApplicationNotFoundException,
    InvalidApplicationDataException,
)


class ApplicationServiceTestCase(TestCase):
    """Test cases for ApplicationService business logic."""

    def tearDown(self):
        """Clean up test data after each test."""
        Application.objects.all().delete()

    # ==================== CREATE TESTS ====================

    def test_create_application_success(self):
        """Test successful application creation."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
            display_name='Example App',
            category='Social',
            is_messaging_app=True,
        )
        self.assertEqual(app.package_name, 'com.example.app')
        self.assertEqual(app.display_name, 'Example App')
        self.assertEqual(app.category, 'Social')
        self.assertTrue(app.is_messaging_app)
        self.assertTrue(app.is_active)  # Should be active by default
        self.assertIsNotNone(app.id)

    def test_create_application_minimal(self):
        """Test creating application with minimum required data."""
        app = ApplicationService.create_application(
            package_name='com.minimal.app'
        )
        self.assertEqual(app.package_name, 'com.minimal.app')
        self.assertIsNone(app.display_name)
        self.assertIsNone(app.category)
        self.assertFalse(app.is_messaging_app)
        self.assertFalse(app.is_iranian_app)
        self.assertTrue(app.is_active)

    def test_create_application_iranian(self):
        """Test creating an application flagged as Iranian."""
        app = ApplicationService.create_application(
            package_name='com.iranian.app',
            is_iranian_app=True,
        )
        self.assertTrue(app.is_iranian_app)

    def test_create_application_duplicate_package_name(self):
        """Test that duplicate package_name raises exception before DB hit."""
        ApplicationService.create_application(
            package_name='com.duplicate.app'
        )
        
        # Second attempt with same package_name should fail
        with self.assertRaises(ApplicationAlreadyExistsException) as context:
            ApplicationService.create_application(
                package_name='com.duplicate.app'
            )
        
        self.assertIn('com.duplicate.app', str(context.exception))

    def test_create_application_whitespace_normalization(self):
        """Test that whitespace in optional fields is normalized."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
            display_name='  My App  ',
            category='  Social  ',
        )
        # Whitespace should be normalized in the service
        self.assertEqual(app.display_name, 'My App')
        self.assertEqual(app.category, 'Social')

    # ==================== LIST TESTS ====================

    def test_list_applications_empty(self):
        """Test listing applications when none exist."""
        queryset = ApplicationService.list_applications()
        self.assertEqual(queryset.count(), 0)

    def test_list_applications_all(self):
        """Test listing all applications."""
        ApplicationService.create_application(package_name='com.app1.app')
        ApplicationService.create_application(package_name='com.app2.app')
        ApplicationService.create_application(package_name='com.app3.app')

        queryset = ApplicationService.list_applications()
        self.assertEqual(queryset.count(), 3)

    def test_list_applications_filter_is_active(self):
        """Test filtering applications by is_active status."""
        ApplicationService.create_application(package_name='com.active1.app')
        ApplicationService.create_application(package_name='com.active2.app')
        app3 = ApplicationService.create_application(package_name='com.inactive.app')
        
        # Deactivate one
        ApplicationService.deactivate_application(app_id=app3.id)

        # List only active
        active_apps = ApplicationService.list_applications(
            filters={'is_active': True}
        )
        self.assertEqual(active_apps.count(), 2)

        # List only inactive
        inactive_apps = ApplicationService.list_applications(
            filters={'is_active': False}
        )
        self.assertEqual(inactive_apps.count(), 1)

    def test_list_applications_filter_is_messaging_app(self):
        """Test filtering applications by is_messaging_app flag."""
        ApplicationService.create_application(
            package_name='com.whatsapp.app',
            is_messaging_app=True,
        )
        ApplicationService.create_application(
            package_name='com.game.app',
            is_messaging_app=False,
        )

        messaging = ApplicationService.list_applications(
            filters={'is_messaging_app': True}
        )
        self.assertEqual(messaging.count(), 1)
        self.assertEqual(messaging.first().package_name, 'com.whatsapp.app')

    def test_list_applications_ordering(self):
        """Test that applications are ordered by newest first."""
        app1 = ApplicationService.create_application(package_name='com.app1.app')
        app2 = ApplicationService.create_application(package_name='com.app2.app')
        app3 = ApplicationService.create_application(package_name='com.app3.app')

        queryset = ApplicationService.list_applications()
        apps = list(queryset)
        
        # Should be in reverse order (newest first)
        self.assertEqual(apps[0].id, app3.id)
        self.assertEqual(apps[1].id, app2.id)
        self.assertEqual(apps[2].id, app1.id)

    # ==================== RETRIEVE TESTS ====================

    def test_get_application_by_id(self):
        """Test retrieving a single application by ID."""
        created_app = ApplicationService.create_application(
            package_name='com.example.app',
            display_name='Example App',
        )
        
        retrieved_app = ApplicationService.get_application(app_id=created_app.id)
        
        self.assertEqual(retrieved_app.id, created_app.id)
        self.assertEqual(retrieved_app.package_name, 'com.example.app')
        self.assertEqual(retrieved_app.display_name, 'Example App')

    def test_get_application_not_found(self):
        """Test that retrieving non-existent application raises exception."""
        with self.assertRaises(ApplicationNotFoundException) as context:
            ApplicationService.get_application(app_id=999)
        
        self.assertIn('999', str(context.exception))

    def test_get_application_by_package_name(self):
        """Test retrieving application by package name."""
        ApplicationService.create_application(
            package_name='com.example.app',
            display_name='Example App',
        )
        
        app = ApplicationService.get_application_by_package_name(
            'com.example.app'
        )
        
        self.assertEqual(app.package_name, 'com.example.app')

    def test_get_application_by_package_name_not_found(self):
        """Test that non-existent package_name raises exception."""
        with self.assertRaises(ApplicationNotFoundException) as context:
            ApplicationService.get_application_by_package_name(
                'com.nonexistent.app'
            )
        
        self.assertIn('com.nonexistent.app', str(context.exception))

    # ==================== UPDATE TESTS ====================

    def test_update_application_success(self):
        """Test successful application update."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
            display_name='Old Name',
            category='Games',
        )
        
        updated_app = ApplicationService.update_application(
            app_id=app.id,
            update_data={
                'display_name': 'New Name',
                'category': 'Social',
            },
        )
        
        self.assertEqual(updated_app.display_name, 'New Name')
        self.assertEqual(updated_app.category, 'Social')
        self.assertEqual(updated_app.package_name, 'com.example.app')  # Unchanged

    def test_update_application_partial(self):
        """Test partial update (only changing some fields)."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
            display_name='Original Name',
            category='Games',
            is_messaging_app=False,
        )
        
        updated_app = ApplicationService.update_application(
            app_id=app.id,
            update_data={'is_messaging_app': True},
        )
        
        self.assertEqual(updated_app.display_name, 'Original Name')  # Unchanged
        self.assertEqual(updated_app.category, 'Games')  # Unchanged
        self.assertTrue(updated_app.is_messaging_app)  # Changed

    def test_update_application_iranian_flag(self):
        """Test updating the is_iranian_app flag."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
        )
        self.assertFalse(app.is_iranian_app)

        updated_app = ApplicationService.update_application(
            app_id=app.id,
            update_data={'is_iranian_app': True},
        )
        self.assertTrue(updated_app.is_iranian_app)

        # And back to False
        reverted = ApplicationService.update_application(
            app_id=app.id,
            update_data={'is_iranian_app': False},
        )
        self.assertFalse(reverted.is_iranian_app)

    def test_update_application_cannot_change_package_name(self):
        """Test that package_name cannot be changed (immutable key)."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
        )
        
        with self.assertRaises(InvalidApplicationDataException) as context:
            ApplicationService.update_application(
                app_id=app.id,
                update_data={'package_name': 'com.new.app'},
            )
        
        self.assertIn('package_name', str(context.exception).lower())

    def test_update_nonexistent_application(self):
        """Test that updating non-existent app raises exception."""
        with self.assertRaises(ApplicationNotFoundException):
            ApplicationService.update_application(
                app_id=999,
                update_data={'display_name': 'New Name'},
            )

    def test_update_ignores_unknown_fields(self):
        """Test that unknown fields in update_data are ignored."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
            display_name='Original Name',
        )
        
        updated_app = ApplicationService.update_application(
            app_id=app.id,
            update_data={
                'display_name': 'New Name',
                'unknown_field': 'should_be_ignored',
                'another_unknown': 123,
            },
        )
        
        # Display name should be updated
        self.assertEqual(updated_app.display_name, 'New Name')
        # Unknown fields should be ignored (no error raised)

    # ==================== DEACTIVATE/REACTIVATE TESTS ====================

    def test_deactivate_application(self):
        """Test soft delete via deactivation."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
        )
        self.assertTrue(app.is_active)

        deactivated = ApplicationService.deactivate_application(app_id=app.id)
        
        self.assertFalse(deactivated.is_active)
        # Record should still exist in DB
        self.assertTrue(
            Application.objects.filter(id=app.id).exists()
        )

    def test_deactivate_already_inactive_is_idempotent(self):
        """Test that deactivating an already inactive app is idempotent."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
        )
        ApplicationService.deactivate_application(app_id=app.id)

        deactivated_again = ApplicationService.deactivate_application(app_id=app.id)
        self.assertFalse(deactivated_again.is_active)
        self.assertTrue(Application.objects.filter(id=app.id, is_active=False).exists())

    def test_deactivate_nonexistent_application(self):
        """Test that deactivating non-existent app raises exception."""
        with self.assertRaises(ApplicationNotFoundException):
            ApplicationService.deactivate_application(app_id=999)

    def test_reactivate_application(self):
        """Test reactivating a deactivated application."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
        )
        ApplicationService.deactivate_application(app_id=app.id)
        
        reactivated = ApplicationService.reactivate_application(app_id=app.id)
        
        self.assertTrue(reactivated.is_active)

    def test_reactivate_already_active_is_idempotent(self):
        """Test that reactivating an already active app is idempotent."""
        app = ApplicationService.create_application(
            package_name='com.example.app',
        )

        reactivated_again = ApplicationService.reactivate_application(app_id=app.id)
        self.assertTrue(reactivated_again.is_active)
        self.assertTrue(Application.objects.filter(id=app.id, is_active=True).exists())

    def test_reactivate_nonexistent_application(self):
        """Test that reactivating non-existent app raises exception."""
        with self.assertRaises(ApplicationNotFoundException):
            ApplicationService.reactivate_application(app_id=999)

    # ==================== INTEGRATION TESTS ====================

    def test_full_lifecycle(self):
        """Test complete lifecycle: create → read → update → deactivate → reactivate."""
        # Create
        app = ApplicationService.create_application(
            package_name='com.lifecycle.app',
            display_name='Lifecycle App',
            is_messaging_app=False,
        )
        initial_id = app.id
        self.assertTrue(app.is_active)
        self.assertFalse(app.is_messaging_app)

        # Read
        retrieved = ApplicationService.get_application(app_id=initial_id)
        self.assertEqual(retrieved.display_name, 'Lifecycle App')

        # Update
        updated = ApplicationService.update_application(
            app_id=initial_id,
            update_data={
                'display_name': 'Updated Name',
                'is_messaging_app': True,
            },
        )
        self.assertEqual(updated.display_name, 'Updated Name')
        self.assertTrue(updated.is_messaging_app)

        # Deactivate
        deactivated = ApplicationService.deactivate_application(app_id=initial_id)
        self.assertFalse(deactivated.is_active)

        # Reactivate
        reactivated = ApplicationService.reactivate_application(app_id=initial_id)
        self.assertTrue(reactivated.is_active)

        # Verify final state
        final = ApplicationService.get_application(app_id=initial_id)
        self.assertEqual(final.display_name, 'Updated Name')
        self.assertTrue(final.is_messaging_app)
        self.assertTrue(final.is_active)
