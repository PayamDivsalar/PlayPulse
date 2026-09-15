"""
Unit tests for ApplicationSerializer.

Tests validation logic at the serializer level.
Focuses on package_name validation and data serialization.
"""

from django.test import TestCase
from rest_framework import serializers

from ..models import Application
from ..serializers import ApplicationSerializer


class ApplicationSerializerTestCase(TestCase):
    """Test cases for ApplicationSerializer validation."""

    def test_serializer_with_valid_data(self):
        """Test serialization with valid, complete data."""
        data = {
            'package_name': 'com.example.app',
            'display_name': 'Example App',
            'category': 'Social',
            'is_messaging_app': True,
        }
        serializer = ApplicationSerializer(data=data)
        self.assertTrue(serializer.is_valid())
        self.assertEqual(serializer.validated_data['package_name'], 'com.example.app')

    def test_serializer_with_minimal_data(self):
        """Test serialization with only required package_name."""
        data = {
            'package_name': 'com.minimal.app',
        }
        serializer = ApplicationSerializer(data=data)
        self.assertTrue(serializer.is_valid())
        self.assertIsNone(serializer.validated_data.get('display_name'))

    def test_is_iranian_app_deserializes_true(self):
        """is_iranian_app=True is accepted and lands in validated data."""
        data = {
            'package_name': 'com.example.app',
            'is_iranian_app': True,
        }
        serializer = ApplicationSerializer(data=data)
        self.assertTrue(serializer.is_valid())
        self.assertTrue(serializer.validated_data['is_iranian_app'])

    def test_is_iranian_app_defaults_to_false(self):
        """When omitted, is_iranian_app defaults to False."""
        data = {
            'package_name': 'com.example.app',
        }
        serializer = ApplicationSerializer(data=data)
        self.assertTrue(serializer.is_valid())
        self.assertFalse(serializer.validated_data.get('is_iranian_app', False))

    def test_package_name_validation_empty_string(self):
        """Package name cannot be empty."""
        data = {'package_name': ''}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)

    def test_package_name_validation_whitespace_only(self):
        """Package name cannot be only whitespace."""
        data = {'package_name': '   '}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)

    def test_package_name_validation_no_dot(self):
        """Package name must contain at least one dot."""
        data = {'package_name': 'nodotshere'}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)
        self.assertIn('dot', str(serializer.errors['package_name'][0]).lower())

    def test_package_name_validation_with_spaces(self):
        """Package name cannot contain spaces."""
        data = {'package_name': 'com.example .app'}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)

    def test_package_name_validation_starts_with_dot(self):
        """Package name cannot start with a dot."""
        data = {'package_name': '.com.example'}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)

    def test_package_name_validation_ends_with_dot(self):
        """Package name cannot end with a dot."""
        data = {'package_name': 'com.example.'}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)

    def test_package_name_validation_consecutive_dots(self):
        """Package name cannot contain consecutive dots."""
        data = {'package_name': 'com..example.app'}
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('package_name', serializer.errors)

    def test_package_name_validation_valid_formats(self):
        """Test valid package name formats."""
        valid_names = [
            'com.whatsapp',
            'com.google.android',
            'com.example.myapp',
            'io.github.user.project',
            'a.b',  # Minimum valid: two components
        ]
        for name in valid_names:
            data = {'package_name': name}
            serializer = ApplicationSerializer(data=data)
            self.assertTrue(
                serializer.is_valid(),
                f"Should accept package_name '{name}' but got errors: {serializer.errors}"
            )

    def test_display_name_validation_whitespace(self):
        """Display name cannot be only whitespace."""
        data = {
            'package_name': 'com.example.app',
            'display_name': '   ',
        }
        serializer = ApplicationSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('display_name', serializer.errors)

    def test_display_name_normalization_removed_whitespace(self):
        """Display name is preserved by serializer (normalization is in service layer)."""
        data = {
            'package_name': 'com.example.app',
            'display_name': '  Valid Name  ',
        }
        serializer = ApplicationSerializer(data=data)
        self.assertTrue(serializer.is_valid())
        # Serializer preserves the input; normalization happens in service layer
        self.assertEqual(serializer.validated_data['display_name'], '  Valid Name  ')

    def test_read_only_fields_cannot_be_set(self):
        """id, created_at, updated_at should be read-only."""
        data = {
            'package_name': 'com.example.app',
            'id': 999,
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2020-01-01T00:00:00Z',
        }
        serializer = ApplicationSerializer(data=data)
        # Should be valid, read-only fields are simply ignored in input
        self.assertTrue(serializer.is_valid())
        self.assertNotIn('id', serializer.validated_data)
        self.assertNotIn('created_at', serializer.validated_data)
        self.assertNotIn('updated_at', serializer.validated_data)

    def test_serialized_output_contains_all_fields(self):
        """Test that serializer output includes all model fields."""
        app = Application.objects.create(
            package_name='com.test.app',
            display_name='Test App',
            category='Testing',
            is_messaging_app=True,
            is_active=True,
        )
        serializer = ApplicationSerializer(instance=app)
        data = serializer.data

        # Verify all expected fields are present
        self.assertIn('id', data)
        self.assertIn('package_name', data)
        self.assertIn('display_name', data)
        self.assertIn('category', data)
        self.assertIn('is_messaging_app', data)
        self.assertIn('is_iranian_app', data)
        self.assertIn('is_active', data)
        self.assertIn('created_at', data)
        self.assertIn('updated_at', data)

        # Verify values
        self.assertEqual(data['package_name'], 'com.test.app')
        self.assertEqual(data['display_name'], 'Test App')
        self.assertTrue(data['is_messaging_app'])
        self.assertFalse(data['is_iranian_app'])  # Model default
        self.assertTrue(data['is_active'])
