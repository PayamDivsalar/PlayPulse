from rest_framework import serializers
from .models import Application


class ApplicationSerializer(serializers.ModelSerializer):
    """
    Serializer for Application model.
    
    Handles validation of application data and serialization of model instances.
    Fields like id, created_at, updated_at are read-only as they're managed by the system.
    """
    
    # Override display_name field to not trim whitespace (so we can validate it)
    display_name = serializers.CharField(
        required=False,
        allow_null=True,
        allow_blank=False,
        trim_whitespace=False,  # Don't auto-trim, let validator handle it
    )

    class Meta:
        model = Application
        fields = [
            'id',
            'package_name',
            'display_name',
            'category',
            'is_messaging_app',
            'is_iranian_app',
            'is_active',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
        # Disable unique constraint validation at serializer level
        # (let the service layer handle uniqueness checking)
        extra_kwargs = {
            'package_name': {'validators': []},  # Remove unique validator
        }

    def validate_package_name(self, value):
        """
        Validate package name format.
        
        Package names must follow Android convention: com.company.appname
        - Must contain at least one dot
        - Must not contain spaces or special characters
        
        Args:
            value: The package name to validate
            
        Returns:
            The validated package name
            
        Raises:
            serializers.ValidationError: If package name format is invalid
        """
        if not value or len(value.strip()) == 0:
            raise serializers.ValidationError(
                "Package name cannot be empty."
            )
        
        if ' ' in value:
            raise serializers.ValidationError(
                "Package name cannot contain spaces."
            )
        
        if value.count('.') < 1:
            raise serializers.ValidationError(
                "Package name must contain at least one dot (e.g., com.company.app)"
            )
        
        # Check if it starts or ends with a dot
        if value.startswith('.') or value.endswith('.'):
            raise serializers.ValidationError(
                "Package name cannot start or end with a dot."
            )
        
        # Check for consecutive dots
        if '..' in value:
            raise serializers.ValidationError(
                "Package name cannot contain consecutive dots."
            )
        
        return value

    def validate_display_name(self, value):
        """
        Validate display name if provided.
        
        Display name is optional (can be filled by crawler later),
        but if provided, it cannot be just whitespace.
        """
        if value is None:
            return value

        if isinstance(value, str) and len(value.strip()) == 0:
            raise serializers.ValidationError(
                "Display name cannot be just whitespace."
            )

        return value

    def validate(self, attrs):
        """Validate whitespace-only input edge cases."""
        display_name = attrs.get('display_name')
        if isinstance(display_name, str) and len(display_name.strip()) == 0:
            raise serializers.ValidationError(
                {'display_name': 'Display name cannot be just whitespace.'}
            )
        return attrs


class ApplicationUpdateSerializer(ApplicationSerializer):
    """Serializer for update operations where package_name is immutable."""

    package_name = serializers.CharField(read_only=True)
