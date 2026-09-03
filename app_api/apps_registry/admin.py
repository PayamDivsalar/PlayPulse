from django.contrib import admin
from .models import Application

@admin.register(Application)
class ApplicationAdmin(admin.ModelAdmin):
    list_display = ('package_name', 'display_name', 'category', 'is_messaging_app', 'is_active', 'created_at')
    list_filter = ('is_active', 'is_messaging_app', 'category')
    search_fields = ('package_name', 'display_name')