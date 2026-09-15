from django.db import models

# Create your models here.

class Application(models.Model):
    package_name = models.CharField(
        max_length=255,
        unique=True,
        help_text="Unique app identifier on Google Play (e.g. com.whatsapp)"
    )
    display_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Display name. If empty, filled by crawler on first run."
    )
    category = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Official Play Store category"
    )
    is_messaging_app = models.BooleanField(
        default=False,
        help_text="Flags whether this app needs network (pcap) analysis"
    )
    is_iranian_app = models.BooleanField(
        default=False,
        help_text="Whether this app targets the Iranian market; used by the "
                  "crawler to select the appropriate Play Store region/locale."
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Soft-delete flag; crawler only processes active apps"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return self.display_name or self.package_name