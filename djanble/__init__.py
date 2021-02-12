__version__ = "0.1.0"

from django.contrib.admin.models import LogEntry

from .managers import NoLogEntryManager

# Change the model manager to one that doesn't log
LogEntry.objects = NoLogEntryManager(LogEntry)
