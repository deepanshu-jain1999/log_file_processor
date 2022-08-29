from django.urls import path
from core.views import ProcessLog


urlpatterns = [
    path("process-logs/", ProcessLog.as_view(), name="process_logs"),
]
