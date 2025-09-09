# backend_api/urls.py
from django.contrib import admin
from django.urls import path, include
from core.views import healthz
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from core.views import (
    CRIOperacaoViewSet,
    IndiceViewSet,
    IPCADiarioViewset,   # ✅ new viewset for IPCA Diário
)

# -------------------------------------------------
# DRF Router
# -------------------------------------------------
router = DefaultRouter()
router.register(r"crioperacoes", CRIOperacaoViewSet, basename="crioperacoes")
router.register(r"indices", IndiceViewSet, basename="indices")
router.register(r"ipca-diario", IPCADiarioViewset, basename="ipca-diario")  # ✅

# -------------------------------------------------
# URL Patterns
# -------------------------------------------------
urlpatterns = [
    path("admin/", admin.site.urls),
    path("healthz", healthz), 

    # API (router-generated endpoints)
    path("api/", include(router.urls)),

    # Auth (JWT)
    path("api/token/", TokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("api/token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),

    # Core app urls (if you keep additional endpoints there)
    path("api/", include("core.urls")),
]
