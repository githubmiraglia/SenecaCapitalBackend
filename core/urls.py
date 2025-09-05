from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    InvestidorViewSet,
    PrecoViewSet,
    codigos_if_view,
    run_playwright,
    run_investidores_view,
    run_precos_view,
    run_calculos_view,
)

router = DefaultRouter()
router.register(r"investidores", InvestidorViewSet, basename="investidor")
router.register(r"precos", PrecoViewSet, basename="preco")

urlpatterns = [
    # --- API endpoints ---
    path("codigos-if/", codigos_if_view, name="codigos-if"),
    path("", include(router.urls)),  # auto-adds /investidores/ and /precos/

    # --- Robot endpoints ---
    path("run-playwright/", run_playwright, name="run_playwright"),
    path("run-investidores/", run_investidores_view, name="run_investidores"),
    path("run-precos/", run_precos_view, name="run_precos"),
    path("run-calculos/", run_calculos_view, name="run_calculos"),
]
