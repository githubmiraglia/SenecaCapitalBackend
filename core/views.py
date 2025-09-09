from django.db import transaction, models
from rest_framework import viewsets, permissions, status
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes, action
from .models import Investidor, Preco
from .serializers import InvestidorSerializer, PrecoSerializer
from .utils.cashflow import build_cashflow_input_from_cri
from .utils.helpers_calcs import analyze_CRI

# ✅ put system imports here, not inside functions
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from .models import CRIOperacao, Indice, Investidor, IPCADiario, Preco
from .serializers import (
    CRIOperacaoSerializer,
    IndiceSerializer,
    InvestidorSerializer,
    IPCADiarioSerializer,
    PrecoSerializer,
)

# ✅ Correct import — both helpers come from db_helpers
from .utils.db_helpers import normalize_row_for_model

import subprocess
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from concurrent.futures import ThreadPoolExecutor, as_completed

# IMPORT ROBOTS
from playwright_scripts.fetch_investidores import run_robot


# ------------------------- API: CRI OPERAÇÕES -------------------------
from django.db import models
from rest_framework import viewsets, permissions
from rest_framework.decorators import action
from rest_framework.response import Response

from .models import CRIOperacao
from .serializers import CRIOperacaoSerializer
from .utils.db_helpers import normalize_row_for_model


from django.http import HttpResponse, JsonResponse
from django.db import connections
from django.db.utils import OperationalError

def healthz(request):
    db_conn = connections['default']
    try:
        db_conn.cursor()
        return JsonResponse({"status": "ok", "db": "ok"})
    except OperationalError:
        return JsonResponse({"status": "error", "db": "unreachable"}, status=500)


class CRIOperacaoViewSet(viewsets.ModelViewSet):
    queryset = CRIOperacao.objects.all().order_by("-id")
    serializer_class = CRIOperacaoSerializer
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=False, methods=["post"], url_path="upsert")
    def upsert(self, request):
        rows = request.data.get("rows", [])
        unique_by = request.data.get("unique_by", "codigo_if")

        # 🔎 Debug: log what we received
        print("📥 UPLOAD rows:", rows[:5])  # só primeiros 5 para não poluir
        print("📥 unique_by:", unique_by)

        if not isinstance(rows, list):
            return Response({"detail": "`rows` must be a list"}, status=400)

        updated, errors = [], []

        for r in rows:
            try:
                key_value = r.get(unique_by)
                if isinstance(key_value, str):
                    key_value = key_value.replace("\u200b", "").strip()

                if not key_value:
                    errors.append(f"Missing {unique_by} in row")
                    continue

                obj = CRIOperacao.objects.get(**{unique_by: key_value})

                # 🔎 Debug: show before/after update
                print(f"🔄 Updating {unique_by}={key_value} with:", r)

                normalized = normalize_row_for_model(r, CRIOperacao)
                for key, value in normalized.items():
                    if key not in ["id", unique_by]:
                        setattr(obj, key, value)
                obj.save()

                updated.append(key_value)

            except CRIOperacao.DoesNotExist:
                errors.append(f"{unique_by}={r.get(unique_by)} not found")
            except Exception as e:
                errors.append(str(e))

        return Response({
            "updated_count": len(updated),
            "errors_count": len(errors),
            "updated": updated,
            "errors": errors,
        })
    # -------------------------------------------------------
    # ➕ INSERT NEW ONLY (skip if exists)
    # -------------------------------------------------------
    @action(detail=False, methods=["post"], url_path="insertnew")
    def insertnew(self, request):
        rows = request.data.get("rows", [])
        if not isinstance(rows, list):
            return Response({"detail": "`rows` must be a list"}, status=400)

        # Collect incoming keys
        incoming_codigo_if = {
            (r.get("codigo_if").replace("\u200b", "").strip()
             if isinstance(r.get("codigo_if"), str) else r.get("codigo_if"))
            for r in rows if r.get("codigo_if")
        }
        incoming_isin = {
            (r.get("isin").replace("\u200b", "").strip()
             if isinstance(r.get("isin"), str) else r.get("isin"))
            for r in rows if r.get("isin")
        }

        # Query existing in DB
        existing = set(
            CRIOperacao.objects.filter(
                models.Q(codigo_if__in=incoming_codigo_if) |
                models.Q(isin__in=incoming_isin)
            ).values_list("codigo_if", "isin")
        )

        # Deduplicate inside this batch
        seen_keys = set()
        new_objects, skipped = [], 0

        for r in rows:
            codigo_if = r.get("codigo_if")
            isin = r.get("isin")

            if isinstance(codigo_if, str):
                codigo_if = codigo_if.replace("\u200b", "").strip()
            if isinstance(isin, str):
                isin = isin.replace("\u200b", "").strip()

            if not codigo_if:  # must have codigo_if
                skipped += 1
                continue

            key = (codigo_if, isin)

            if key in existing or (codigo_if, None) in existing or (None, isin) in existing:
                skipped += 1
                continue
            if key in seen_keys:
                skipped += 1
                continue

            seen_keys.add(key)

            # Normalize and save cleaned values
            r["codigo_if"], r["isin"] = codigo_if, isin
            normalized = normalize_row_for_model(r, CRIOperacao)
            new_objects.append(CRIOperacao(**normalized))

        # ✅ Safe bulk insert
        created_count = len(new_objects)
        if new_objects:
            CRIOperacao.objects.bulk_create(new_objects, batch_size=1000, ignore_conflicts=True)

        return Response({"created": created_count, "skipped": skipped})

    # -------------------------------------------------------
    # 📊 CALCULATE + RETURN UPDATED ROWS
    # -------------------------------------------------------
    @action(detail=False, methods=["post"], url_path="calculate")
    def calculate(self, request):
        """
        Run calculation, update CRIOperacao rows in the DB,
        and return updated duration/spread/taxa for the spreadsheet.
        """
        from decimal import Decimal
        from datetime import datetime
        from .utils.cashflow import build_cashflow_input_from_cri
        from .utils.helpers_calcs import analyze_CRI

        updated_rows = []

        try:
            for op in CRIOperacao.objects.all():
                try:
                    # ✅ build cashflow
                    cri = build_cashflow_input_from_cri(op.__dict__)
                    if not cri:
                        continue

                    # ✅ reference date = data_emissao (if exists)
                    data_referencia = None
                    if op.data_emissao:
                        try:
                            data_referencia = datetime.strptime(str(op.data_emissao), "%Y-%m-%d").date()
                        except Exception:
                            pass

                    # ✅ run analyze_CRI
                    calculos_cri = analyze_CRI(
                        cri,
                        reference_date=data_referencia,
                        pu=1000,
                        quantity=(op.montante_emitido / 1000 if op.montante_emitido else None),
                    )

                    taxa = calculos_cri.get("xirr")
                    spread = calculos_cri.get("sov")
                    duration = calculos_cri.get("macaulay_market")

                    # ✅ normalize decimals
                    taxa = Decimal(str(taxa)) if taxa is not None else None
                    spread = Decimal(str(spread)) if spread is not None else None
                    duration = Decimal(str(duration)) if duration is not None else None

                    # ✅ save into DB
                    op.duration = duration
                    op.spread = spread
                    op.taxa = taxa
                    op.save(update_fields=["duration", "spread", "taxa"])

                    updated_rows.append({
                        "codigo_if": op.codigo_if,
                        "duration": duration,
                        "spread": spread,
                        "taxa": taxa,
                    })

                except Exception as inner_e:
                    print(f"❌ Error calculating {op.codigo_if}: {inner_e}")

            return Response({"rows": updated_rows})

        except Exception as e:
            return Response(
                {"detail": f"Error during calculation: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


# ------------------------- API: ÍNDICES -------------------------
class IndiceViewSet(viewsets.ModelViewSet):
    queryset = Indice.objects.all().order_by("-data_da_tabela", "dias_uteis")
    serializer_class = IndiceSerializer
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=False, methods=["post"], url_path="upsert")
    def upsert(self, request):
        """
        POST /api/indices/upsert/
        Body: {"rows":[{...}, {...}]}
        """
        rows = request.data.get("rows", [])
        if not isinstance(rows, list):
            return Response({"detail": "`rows` must be a list"}, status=400)

        # 🔄 Normalize each row
        normalized_rows = [normalize_row_for_model(r, Indice) for r in rows]

        # Add composite unique key (tuple of data_da_tabela + dias_uteis)
        for r in normalized_rows:
            if r.get("data_da_tabela") and r.get("dias_uteis") is not None:
                r["_unique_key"] = f"{r['data_da_tabela']}__{r['dias_uteis']}"

        # Build update_fields (all except PK and unique keys)
        update_fields = [
            f.name for f in Indice._meta.fields
            if f.name not in ["id", "data_da_tabela", "dias_uteis", "composite_key"]
        ]

        # Upsert
        created, updated = upsert(
            model=Indice,
            rows=normalized_rows,
            unique_by="_unique_key",
            update_fields=update_fields,
        )
        return Response({"created": created, "updated": updated})
    
    @action(detail=False, methods=["post"], url_path="insertnew")
    def insertnew(self, request):
        """
        POST /api/indices/insertnew/
        Body: {"rows":[{...}, {...}]}
        Inserts only new rows where composite_key does not exist yet.
        """
        rows = request.data.get("rows", [])
        if not isinstance(rows, list):
            return Response({"detail": "`rows` must be a list"}, status=400)

        # Normalize rows
        normalized_rows = [normalize_row_for_model(r, Indice) for r in rows]

        # Build composite keys for incoming
        incoming_keys = {
            f"{r.get('data_da_tabela')}__{r.get('dias_uteis')}"
            for r in normalized_rows
            if r.get("data_da_tabela") and r.get("dias_uteis") is not None
        }

        # Fetch existing keys from DB
        existing_keys = set(
            Indice.objects.filter(
                composite_key__in=incoming_keys
            ).values_list("composite_key", flat=True)
        )

        seen_keys = set()
        new_objects = []
        skipped = 0

        for r in normalized_rows:
            if r.get("data_da_tabela") and r.get("dias_uteis") is not None:
                key = f"{r['data_da_tabela']}__{r['dias_uteis']}"

                if key in existing_keys or key in seen_keys:
                    skipped += 1
                    continue

                seen_keys.add(key)
                r["composite_key"] = key
                new_objects.append(Indice(**r))

        created_count = len(new_objects)
        if new_objects:
            # ✅ safe insert — skip duplicates at DB level
            Indice.objects.bulk_create(new_objects, batch_size=1000, ignore_conflicts=True)

        return Response({"created": created_count, "skipped": skipped})


# ------------------------- API: INVESTIDORES -------------------------

@api_view(["GET"])
def codigos_if_view(request):
    """
    Return all Codigo IF values from CRIOperacao
    """
    codigos = list(CRIOperacao.objects.values_list("codigo_if", flat=True).distinct())
    return Response({"codigos_if": codigos})


class InvestidorViewSet(viewsets.ModelViewSet):
    queryset = Investidor.objects.all()
    serializer_class = InvestidorSerializer

    # -------------------------------------------------------
    # 🔄 Batch Upsert (update if exists, create otherwise)
    # -------------------------------------------------------
    @action(detail=False, methods=["post"], url_path="batch-upsert", permission_classes=[])
    def batch_upsert(self, request):
        """
        POST /api/investidores/batch-upsert/
        Body: [ {...}, {...} ]

        Upserts by (isin, codigo_if, fii_investidor, mes_referencia).
        """
        data = request.data
        if not isinstance(data, list):
            return Response({"error": "Expected a list"}, status=status.HTTP_400_BAD_REQUEST)

        results, created_count, updated_count = [], 0, 0

        for record in data:
            isin = record.get("isin")
            codigo_if = record.get("codigo_if")
            fii_investidor = record.get("fii_investidor")
            mes_referencia = record.get("mes_referencia")

            if not isin or not codigo_if or not fii_investidor or not mes_referencia:
                continue  # skip invalid rows

            obj, created = Investidor.objects.update_or_create(
                isin=isin,
                codigo_if=codigo_if,
                fii_investidor=fii_investidor,
                mes_referencia=mes_referencia,
                defaults={
                    "quantidade": record.get("quantidade"),
                    "valor_mercado": record.get("valor_mercado"),
                    "serie_investida": record.get("serie_investida"),
                    "classe_investida": record.get("classe_investida"),
                    "nome_operacao": record.get("nome_operacao"),
                }
            )

            if created:
                created_count += 1
            else:
                updated_count += 1

            results.append({
                "id": obj.id,
                "isin": obj.isin,
                "codigo_if": obj.codigo_if,
                "fii_investidor": obj.fii_investidor,
                "mes_referencia": obj.mes_referencia,
                "status": "created" if created else "updated"
            })

        return Response(
            {"created": created_count, "updated": updated_count, "total": len(results), "rows": results},
            status=status.HTTP_200_OK,
        )

    # -------------------------------------------------------
    # ➕ Insert New Only (skip if exists)
    # -------------------------------------------------------
    @action(detail=False, methods=["post"], url_path="insertnew", permission_classes=[])
    def insertnew(self, request):
        """
        POST /api/investidores/insertnew/
        Body: [ {...}, {...} ]

        Inserts only new records, skips existing ones.
        """
        data = request.data
        if not isinstance(data, list):
            return Response({"error": "Expected a list"}, status=status.HTTP_400_BAD_REQUEST)

        created_count, skipped_count = 0, 0
        new_objects, seen_keys = [], set()

        for record in data:
            isin = record.get("isin")
            codigo_if = record.get("codigo_if")
            fii_investidor = record.get("fii_investidor")
            mes_referencia = record.get("mes_referencia")

            if not isin or not codigo_if or not fii_investidor or not mes_referencia:
                skipped_count += 1
                continue

            key = (isin, codigo_if, fii_investidor, mes_referencia)
            if key in seen_keys:
                skipped_count += 1
                continue
            seen_keys.add(key)

            exists = Investidor.objects.filter(
                isin=isin,
                codigo_if=codigo_if,
                fii_investidor=fii_investidor,
                mes_referencia=mes_referencia
            ).exists()

            if exists:
                skipped_count += 1
                continue

            new_objects.append(Investidor(**record))

        if new_objects:
            Investidor.objects.bulk_create(new_objects, batch_size=1000, ignore_conflicts=True)
            created_count = len(new_objects)

        return Response(
            {"created": created_count, "skipped": skipped_count, "total": created_count + skipped_count},
            status=status.HTTP_200_OK,
        )


# --- Endpoint: Run only Investidores ---
@csrf_exempt
def run_investidores_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Método não permitido"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))
        raw_codigos = body.get("listaCodigos", [])

        print("📥 Received from frontend:", raw_codigos[:2], f"... total={len(raw_codigos)}")

        if not raw_codigos:
            return JsonResponse({"error": "Nenhum código recebido"}, status=400)

        # ✅ Normalize into [isin, codigo_if]
        lista_codigos = []
        for row in raw_codigos:
            codigo_if = str(row.get("Código IF", "")).replace("\u200B", "").strip()
            isin = row.get("ISIN") or ""
            if codigo_if:
                lista_codigos.append([isin, codigo_if])

        if not lista_codigos:
            return JsonResponse({"error": "Nenhum código válido encontrado"}, status=400)

        print("✅ Normalized codes:", lista_codigos[:5])

        # 🚀 Run Playwright robot
        run_robot(headless=False, lista_codigos_isin_if=lista_codigos)

        return JsonResponse({"status": "ok", "total_codigos": len(lista_codigos)})
    except Exception as e:
        print("❌ Error in run_investidores_view:", str(e))
        return JsonResponse({"error": str(e)}, status=500)

# ------------------------- API: PRECO -------------------------
class PrecoViewSet(viewsets.ModelViewSet):
    queryset = Preco.objects.all()
    serializer_class = PrecoSerializer

    # -------------------------------------------------------
    # 🔄 Batch Upsert (update if exists, create otherwise)
    # -------------------------------------------------------
    @action(detail=False, methods=["post"], url_path="batch-upsert", permission_classes=[])
    def batch_upsert(self, request):
        """
        POST /api/precos/batch-upsert/
        Body: [ {...}, {...} ]

        Upserts by (isin, codigo_if, data).
        """
        data = request.data
        if not isinstance(data, list):
            return Response({"error": "Expected a list"}, status=status.HTTP_400_BAD_REQUEST)

        results, created_count, updated_count = [], 0, 0

        for record in data:
            isin = record.get("isin")
            codigo_if = record.get("codigo_if")
            data_val = record.get("data")

            if not isin or not codigo_if or not data_val:
                continue  # skip invalid rows

            obj, created = Preco.objects.update_or_create(
                isin=isin,
                codigo_if=codigo_if,
                data=data_val,
                defaults={
                    "classe": record.get("classe"),
                    "titulo": record.get("titulo"),
                    "preco_minimo": record.get("preco_minimo"),
                    "preco_maximo": record.get("preco_maximo"),
                    "preco_ultimo": record.get("preco_ultimo"),
                    "quantidade": record.get("quantidade"),
                    "num_negocios": record.get("num_negocios"),
                    "volume": record.get("volume"),
                    "ambiente": record.get("ambiente"),
                }
            )

            if created:
                created_count += 1
            else:
                updated_count += 1

            results.append({
                "id": obj.id,
                "isin": obj.isin,
                "codigo_if": obj.codigo_if,
                "data": obj.data,
                "status": "created" if created else "updated"
            })

        return Response(
            {"created": created_count, "updated": updated_count, "total": len(results), "rows": results},
            status=status.HTTP_200_OK,
        )

    #--------------------------------------------------------
    # ➕ Insert New Only (skip if exists)
    # -------------------------------------------------------
    @action(detail=False, methods=["post"], url_path="insertnew", permission_classes=[])
    def insertnew(self, request):
        """
        POST /api/precos/insertnew/
        Body: [ {...}, {...} ]

        Inserts only new records, skips existing ones.
        """
        data = request.data
        if not isinstance(data, list):
            return Response({"error": "Expected a list"}, status=status.HTTP_400_BAD_REQUEST)

        created_count, skipped_count = 0, 0
        new_objects, seen_keys = [], set()

        # Collect allowed model fields
        allowed_fields = {f.name for f in Preco._meta.fields}

        for record in data:
            codigo_if = record.get("codigo_if")
            data_val = record.get("data")

            if not codigo_if or not data_val:
                skipped_count += 1
                continue

            key = (codigo_if, data_val)
            if key in seen_keys:
                skipped_count += 1
                continue
            seen_keys.add(key)

            exists = Preco.objects.filter(
                codigo_if=codigo_if,
                data=data_val
            ).exists()

            if exists:
                skipped_count += 1
                continue

            # ✅ Keep only fields that exist in the model
            clean_record = {k: v for k, v in record.items() if k in allowed_fields}
            new_objects.append(Preco(**clean_record))

        if new_objects:
            Preco.objects.bulk_create(new_objects, batch_size=1000, ignore_conflicts=True)
            created_count = len(new_objects)

        return Response(
            {"created": created_count, "skipped": skipped_count, "total": created_count + skipped_count},
            status=status.HTTP_200_OK,
        )

# --- Endpoint: Run only Preços ---
@csrf_exempt
def run_precos_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Método não permitido"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))
        raw_codigos = body.get("listaCodigos", [])

        print("📥 Received from frontend (Preços):", raw_codigos[:2], f"... total={len(raw_codigos)}")

        if not raw_codigos:
            return JsonResponse({"error": "Nenhum código recebido"}, status=400)

        # ✅ Normalize into [isin, codigo_if]
        lista_codigos = []
        for row in raw_codigos:
            codigo_if = str(row.get("Código IF", "")).replace("\u200B", "").strip()
            isin = row.get("ISIN") or ""
            if codigo_if:
                lista_codigos.append([isin, codigo_if])

        if not lista_codigos:
            return JsonResponse({"error": "Nenhum código válido encontrado"}, status=400)

        print("✅ Normalized Preço codes:", lista_codigos[:5])

        # 🚀 Run Playwright robot for Preços
        from playwright_scripts.fetch_precos import run_robot
        run_robot(headless=False, lista_codigos_isin_if=lista_codigos)

        return JsonResponse({"status": "ok", "total_codigos": len(lista_codigos)})
    except Exception as e:
        print("❌ Error in run_precos_view:", str(e))
        return JsonResponse({"error": str(e)}, status=500)

#---------------------------------------------------------------------------------
# API TO MAKE CALCULATIONS
# --------------------------------------------------------------------------------
# Map from frontend labels to DB field names
CRI_OPERACOES_DEPARA = {
    "Código IF": "codigo_if",
    "Data Emissão": "data_emissao",
    "Montante Emitido": "montante_emitido",
    "Remuneração": "indexation",
    "Spread a.a.": "spread_aa",
    "Prazo (meses)": "prazo_meses",
    "Carência Principal": "carencia_principal",
    "Frequência Principal": "frequencia_principal",
    "Tabela Juros": "tabela_juros",
    "Frequência Juros": "frequencia_juros",
    "Método Principal": "principal_method",
    "Período Integralização": "periodo_integralizacao",
    "Frequência Integralização": "freq_integralizacao",
    # … add others as needed
}



def normalize_cri_op(row: dict) -> dict:
    """Convert frontend row to DB-style dict for calculations."""
    normalized = {}
    for frontend_key, backend_key in CRI_OPERACOES_DEPARA.items():
        if frontend_key in row:
            normalized[backend_key] = row[frontend_key]
    
    for key in ["spread_aa", "taxa_nominal_aa"]:    
        if key in normalized:
            normalized[key] = float(normalized[key]) / 100 if float(normalized[key]) > 1 else float(normalized[key])
    
    return normalized


# views.py
import json
import traceback
from datetime import datetime
from decimal import Decimal
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import CRIOperacao, Preco
from .utils.cashflow import build_cashflow_input_from_cri
from .utils.helpers_calcs import analyze_CRI


def normalize_codigo_if(codigo_if: str) -> str:
    """Remove espaços, caracteres invisíveis e normaliza maiúsculas."""
    if not codigo_if:
        return ""
    return codigo_if.replace("\u200B", "").replace(" ", "").strip().upper()


@csrf_exempt
def run_calculos_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Método não permitido"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))
        raw_codigos = body.get("listaCodigos", [])

        if not raw_codigos:
            return JsonResponse({"error": "Nenhum código recebido"}, status=400)

        results = []

        # ⚡ Caso aba de PREÇOS
        if "NUM NEGOCIOS" in raw_codigos[0] and "VOLUME" in raw_codigos[0]:
            print("📊 Detectado aba PREÇOS. Total rows:", len(raw_codigos))
            for row in raw_codigos:
                codigo_if = str(row.get("CÓDIGO IF", "")).replace("\u200B", "").strip()
                if not codigo_if:
                    continue

                cri_ops = list(CRIOperacao.objects.filter(codigo_if=codigo_if).values())
                if not cri_ops:
                    continue

                # ✅ Parse data de referência
                data_ref_str = row.get("DATA") or row.get("DATA REFERENCIA")
                data_referencia = None
                if data_ref_str:
                    try:
                        data_referencia = datetime.strptime(data_ref_str, "%Y-%m-%d").date()
                    except ValueError:
                        pass

                # ✅ Extrair PU
                pu = (
                    row.get("PREÇO (ÚLTIMO)")
                    or row.get("PREÇO (MÍNIMO)")
                    or row.get("PREÇO (MÁXIMO)")
                )
                pu = float(pu) if pu not in ("", None) else None

                for op in cri_ops:
                    try:
                        montante_emitido = op.get("montante_emitido")
                        quantity = montante_emitido / 1000 if montante_emitido else None
                        cri = build_cashflow_input_from_cri(op)

                        calculos_cri = analyze_CRI(
                            cri,
                            reference_date=data_referencia,
                            pu=pu,
                            quantity=quantity,
                        )

                        taxa = calculos_cri.get("xirr")
                        spread = calculos_cri.get("sov")
                        duration = calculos_cri.get("macaulay_market")

                        # ✅ Normaliza valores numéricos
                        duration = Decimal(str(duration)) if duration is not None else None
                        spread = Decimal(str(spread)) if spread is not None else None
                        taxa = Decimal(str(taxa)) if taxa is not None else None

                        # ✅ Atualiza base de dados Preco
                        Preco.objects.filter(codigo_if=codigo_if).update(
                            duration=duration,
                            spread=spread,
                            taxa=taxa,
                        )

                        results.append(
                            {
                                "codigo_if": codigo_if,
                                "data_referencia": str(data_referencia) if data_referencia else None,
                                "duration": duration,
                                "spread": spread,
                                "taxa": taxa,
                            }
                        )
                    except Exception as calc_err:
                        traceback.print_exc()
                        return JsonResponse(
                            {"error": str(calc_err), "codigo_if": codigo_if},
                            status=500,
                        )

        # ⚡ Caso aba de CRI OPERAÇÕES
        else:
            for op in raw_codigos:
                codigo_if = normalize_codigo_if(op.get("Código IF"))
                remuneracao = op.get("Remuneração")
                if not remuneracao:
                    print("SEM REMUNERACAO")
                    continue
                remuneracao = str(remuneracao).strip().upper()
                if remuneracao != "IPCA":
                    #print("NAO FACO OUTRA CONTA")
                    continue
                try:
                    # ✅ Pega Data Emissão como reference_date
                    data_emissao_str = op.get("Data Emissão")
                    data_referencia = None
                    if data_emissao_str:
                        try:
                            data_referencia = datetime.strptime(data_emissao_str, "%Y-%m-%d").date()
                        except ValueError:
                            print(f"⚠️ Erro ao converter Data Emissão: {data_emissao_str}")
                            continue
                    # 🔄 Normaliza e calcula
                    normalized_op = normalize_cri_op(op)
                    montante_emitido = normalized_op.get("montante_emitido")
                    cri = build_cashflow_input_from_cri(normalized_op)
                    pu = 1000
                    quantity = float(montante_emitido)/float(pu)

                    calculos_cri = analyze_CRI(
                        cri,
                        reference_date=data_referencia,
                    )
                    

                    taxa = calculos_cri.get("xirr") * 100
                    spread = calculos_cri.get("sov") * 100
                    duration = calculos_cri.get("macaulay_market") 
        
                    #print("Processing Código IF:", codigo_if, "Data Emissão:", data_referencia)
        
                    # ✅ Atualiza base de Cri operacoes
                    CRIOperacao.objects.filter(codigo_if=codigo_if).update(
                        duration=duration,
                        spread=spread,
                        taxa=taxa,
                    )
                    #print("Processing Código IF:", codigo_if, "Data Emissão:", data_referencia, "Taxa:", taxa, "Spread:", spread, "Duration:", duration)
                except Exception as calc_err:
                    traceback.print_exc()
                    return JsonResponse(
                        {"error": str(calc_err), "codigo_if": codigo_if},
                        status=500,
                    )

        return JsonResponse({"status": "ok", "rows": results})

    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)



# ------------------------- API: IPCADiario -------------------------

class IPCADiarioViewset(viewsets.ModelViewSet):
    queryset = IPCADiario.objects.all().order_by("data")
    serializer_class = IPCADiarioSerializer



# --- Endpoint: Run ALL (but exclude Investidores + Preços) ---
# core/views.py
import subprocess
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

# core/views.py
# core/views.py
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import subprocess

@csrf_exempt
def run_playwright_view(request):
    if request.method == "POST":
        try:
            result = subprocess.run(
                ["python", "scripts/robo.py"],  # example Playwright script
                capture_output=True,
                text=True,
                check=True,
            )
            return JsonResponse({
                "status": "success",
                "output": result.stdout,
                "errors": result.stderr,
            })
        except subprocess.CalledProcessError as e:
            return JsonResponse({
                "status": "error",
                "output": e.stdout,
                "errors": e.stderr,
            }, status=500)
    return JsonResponse({"error": "Only POST allowed"}, status=405)


@csrf_exempt
# --- Helper: run one script ---
def run_script(script):
    try:
        process = subprocess.run(
            ["python", script],
            capture_output=True,
            text=True
        )
        status = "success" if process.returncode == 0 else "error"
        return script, {
            "status": status,
            "stdout": process.stdout,
            "stderr": process.stderr,
        }
    except Exception as e:
        return script, {"status": "error", "stderr": str(e)}

# --- Endpoint: Run ALL (but exclude Investidores + Preços) ---
@csrf_exempt
@csrf_exempt
def run_playwright(request):
    print(" GOT HERE ")

    results = {}
    scripts = [
        "playwright_scripts/fetch_uqbar_cri_to_django.py",
        "playwright_scripts/fetch_indices.py",
        "playwright_scripts/fetch_taxas.py",
    ]

    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        future_to_script = {executor.submit(run_script, s): s for s in scripts}
        for future in as_completed(future_to_script):
            script, result = future.result()

            # ✅ print captured output in Django console
            print(f"\n---- {script} ----")
            print("STDOUT:\n", result.get("stdout"))
            print("STDERR:\n", result.get("stderr"))
            print("------------------\n")

            results[script] = result

    return JsonResponse(results)


'''
@api_view(["GET"])
def codigos_if_view(request):
    """
    Return all Codigo IF values from CRIOperacao
    """
    codigos = list(CRIOperacao.objects.values_list("codigo_if", flat=True).distinct())
    return Response({"codigos_if": codigos})

class InvestidorViewSet(viewsets.ModelViewSet):
    queryset = Investidor.objects.all()
    serializer_class = InvestidorSerializer

    # batch-upsert endpoint
    @action(detail=False, methods=["post"], url_path="batch-upsert")
    def batch_upsert(self, request):
        data = request.data
        if not isinstance(data, list):
            return Response({"error": "Expected a list"}, status=status.HTTP_400_BAD_REQUEST)

        serializer = self.get_serializer(data=data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class IPCADiarioViewset(viewsets.ModelViewSet):
    queryset = IPCADiario.objects.all().order_by("data")
    serializer_class = IPCADiarioSerializer

class PrecoViewSet(viewsets.ModelViewSet):
    queryset = Preco.objects.all()
    serializer_class = PrecoSerializer

    @action(detail=False, methods=["post"], url_path="batch-upsert")
    def batch_upsert(self, request):
        data = request.data

        if not isinstance(data, list):
            return Response({"error": "Expected a list of records"}, status=status.HTTP_400_BAD_REQUEST)

        created, updated = 0, 0

        for record in data:
            obj, created_flag = Preco.objects.update_or_create(
                isin=record.get("isin"),
                codigo_if=record.get("codigo_if"),
                data=record.get("data"),
                defaults=record,
            )

            if created_flag:
                created += 1
            else:
                # ✅ Force overwrite all fields with the latest values
                for field, value in record.items():
                    if hasattr(obj, field):
                        setattr(obj, field, value)
                obj.save()
                updated += 1

        return Response(
            {"created": created, "updated": updated, "total": len(data)},
            status=status.HTTP_200_OK,
        )


# --- Endpoint: Run only Investidores ---
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json
from playwright_scripts.fetch_investidores import run_robot
from datetime import datetime, date
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .models import Investidor   # ✅ make sure this matches your app’s models.py


@csrf_exempt
def run_investidores_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Método não permitido"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))
        raw_codigos = body.get("listaCodigos", [])

        print("📥 Received from frontend:", raw_codigos[:2], f"... total={len(raw_codigos)}")

        if not raw_codigos:
            return JsonResponse({"error": "Nenhum código recebido"}, status=400)

        # ✅ Normalize into [isin, codigo_if]
        lista_codigos = []
        for row in raw_codigos:
            codigo_if = str(row.get("Código IF", "")).replace("\u200B", "").strip()
            isin = row.get("ISIN") or ""   # if you don’t have ISIN yet, leave blank
            if codigo_if:
                lista_codigos.append([isin, codigo_if])

        if not lista_codigos:
            return JsonResponse({"error": "Nenhum código válido encontrado"}, status=400)

        print("✅ Normalized codes:", lista_codigos[:5])

        # 🚀 Run Playwright robot
        run_robot(headless=False, lista_codigos_isin_if=lista_codigos)

        return JsonResponse({"status": "ok", "total_codigos": len(lista_codigos)})

    except Exception as e:
        print("❌ Error in run_investidores_view:", str(e))
        return JsonResponse({"error": str(e)}, status=500)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def batch_upsert_investidores(request):
    """
    Upsert investidores:
    - If (isin, codigo_if, fii_investidor, mes_referencia) exists → update
    - Otherwise → create
    """
    data = request.data
    results = []

    for record in data:
        isin = record.get("isin")
        codigo_if = record.get("codigo_if")
        fii_investidor = record.get("fii_investidor")
        mes_referencia = record.get("mes_referencia")

        if not isin or not codigo_if or not fii_investidor or not mes_referencia:
            continue  # skip invalid rows

        obj, created = Investidor.objects.update_or_create(
            isin=isin,
            codigo_if=codigo_if,
            fii_investidor=fii_investidor,
            mes_referencia=mes_referencia,
            defaults={
                "quantidade": record.get("quantidade"),
                "valor_mercado": record.get("valor_mercado"),
                "serie_investida": record.get("serie_investida"),
                "classe_investida": record.get("classe_investida"),
                "nome_operacao": record.get("nome_operacao"),
            }
        )

        results.append({
            "id": obj.id,
            "created": created,
            "isin": obj.isin,
            "codigo_if": obj.codigo_if,
            "fii_investidor": obj.fii_investidor,
            "mes_referencia": obj.mes_referencia,
            "status": "created" if created else "updated"
        })

    return Response(results, status=200)


# --- Endpoint: Run only Preços ---
def run_precos_view(request):
    script = "scripts/precos.py"
    _, result = run_script(script)
    return JsonResponse({"precos": result})

# --- Endpoint: Run only Taxas (optional if you want a dedicated button) ---
def run_taxas_view(request):
    script = "scripts/taxas.py"
    _, result = run_script(script)
    return JsonResponse({"taxas": result})
'''