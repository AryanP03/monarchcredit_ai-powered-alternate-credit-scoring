# app/main.py

from fastapi import FastAPI, HTTPException

from app.services_individual.schemas import IndividualLoanApplicationRequest
from app.services_MSME.schemas import MSMESchema

from app.services_individual.ingestion_service import IngestionService as IndividualIngestionService
from app.services_MSME.ingestion_service import MSMEIngestionService as MSMEIngestionService

from app.routes.auth_routes import router as auth_router
from pydantic import BaseModel
from typing import Optional
import csv
import io
from fastapi.responses import StreamingResponse
from app.services_MSME.finalization_service import finalization_service, FinalizationError
from app.services_individual.finalization_service import (
    finalization_service as individual_finalization_service,
    FinalizationError as IndividualFinalizationError,
)

import mysql.connector
from app.config import get_mysql_config

def get_db_connection():
    return mysql.connector.connect(**get_mysql_config())


app = FastAPI(title="Alternate Credit Scoring API")

import mysql.connector

# DB Connection Helper
"""def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",      # Your MySQL username
        password="aryan123",  # Your MySQL password
        database="auditdb"
    )"""

# Create one service object for each flow
individual_ingestion_service = IndividualIngestionService()
msme_ingestion_service = MSMEIngestionService()

# Include auth routes
app.include_router(auth_router, prefix="/auth", tags=["Authentication"])

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # later restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class IndividualManagerDecisionRequest(BaseModel):
    manager_id: Optional[str] = None
    decision: str
    principal: Optional[float] = None
    interest_rate: Optional[float] = None
    tenure: Optional[int] = None
    notes: Optional[str] = None


@app.post("/manager/individual/{application_id}/final-decision")
def submit_individual_final_decision(application_id: str, payload: IndividualManagerDecisionRequest):
    try:
        payload_dict = payload.model_dump()
        payload_dict["application_id"] = application_id
        result = individual_finalization_service.finalize(payload_dict)
        return result
    except IndividualFinalizationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class MSMEManagerDecisionRequest(BaseModel):
    manager_id: Optional[str] = None
    decision: str
    principal: Optional[float] = None
    interest_rate: Optional[float] = None
    tenure: Optional[int]=None
    notes: Optional[str] = None

from app.services_MSME.finalization_service import finalization_service

@app.post("/manager/msme/{application_id}/final-decision")
def submit_msme_final_decision(application_id: str, payload: MSMEManagerDecisionRequest):
    try:
        payload_dict = payload.model_dump()
        payload_dict["application_id"] = application_id
        result = finalization_service.finalize(payload_dict)
        return result
    except FinalizationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def health_check():
    """
    Basic API health check endpoint.
    """
    return {
        "status": "ok",
        "message": "API is running"
    }





@app.get("/api/manager/dashboard/individual")
def get_individual_manager_dashboard():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT
                application_id,
                full_name,
                email,
                phone,
                pan_number,
                employment_status,
                loan_ask,
                loan_tenure_months,
                risk_score,
                risk_band,
                decision,
                suggested_principal,
                suggested_interest_rate,
                manager_pdf_filename,
                status,
                created_at,
                updated_at
            FROM individual_manager_review_data
            ORDER BY created_at DESC
        """)

        rows = cursor.fetchall()

        return {
            "application_type": "individual",
            "count": len(rows),
            "items": rows
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch individual dashboard: {str(e)}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

@app.get("/api/manager/review/individual/{application_id}")
def get_individual_manager_review(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT
                r.application_id,
                r.full_name,
                r.email,
                r.phone,
                r.pan_number,
                r.employment_status,
                r.loan_ask,
                r.loan_tenure_months,
                r.risk_score,
                r.risk_band,
                r.decision,
                r.suggested_principal,
                r.suggested_interest_rate,
                r.shap_plot_base64,
                r.manager_pdf_filename,
                r.status,
                r.created_at,
                r.updated_at,

                f.manager_decision,
                f.final_principal,
                f.final_interest_rate,
                f.final_tenure,
                f.override_flag,
                f.manager_notes,
                f.manager_id

            FROM individual_manager_review_data r
            LEFT JOIN msme_final_decision f
                ON r.application_id = f.application_id
               AND f.application_type = 'individual'
            WHERE r.application_id = %s
            LIMIT 1
        """, (application_id,))

        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Individual application not found")

        row["application_type"] = "individual"
        return row

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch individual review: {str(e)}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

@app.get("/api/manager/individual/{application_id}/pdf")
def get_individual_manager_pdf(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT manager_pdf_blob, manager_pdf_filename
            FROM individual_manager_review_data
            WHERE application_id = %s
        """, (application_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        pdf_blob = row.get("manager_pdf_blob")
        pdf_filename = row.get("manager_pdf_filename") or f"{application_id}.pdf"

        if not pdf_blob:
            raise HTTPException(status_code=404, detail="PDF not available yet")

        pdf_bytes = bytes(pdf_blob)

        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{pdf_filename}"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch PDF: {str(e)}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

@app.get("/api/customer/individual/{application_id}/result")
def get_individual_application_result(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT
                r.application_id,
                r.full_name,
                r.loan_ask,
                r.risk_score,
                r.risk_band,
                r.shap_plot_base64,
                r.manager_pdf_filename,
                r.status,
                r.created_at,
                r.updated_at,
                f.manager_decision,
                f.final_principal,
                f.final_interest_rate,
                f.final_tenure,
                f.manager_notes
            FROM individual_manager_review_data r
            LEFT JOIN msme_final_decision f
                ON r.application_id = f.application_id
               AND f.application_type = 'individual'
            WHERE r.application_id = %s
            LIMIT 1
        """, (application_id,))

        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        row["application_type"] = "individual"
        return row

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

@app.get("/api/customer/msme/{application_id}/result")
def get_customer_application_result(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT
                r.application_id,
                r.company_name,
                r.applicant_name,
                r.entity_type,
                r.registration_number,
                r.loan_ask,
                r.risk_score,
                r.risk_band,
                r.shap_plot_base64,
                r.manager_pdf_filename,
                r.status,
                r.created_at,
                r.updated_at,
                f.manager_decision,
                f.final_principal,
                f.final_interest_rate,
                f.final_tenure,
                f.manager_notes
            FROM msme_manager_review_data r
            LEFT JOIN msme_final_decision f
                ON r.application_id = f.application_id
            WHERE r.application_id = %s
                       LIMIT 1
        """, (application_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        row["application_type"] = "msme"
        return row

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch customer result: {str(e)}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()


import traceback

@app.post("/apply/individual")
def submit_individual_loan_application(payload: IndividualLoanApplicationRequest):
    try:
        data = payload.model_dump()
        print("INDIVIDUAL DATA RECEIVED:", data)

        result = individual_ingestion_service.ingest_application(data)
        application_id = result["application_id"]

        return {
            "application_type": "individual",
            "application_id": application_id,
            "result": result,
            "manager_status": "queued_for_processing"
        }

    except Exception as exc:
        print("Individual apply failed:", exc)
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Individual ingestion failed: {str(exc)}"
        )


import uuid

@app.post("/apply/msme")
def submit_msme_loan_application(payload: MSMESchema):
    try:
        data = payload.model_dump()
        print("DATA RECEIVED:", data)

        result = msme_ingestion_service.ingest_application(data)
        application_id = result["application_id"]

        return {
            "application_type": "msme",
            "application_id": application_id,
            "result": result,
            "manager_status": "queued_for_processing"
        }

    except Exception as exc:
        print("MSME apply failed:", exc)
        raise HTTPException(status_code=500, detail=f"MSME ingestion failed: {str(exc)}")
# ---------------------------------------------------------------------
# MANAGER DASHBOARD ROUTES
# ---------------------------------------------------------------------

@app.get("/api/manager/dashboard/active")
def get_manager_dashboard_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        query = """
            SELECT
                application_id,
                company_name,
                applicant_name,
                entity_type,
                registration_number,
                sector_id,
                loan_ask,
                risk_score,
                risk_band,
                suggested_principal,
                suggested_interest_rate,
                status,
                created_at,
                updated_at,
                manager_pdf_filename
            FROM msme_manager_review_data
            ORDER BY created_at DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        return rows

    except Exception as e:
        print(f"Error fetching manager dashboard data: {e}")
        raise HTTPException(status_code=500, detail="Database fetch failed")


@app.get("/api/manager/review/{application_id}")
def get_application_details(application_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        query = """
    SELECT
        application_id,
        company_name,
        applicant_name,
        entity_type,
        registration_number,
        sector_id,
        loan_ask,
        risk_score,
        risk_band,
        decision,
        suggested_principal,
        suggested_interest_rate,
        shap_plot_base64,
        manager_pdf_filename,
        status,
        created_at,
        updated_at
    FROM msme_manager_review_data
    WHERE application_id = %s
"""
        cursor.execute(query, (application_id,))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Application not found")

        result["application_type"] = "msme"
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    



@app.get("/api/manager/review/{application_id}/pdf")
def get_manager_pdf(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT manager_pdf_blob, manager_pdf_filename
            FROM msme_manager_review_data
            WHERE application_id = %s
        """, (application_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        pdf_blob = row.get("manager_pdf_blob")
        pdf_filename = row.get("manager_pdf_filename") or f"{application_id}.pdf"

        if not pdf_blob:
            raise HTTPException(status_code=404, detail="PDF not available yet")

        pdf_bytes = bytes(pdf_blob)

        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{pdf_filename}"'
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch PDF: {str(e)}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()


@app.get("/api/customer/individual/{application_id}/pdf")
def get_customer_individual_application_pdf(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
            SELECT customer_pdf_blob, customer_pdf_filename
            FROM msme_final_decision
            WHERE application_id = %s
              AND application_type = 'individual'
        """, (application_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        pdf_blob = row.get("customer_pdf_blob")
        pdf_filename = row.get("customer_pdf_filename") or f"{application_id}.pdf"

        if not pdf_blob:
            raise HTTPException(status_code=404, detail="Customer PDF not available yet")

        pdf_bytes = bytes(pdf_blob)

        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{pdf_filename}"'
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

@app.get("/api/customer/msme/{application_id}/pdf")
def get_customer_msme_application_pdf(application_id: str):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        cursor.execute("""
    SELECT customer_pdf_blob, customer_pdf_filename
    FROM msme_final_decision
    WHERE application_id = %s
      AND application_type = 'msme'
""", (application_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        pdf_blob = row.get("customer_pdf_blob")
        pdf_filename = row.get("customer_pdf_filename") or f"{application_id}.pdf"

        if not pdf_blob:
            raise HTTPException(status_code=404, detail="Customer PDF not available yet")

        pdf_bytes = bytes(pdf_blob)

        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{pdf_filename}"'
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

@app.get("/api/manager/dashboard/audit/download")
def download_audit():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

        query = """
    SELECT
        f.application_id,
        r.company_name,
        r.applicant_name,
        r.entity_type,
        r.registration_number,
        r.loan_ask,
        f.model_decision,
        f.model_risk_score,
        f.model_suggested_principal,
        f.model_suggested_interest_rate,
        f.manager_decision,
        f.final_principal,
        f.final_interest_rate,
        f.final_tenure,
        f.override_flag,
        f.manager_notes,
        f.manager_id,
        f.created_at,
        f.updated_at
    FROM msme_final_decision f
    JOIN msme_manager_review_data r
      ON r.application_id = f.application_id
    ORDER BY f.created_at DESC
"""
        cursor.execute(query)
        rows = cursor.fetchall()

        output = io.StringIO()

        fieldnames = [
    "application_id",
    "company_name",
    "applicant_name",
    "entity_type",
    "registration_number",
    "loan_ask",
    "model_decision",
    "model_risk_score",
    "model_suggested_principal",
    "model_suggested_interest_rate",
    "manager_decision",
    "final_principal",
    "final_interest_rate",
    "final_tenure",
    "override_flag",
    "manager_notes",
    "manager_id",
    "created_at",
    "updated_at",
]

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow({
        "application_id": row.get("application_id"),
        "company_name": row.get("company_name"),
        "applicant_name": row.get("applicant_name"),
        "entity_type": row.get("entity_type"),
        "registration_number": row.get("registration_number"),
        "loan_ask": row.get("loan_ask"),
        "model_decision": row.get("model_decision"),
        "model_risk_score": row.get("model_risk_score"),
        "model_suggested_principal": row.get("model_suggested_principal"),
        "model_suggested_interest_rate": row.get("model_suggested_interest_rate"),
        "manager_decision": row.get("manager_decision"),
        "final_principal": row.get("final_principal"),
        "final_interest_rate": row.get("final_interest_rate"),
        "final_tenure": row.get("final_tenure"),
        "override_flag": row.get("override_flag"),
        "manager_notes": row.get("manager_notes"),
        "manager_id": row.get("manager_id"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    })

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": 'attachment; filename="AUDIT_STORE.csv"'
            },
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate audit CSV: {str(e)}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

    