from __future__ import annotations

import uuid
from typing import Any, Dict, Tuple

from app.messaging.producer import JSONKafkaProducer
from app.services_individual.audit_service_individual import audit_service


class IngestionService:
    CUST_INFO = "customer_info"
    RAW_DATA_INGESTED = "raw_data_ingested"

    def __init__(self):
        self.producer = JSONKafkaProducer()

    @staticmethod
    def _safe_get(data: Dict[str, Any], *keys, default=None):
        current = data
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current

    def ingest_application(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Receives already-validated individual payload from main.py,
        generates application_id,
        splits payload into:
          1. model-required raw individual data
          2. non-model customer/info data
        then pushes both to Kafka.
        """
        application_id = uuid.uuid4().hex

        raw_payload, cust_info_payload = self._split_payload(
            payload=payload,
            application_id=application_id
        )

        # Initial DB/audit insert
        audit_service.process_ingestion_payload(cust_info_payload)

        # Publish to Kafka
        self.producer.send(self.RAW_DATA_INGESTED, raw_payload)
        self.producer.send(self.CUST_INFO, cust_info_payload)

        return {
            "application_id": application_id,
            "status": "submitted",
            "message": "Individual payload successfully published to Kafka",
            "published_topics": {
                "customer_info": self.CUST_INFO,
                "raw_data_ingested": self.RAW_DATA_INGESTED,
            }
        }

    def _split_payload(
        self,
        payload: Dict[str, Any],
        application_id: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Splits incoming validated individual JSON into:
        - RAW_DATA_INGESTED: only model-required fields
        - CUST_INFO: remaining descriptive customer information
        """
        raw = payload["raw_model_input_fields"]

        raw_payload = {
            "application_id": application_id,

            # ---------------------------------------------------
            # 1. Loan Details
            # ---------------------------------------------------
            "AMT_CREDIT": raw["AMT_CREDIT"],
            "AMT_ANNUITY": raw["AMT_ANNUITY"],
            "AMT_GOODS_PRICE": raw["AMT_GOODS_PRICE"],

            # ---------------------------------------------------
            # 2. Applicant Profile
            # ---------------------------------------------------
            "AMT_INCOME_TOTAL": raw["AMT_INCOME_TOTAL"],
            "AGE_YEARS": raw["AGE_YEARS"],
            "EMPLOYMENT_YEARS": raw["EMPLOYMENT_YEARS"],
            "NAME_EDUCATION_TYPE": raw["NAME_EDUCATION_TYPE"],
            "FLAG_OWN_CAR": raw["FLAG_OWN_CAR"],
            "FLAG_OWN_REALTY": raw["FLAG_OWN_REALTY"],
            "CNT_CHILDREN": raw["CNT_CHILDREN"],
            "CNT_FAM_MEMBERS": raw["CNT_FAM_MEMBERS"],

            # ---------------------------------------------------
            # 3. Identity & Address
            # ---------------------------------------------------
            "ID_DOCUMENT_AGE_YEARS": raw["ID_DOCUMENT_AGE_YEARS"],
            "YEARS_AT_CURRENT_ADDRESS": raw["YEARS_AT_CURRENT_ADDRESS"],
            "REGION_RATING_CLIENT": raw["REGION_RATING_CLIENT"],
            "REG_CITY_NOT_LIVE_CITY": raw["REG_CITY_NOT_LIVE_CITY"],
            "REG_CITY_NOT_WORK_CITY": raw["REG_CITY_NOT_WORK_CITY"],
            "LIVE_CITY_NOT_WORK_CITY": raw["LIVE_CITY_NOT_WORK_CITY"],

            # ---------------------------------------------------
            # 4. Contact Info
            # ---------------------------------------------------
            "FLAG_EMP_PHONE": raw["FLAG_EMP_PHONE"],
            "FLAG_PHONE": raw["FLAG_PHONE"],
            "FLAG_MOBIL": raw["FLAG_MOBIL"],
            "FLAG_CONT_MOBILE": raw["FLAG_CONT_MOBILE"],
            "FLAG_EMAIL": raw["FLAG_EMAIL"],
            "MONTHS_SINCE_LAST_PHONE_CHANGE": raw["MONTHS_SINCE_LAST_PHONE_CHANGE"],
            "TOTAL_DOCS_SUBMITTED": raw["TOTAL_DOCS_SUBMITTED"],

            # ---------------------------------------------------
            # 5. Credit History
            # ---------------------------------------------------
            "AMT_REQ_CREDIT_BUREAU_YEAR": raw["AMT_REQ_CREDIT_BUREAU_YEAR"],
            "AMT_REQ_CREDIT_BUREAU_MON": raw["AMT_REQ_CREDIT_BUREAU_MON"],
            "PREV_APPROVED_COUNT": raw["PREV_APPROVED_COUNT"],
            "PREV_REFUSED_COUNT": raw["PREV_REFUSED_COUNT"],
            "INST_LATE_COUNT": raw["INST_LATE_COUNT"],
            "INST_RECENT_LATE": raw["INST_RECENT_LATE"],
            "INST_RATIO_MIN": raw["INST_RATIO_MIN"],
            "INST_RATIO_MEAN": raw["INST_RATIO_MEAN"],
            "BUREAU_CREDIT_COUNT": raw["BUREAU_CREDIT_COUNT"],
            "BUREAU_WORST_STATUS": raw["BUREAU_WORST_STATUS"],
            "BUREAU_CLOSED_COUNT": raw["BUREAU_CLOSED_COUNT"],
            "BUREAU_OVERDUE_SUM": raw["BUREAU_OVERDUE_SUM"],
            "BUREAU_TOTAL_DEBT": raw["BUREAU_TOTAL_DEBT"],
            "CC_DPD_MAX": raw["CC_DPD_MAX"],

            # ---------------------------------------------------
            # 6. External Credit Scores
            # ---------------------------------------------------
            "EXT_SOURCE_1": raw.get("EXT_SOURCE_1"),
            "EXT_SOURCE_2": raw.get("EXT_SOURCE_2"),
            "EXT_SOURCE_3": raw.get("EXT_SOURCE_3"),

            # ---------------------------------------------------
            # Internal derived fields
            # ---------------------------------------------------
            "DAYS_BIRTH": payload["internal_derived_fields"]["DAYS_BIRTH"],
            "DAYS_EMPLOYED": payload["internal_derived_fields"]["DAYS_EMPLOYED"],
            "DAYS_ID_PUBLISH": payload["internal_derived_fields"]["DAYS_ID_PUBLISH"],
            "DAYS_REGISTRATION": payload["internal_derived_fields"]["DAYS_REGISTRATION"],
            "DAYS_LAST_PHONE_CHANGE": payload["internal_derived_fields"]["DAYS_LAST_PHONE_CHANGE"],
        }

        cust_info_payload = self._build_customer_info_payload(
            payload=payload,
            application_id=application_id
        )

        return raw_payload, cust_info_payload

    def _build_customer_info_payload(
        self,
        payload: Dict[str, Any],
        application_id: str
) -> Dict[str, Any]:
        return {
        "application_id": application_id,

        # -------------------------------
        # Application Meta
        # -------------------------------
        "application_date": self._safe_get(payload, "application_meta", "application_date"),
        "application_channel": self._safe_get(payload, "application_meta", "application_channel"),
        "product_type": self._safe_get(payload, "application_meta", "product_type"),
        "currency": self._safe_get(payload, "application_meta", "currency"),

        # -------------------------------
        # Personal Details
        # -------------------------------
        "full_name": self._safe_get(payload, "customer_profile", "personal_details", "full_name"),
        "first_name": self._safe_get(payload, "customer_profile", "personal_details", "first_name"),
        "middle_name": self._safe_get(payload, "customer_profile", "personal_details", "middle_name"),
        "last_name": self._safe_get(payload, "customer_profile", "personal_details", "last_name"),
        "gender": self._safe_get(payload, "customer_profile", "personal_details", "gender"),
        "date_of_birth": self._safe_get(payload, "customer_profile", "personal_details", "date_of_birth"),
        "age_years": self._safe_get(payload, "customer_profile", "personal_details", "age_years"),
        "marital_status": self._safe_get(payload, "customer_profile", "personal_details", "marital_status"),
        "education_qualification": self._safe_get(payload, "customer_profile", "personal_details", "education_qualification"),
        "nationality": self._safe_get(payload, "customer_profile", "personal_details", "nationality"),
        "pan_number": self._safe_get(payload, "customer_profile", "personal_details", "pan_number"),
        "aadhaar_last4": self._safe_get(payload, "customer_profile", "personal_details", "aadhaar_last4"),
        "government_id_type": self._safe_get(payload, "customer_profile", "personal_details", "government_id_type"),
        "government_id_number": self._safe_get(payload, "customer_profile", "personal_details", "government_id_number"),
        "id_document_age_years": self._safe_get(payload, "customer_profile", "personal_details", "id_document_age_years"),

        # -------------------------------
        # Family Details
        # -------------------------------
        "family_members_count": self._safe_get(payload, "customer_profile", "family_details", "family_members_count"),
        "children_count": self._safe_get(payload, "customer_profile", "family_details", "children_count"),
        "dependents_count": self._safe_get(payload, "customer_profile", "family_details", "dependents_count"),
        "spouse_name": self._safe_get(payload, "customer_profile", "family_details", "spouse_name"),

        # -------------------------------
        # Employment Details
        # -------------------------------
        "employment_status": self._safe_get(payload, "customer_profile", "employment_details", "employment_status"),
        "occupation_type": self._safe_get(payload, "customer_profile", "employment_details", "occupation_type"),
        "employer_name": self._safe_get(payload, "customer_profile", "employment_details", "employer_name"),
        "work_experience_years_total": self._safe_get(payload, "customer_profile", "employment_details", "work_experience_years_total"),
        "employment_years_current_job": self._safe_get(payload, "customer_profile", "employment_details", "employment_years_current_job"),
        "monthly_income": self._safe_get(payload, "customer_profile", "employment_details", "monthly_income"),
        "annual_income_total": self._safe_get(payload, "customer_profile", "employment_details", "annual_income_total"),
        "has_employment_phone": self._safe_get(payload, "customer_profile", "employment_details", "has_employment_phone"),

        # -------------------------------
        # Assets
        # -------------------------------
        "owns_car": self._safe_get(payload, "customer_profile", "assets_and_ownership", "owns_car"),
        "owns_property": self._safe_get(payload, "customer_profile", "assets_and_ownership", "owns_property"),

        # -------------------------------
        # Contact
        # -------------------------------
        "email": self._safe_get(payload, "contact_details", "email"),
        "primary_phone": self._safe_get(payload, "contact_details", "primary_phone"),
        "alternate_phone": self._safe_get(payload, "contact_details", "alternate_phone"),
        "has_personal_phone": self._safe_get(payload, "contact_details", "has_personal_phone"),
        "has_mobile": self._safe_get(payload, "contact_details", "has_mobile"),
        "mobile_contactable": self._safe_get(payload, "contact_details", "mobile_contactable"),
        "has_email": self._safe_get(payload, "contact_details", "has_email"),
        "months_since_last_phone_change": self._safe_get(payload, "contact_details", "months_since_last_phone_change"),

        # -------------------------------
        # Address
        # -------------------------------
        "current_address_line_1": self._safe_get(payload, "address_details", "current_address_line_1"),
        "current_address_line_2": self._safe_get(payload, "address_details", "current_address_line_2"),
        "city": self._safe_get(payload, "address_details", "city"),
        "state": self._safe_get(payload, "address_details", "state"),
        "postal_code": self._safe_get(payload, "address_details", "postal_code"),
        "country": self._safe_get(payload, "address_details", "country"),
        "years_at_current_address": self._safe_get(payload, "address_details", "years_at_current_address"),
        "region_rating_client": self._safe_get(payload, "address_details", "region_rating_client"),
        "city_rating_differs_from_region": self._safe_get(payload, "address_details", "city_rating_differs_from_region"),
        "registered_city_not_live_city": self._safe_get(payload, "address_details", "registered_city_not_live_city"),
        "registered_city_not_work_city": self._safe_get(payload, "address_details", "registered_city_not_work_city"),
        "live_city_not_work_city": self._safe_get(payload, "address_details", "live_city_not_work_city"),

        # -------------------------------
        # Loan Details (flat, as requested)
        # -------------------------------
        "loan_purpose": self._safe_get(payload, "loan_details", "loan_purpose"),
        "loan_amount": self._safe_get(payload, "loan_details", "loan_amount"),
        "loan_tenure_months": self._safe_get(payload, "loan_details", "loan_tenure_months"),
        "goods_price": self._safe_get(payload, "loan_details", "goods_price"),

        # -------------------------------
        # Documents
        # -------------------------------
        "total_documents_submitted": self._safe_get(payload, "documents_submitted", "total_documents_submitted"),
        "submitted_documents": self._safe_get(payload, "documents_submitted", "submitted_documents", default=[]),

        # -------------------------------
        # Credit History
        # -------------------------------
        "bureau_inquiries_last_12_months": self._safe_get(payload, "credit_history", "bureau_inquiries_last_12_months"),
        "bureau_inquiries_last_month": self._safe_get(payload, "credit_history", "bureau_inquiries_last_month"),
        "previous_approved_loan_count": self._safe_get(payload, "credit_history", "previous_approved_loan_count"),
        "previous_refused_loan_count": self._safe_get(payload, "credit_history", "previous_refused_loan_count"),
        "late_installment_payments_total": self._safe_get(payload, "credit_history", "late_installment_payments_total"),
        "late_payments_last_6_months": self._safe_get(payload, "credit_history", "late_payments_last_6_months"),
        "lowest_payment_ratio": self._safe_get(payload, "credit_history", "lowest_payment_ratio"),
        "average_payment_ratio": self._safe_get(payload, "credit_history", "average_payment_ratio"),
        "bureau_credit_lines_total": self._safe_get(payload, "credit_history", "bureau_credit_lines_total"),
        "bureau_worst_status": self._safe_get(payload, "credit_history", "bureau_worst_status"),
        "bureau_closed_credit_count": self._safe_get(payload, "credit_history", "bureau_closed_credit_count"),
        "bureau_overdue_sum": self._safe_get(payload, "credit_history", "bureau_overdue_sum"),
        "bureau_total_debt": self._safe_get(payload, "credit_history", "bureau_total_debt"),
        "credit_card_max_dpd": self._safe_get(payload, "credit_history", "credit_card_max_dpd"),

        # -------------------------------
        # External Credit Scores
        # -------------------------------
        "ext_source_1": self._safe_get(payload, "external_credit_scores", "ext_source_1"),
        "ext_source_2": self._safe_get(payload, "external_credit_scores", "ext_source_2"),
        "ext_source_3": self._safe_get(payload, "external_credit_scores", "ext_source_3"),
    }

    
    """
    Exact dict of RAW_DATA_INGESTED
    {
  "application_id": "<generated_uuid>",

  "AMT_CREDIT": 350000,
  "AMT_ANNUITY": 11500,
  "AMT_GOODS_PRICE": 320000,

  "AMT_INCOME_TOTAL": 900000,
  "AGE_YEARS": 27,
  "EMPLOYMENT_YEARS": 3,
  "NAME_EDUCATION_TYPE": "higher_education",
  "FLAG_OWN_CAR": 1,
  "FLAG_OWN_REALTY": 1,
  "CNT_CHILDREN": 1,
  "CNT_FAM_MEMBERS": 4,

  "ID_DOCUMENT_AGE_YEARS": 6,
  "YEARS_AT_CURRENT_ADDRESS": 4,
  "REGION_RATING_CLIENT": 2,
  "REG_CITY_NOT_LIVE_CITY": 0,
  "REG_CITY_NOT_WORK_CITY": 0,
  "LIVE_CITY_NOT_WORK_CITY": 0,

  "FLAG_EMP_PHONE": 1,
  "FLAG_PHONE": 1,
  "FLAG_MOBIL": 1,
  "FLAG_CONT_MOBILE": 1,
  "FLAG_EMAIL": 1,
  "MONTHS_SINCE_LAST_PHONE_CHANGE": 18,
  "TOTAL_DOCS_SUBMITTED": 5,

  "AMT_REQ_CREDIT_BUREAU_YEAR": 2,
  "AMT_REQ_CREDIT_BUREAU_MON": 0,
  "PREV_APPROVED_COUNT": 1,
  "PREV_REFUSED_COUNT": 0,
  "INST_LATE_COUNT": 1,
  "INST_RECENT_LATE": 0,
  "INST_RATIO_MIN": 0.95,
  "INST_RATIO_MEAN": 0.99,
  "BUREAU_CREDIT_COUNT": 3,
  "BUREAU_WORST_STATUS": 1,
  "BUREAU_CLOSED_COUNT": 1,
  "BUREAU_OVERDUE_SUM": 0,
  "BUREAU_TOTAL_DEBT": 85000,
  "CC_DPD_MAX": 0,

  "EXT_SOURCE_1": 0.62,
  "EXT_SOURCE_2": 0.71,
  "EXT_SOURCE_3": 0.68,

  "DAYS_BIRTH": -9855,
  "DAYS_EMPLOYED": -1095,
  "DAYS_ID_PUBLISH": -2190,
  "DAYS_REGISTRATION": -1460,
  "DAYS_LAST_PHONE_CHANGE": -540
}


EXACT DICT THAT WILL BE STORED IN CUST_INFO
{
  "application_id": "<generated_uuid>",

  "application_date": "2026-03-26",
  "application_channel": "web_portal",
  "product_type": "personal_loan",
  "currency": "INR",

  "full_name": "Ravi Kumar",
  "first_name": "Ravi",
  "middle_name": "",
  "last_name": "Kumar",
  "gender": "male",
  "date_of_birth": "1998-08-14",
  "age_years": 27,
  "marital_status": "married",
  "education_qualification": "higher_education",
  "nationality": "Indian",
  "pan_number": "ABCDE1234F",
  "aadhaar_last4": "4521",
  "government_id_type": "PAN",
  "government_id_number": "ABCDE1234F",
  "id_document_age_years": 6,

  "family_members_count": 4,
  "children_count": 1,
  "dependents_count": 2,
  "spouse_name": "Priya Kumar",

  "employment_status": "salaried",
  "occupation_type": "software_engineer",
  "employer_name": "TechNova Pvt Ltd",
  "work_experience_years_total": 5,
  "employment_years_current_job": 3,
  "monthly_income": 75000,
  "annual_income_total": 900000,
  "has_employment_phone": true,

  "owns_car": true,
  "owns_property": true,

  "email": "ravi.kumar@example.com",
  "primary_phone": "+91-9876543210",
  "alternate_phone": "+91-9123456780",
  "has_personal_phone": true,
  "has_mobile": true,
  "mobile_contactable": true,
  "has_email": true,
  "months_since_last_phone_change": 18,

  "current_address_line_1": "Flat 302, Green Heights",
  "current_address_line_2": "College Road",
  "city": "Nashik",
  "state": "Maharashtra",
  "postal_code": "422005",
  "country": "India",
  "years_at_current_address": 4,
  "region_rating_client": 2,
  "city_rating_differs_from_region": false,
  "registered_city_not_live_city": false,
  "registered_city_not_work_city": false,
  "live_city_not_work_city": false,

  "loan_purpose": "home_appliance_purchase",
  "loan_amount": 350000,
  "loan_tenure_months": 36,
  "goods_price": 320000,

  "total_documents_submitted": 5,
  "submitted_documents": [
    "PAN Card",
    "Aadhaar Card",
    "Salary Slips",
    "Bank Statement",
    "Address Proof"
  ],

  "bureau_inquiries_last_12_months": 2,
  "bureau_inquiries_last_month": 0,
  "previous_approved_loan_count": 1,
  "previous_refused_loan_count": 0,
  "late_installment_payments_total": 1,
  "late_payments_last_6_months": 0,
  "lowest_payment_ratio": 0.95,
  "average_payment_ratio": 0.99,
  "bureau_credit_lines_total": 3,
  "bureau_worst_status": 1,
  "bureau_closed_credit_count": 1,
  "bureau_overdue_sum": 0,
  "bureau_total_debt": 85000,
  "credit_card_max_dpd": 0,

  "ext_source_1": 0.62,
  "ext_source_2": 0.71,
  "ext_source_3": 0.68
}
    """