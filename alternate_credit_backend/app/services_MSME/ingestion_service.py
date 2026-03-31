from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, Dict, Tuple
from .audit_service_MSME import audit_service

from app.messaging.producer import JSONKafkaProducer


class MSMEIngestionService:
    RAW_MSME_DATA = "raw_msme_data_injested"
    MSME_INFO = "msme_info"

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
        Receives already-validated MSME payload from main.py,
        generates application_id,
        splits payload into:
          1. model-required raw MSME data
          2. non-model business/info data
        then pushes both to Kafka.
        """
        application_id = uuid.uuid4().hex

        raw_msme_payload, msme_info_payload = self._split_payload(
            payload=payload,
            application_id=application_id
        )
        audit_service.process_ingestion_payload(msme_info_payload)
        self.producer.send(self.RAW_MSME_DATA, raw_msme_payload)
        self.producer.send(self.MSME_INFO, msme_info_payload)

        return {
            "application_id": application_id,
            "status": "submitted",
            "message": "MSME payload successfully published to Kafka",
            "published_topics": {
                "raw_msme_data": self.RAW_MSME_DATA,
                "msme_info": self.MSME_INFO
            }
        }

    def _split_payload(
        self,
        payload: Dict[str, Any],
        application_id: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Splits incoming validated MSME JSON into:
        - RAW_MSME_DATA: only model-required fields
        - MSME_INFO: remaining non-model fields / descriptive business info
        """
        sector_id = payload["business_identity"]["sector_id"]

        raw_msme_payload = {
            "application_id": application_id,

            # ---------------------------------------------------
            # 0. Loan Requirements (2)
            # ---------------------------------------------------
            "loan_amt": payload["loan_requirements"]["loan_amt"],
            "tenure": payload["loan_requirements"]["tenure"],

            # ---------------------------------------------------
            # 1. Company Basics (5)
            # ---------------------------------------------------
            "msme_size_category": payload["business_identity"]["msme_size_category"],
            "company_age_years": payload["business_identity"]["company_age_years"],
            "paid_in_capital": payload["financial_foundation"]["capital_structure"]["paid_in_capital"],
            "registered_capital": payload["financial_foundation"]["capital_structure"]["registered_capital"],
            "region_risk_score": payload["financial_foundation"]["risk_metrics"]["region_risk_score"],

            # ---------------------------------------------------
            # 2. Sector One-Hot (4)
            # ---------------------------------------------------
            "sector_Construction": 1 if sector_id == 1 else 0,
            "sector_Services": 1 if sector_id == 2 else 0,
            "sector_Tech": 1 if sector_id == 3 else 0,
            "sector_Trade": 1 if sector_id == 4 else 0,

            # ---------------------------------------------------
            # 3. Ownership & Management (4)
            # ---------------------------------------------------
            "SH_num": payload["ownership_and_governance"]["cap_table"]["shareholders_count"],
            "MS_num": payload["ownership_and_governance"]["management"]["management_staff_count"],
            "avg_insured_employees": payload["ownership_and_governance"]["management"]["avg_insured_employees"],
            "Branch_num": payload["ownership_and_governance"]["infrastructure"]["total_branches"],

            # ---------------------------------------------------
            # 4. Legal & Compliance (8)
            # ---------------------------------------------------
            "total_court_executions": payload["legal_and_regulatory_history"]["litigation_summary"]["court_executions"]["total_ever"],
            "recent_court_executions": payload["legal_and_regulatory_history"]["litigation_summary"]["court_executions"]["last_12_months"],
            "total_overdue_tax": payload["financial_foundation"]["taxation"]["total_overdue_tax"],
            "total_admin_penalties": payload["legal_and_regulatory_history"]["litigation_summary"]["administrative_penalties"]["total_ever"],
            "recent_admin_penalties": payload["legal_and_regulatory_history"]["litigation_summary"]["administrative_penalties"]["last_12_months"],
            "total_legal_proceedings": payload["legal_and_regulatory_history"]["litigation_summary"]["legal_proceedings"]["total_ever"],
            "recent_legal_proceedings": payload["legal_and_regulatory_history"]["litigation_summary"]["legal_proceedings"]["last_12_months"],
            "total_case_filings": payload["legal_and_regulatory_history"]["litigation_summary"]["total_case_filings"],

            # ---------------------------------------------------
            # 5. Corporate Activity (2)
            # ---------------------------------------------------
            "total_corporate_changes": payload["legal_and_regulatory_history"]["corporate_governance"]["total_corporate_changes"],
            "recent_corporate_changes": payload["legal_and_regulatory_history"]["corporate_governance"]["recent_corporate_changes"],

            # ---------------------------------------------------
            # 6. IP & Ratings (6)
            # ---------------------------------------------------
            "total_trademarks": payload["intellectual_property_and_ratings"]["ip_portfolio"]["total_trademarks"],
            "total_patents": payload["intellectual_property_and_ratings"]["ip_portfolio"]["total_patents"],
            "total_copyrights": payload["intellectual_property_and_ratings"]["ip_portfolio"]["total_copyrights"],
            "total_certifications": payload["intellectual_property_and_ratings"]["ip_portfolio"]["total_certifications"],
            "total_ip_score": payload["intellectual_property_and_ratings"]["ip_portfolio"]["composite_ip_score"],
            "Ratepaying_Credit_Grade_A_num": payload["intellectual_property_and_ratings"]["external_ratings"]["grade_a_credit_ratings"],

            # ---------------------------------------------------
            # 7. Insurance & Trends (1)
            # ---------------------------------------------------
            "insurance_trend": payload["operational_behavioral_intelligence"]["trends"]["insurance_trend_index"],

            # ---------------------------------------------------
            # 8. Behavioral Data (7)
            # ---------------------------------------------------
            "salary_disbursement_regularity_score": payload["operational_behavioral_intelligence"]["payment_behavior"]["salary_disbursement_regularity"],
            "account_balance_volatility_score": payload["operational_behavioral_intelligence"]["payment_behavior"]["account_balance_stability"],
            "loan_repayment_history_score": payload["operational_behavioral_intelligence"]["payment_behavior"]["loan_repayment_history"],
            "vendor_payment_delay_trend": payload["operational_behavioral_intelligence"]["payment_behavior"]["vendor_payment_delay_trend"],
            "utility_bill_payment_score": payload["operational_behavioral_intelligence"]["payment_behavior"]["utility_bill_payment_regularity"],
            "tax_filing_compliance_score": payload["financial_foundation"]["taxation"]["tax_filing_compliance_score"],
            "digital_payment_ratio": payload["operational_behavioral_intelligence"]["payment_behavior"]["digital_transaction_ratio"],
        }

        msme_info_payload = self._build_msme_info_payload(
            payload=payload,
            application_id=application_id
        )

        return raw_msme_payload, msme_info_payload

    def _build_msme_info_payload(
        self,
        payload: Dict[str, Any],
        application_id: str
    ) -> Dict[str, Any]:
        """
        Stores the remaining non-model descriptive information.
        Uses safe reads so missing optional fields do not crash ingestion.
        """
        info_payload = {
            "application_id": application_id,

            "business_identity": {
                "company_name": self._safe_get(payload, "business_identity", "company_name"),
                "entity_type": self._safe_get(payload, "business_identity", "entity_type"),
                "registration_number": self._safe_get(payload, "business_identity", "registration_number"),
                "founded_year": self._safe_get(payload, "business_identity", "founded_year"),
                "sector_id": self._safe_get(payload, "business_identity", "sector_id"),
                "business_description": self._safe_get(payload, "business_identity", "business_description"),
                "is_active": self._safe_get(payload, "business_identity", "is_active"),
            },

            "loan_requirements": {
                "loan_amt": self._safe_get(payload, "loan_requirements", "loan_amt"),
                "tenure": self._safe_get(payload, "loan_requirements", "tenure"),
            },

            "financial_foundation": {
                "capital_structure": {
                    # REPLACE authorized_capital with the fields you actually have
                    "paid_in_capital": self._safe_get(payload, "financial_foundation", "capital_structure", "paid_in_capital"),
                    "registered_capital": self._safe_get(payload, "financial_foundation", "capital_structure", "registered_capital"),
                },
                "risk_metrics": {
                    # Optional / may be missing in current sample
                    "region_risk_score": self._safe_get(payload, "financial_foundation", "risk_metrics", "region_risk_score"),
                    "debt_to_equity_ratio": self._safe_get(payload, "financial_foundation", "risk_metrics", "debt_to_equity_ratio"),
                    "current_ratio": self._safe_get(payload, "financial_foundation", "risk_metrics", "current_ratio"),
                },
                "taxation": {
                    "total_overdue_tax": self._safe_get(payload, "financial_foundation", "taxation", "total_overdue_tax"),
                    "tax_filing_compliance_score": self._safe_get(payload, "financial_foundation", "taxation", "tax_filing_compliance_score"),
                    "gst_status": self._safe_get(payload, "financial_foundation", "taxation", "gst_status"),
                    "pan_verified": self._safe_get(payload, "financial_foundation", "taxation", "pan_verified"),
                },
            },

            "ownership_and_governance": {
                "cap_table": {
                    "shareholders_count": self._safe_get(payload, "ownership_and_governance", "cap_table", "shareholders_count"),
                    "equity_split": self._safe_get(payload, "ownership_and_governance", "cap_table", "equity_split"),
                    "is_pledged_shares": self._safe_get(payload, "ownership_and_governance", "cap_table", "is_pledged_shares"),
                },
                "management": {
                    "management_staff_count": self._safe_get(payload, "ownership_and_governance", "management", "management_staff_count"),
                    "avg_insured_employees": self._safe_get(payload, "ownership_and_governance", "management", "avg_insured_employees"),
                    "board_members": self._safe_get(payload, "ownership_and_governance", "management", "board_members"),
                },
                "infrastructure": {
                    "total_branches": self._safe_get(payload, "ownership_and_governance", "infrastructure", "total_branches"),
                    "headquarters_address": self._safe_get(payload, "ownership_and_governance", "infrastructure", "headquarters_address"),
                },
            },

            "legal_and_regulatory_history": {
                "corporate_governance": {
                    "total_corporate_changes": self._safe_get(payload, "legal_and_regulatory_history", "corporate_governance", "total_corporate_changes"),
                    "recent_corporate_changes": self._safe_get(payload, "legal_and_regulatory_history", "corporate_governance", "recent_corporate_changes"),
                },
                "litigation_summary": self._safe_get(payload, "legal_and_regulatory_history", "litigation_summary", default={}),
            },

            "intellectual_property_and_ratings": {
                "ip_portfolio": self._safe_get(payload, "intellectual_property_and_ratings", "ip_portfolio", default={}),
                "external_ratings": {
                    "grade_a_credit_ratings": self._safe_get(payload, "intellectual_property_and_ratings", "external_ratings", "grade_a_credit_ratings"),
                    "iso_certified": self._safe_get(payload, "intellectual_property_and_ratings", "external_ratings", "iso_certified"),
                },
            },

            "operational_behavioral_intelligence": {
                "trends": {
                    "insurance_trend_index": self._safe_get(payload, "operational_behavioral_intelligence", "trends", "insurance_trend_index"),
                    "revenue_growth_trend": self._safe_get(payload, "operational_behavioral_intelligence", "trends", "revenue_growth_trend"),
                },
                "payment_behavior": self._safe_get(payload, "operational_behavioral_intelligence", "payment_behavior", default={}),
                "banking_health": {
                    "avg_monthly_balance": self._safe_get(payload, "operational_behavioral_intelligence", "banking_health", "avg_monthly_balance"),
                    "cheque_bounce_incidents": self._safe_get(payload, "operational_behavioral_intelligence", "banking_health", "cheque_bounce_incidents"),
                },
            }
        }

        return info_payload
    
    """
EXACT LIST OF WHAT IS STORED IN: RAW_MSME_DATA = "raw_msme_data_injested"
application_id

loan_amt
tenure

msme_size_category
company_age_years
paid_in_capital
registered_capital
region_risk_score

sector_Construction
sector_Services
sector_Tech
sector_Trade

SH_num
MS_num
avg_insured_employees
Branch_num

total_court_executions
recent_court_executions
total_overdue_tax
total_admin_penalties
recent_admin_penalties
total_legal_proceedings
recent_legal_proceedings
total_case_filings

total_corporate_changes
recent_corporate_changes

total_trademarks
total_patents
total_copyrights
total_certifications
total_ip_score
Ratepaying_Credit_Grade_A_num

insurance_trend

salary_disbursement_regularity_score
account_balance_volatility_score
loan_repayment_history_score
vendor_payment_delay_trend
utility_bill_payment_score
tax_filing_compliance_score
digital_payment_ratio
    """

"""


EXACT LIST OF WHAT IS STORED IN: MSME_INFO = "msme_info"
application_id

business_identity:
    company_name
    entity_type
    registration_number
    founded_year
    sector_id
    business_description
    is_active

loan_requirements:
    loan_amt
    tenure

financial_foundation:
    capital_structure:
        paid_in_capital
        registered_capital
    risk_metrics:
        region_risk_score
        debt_to_equity_ratio
        current_ratio
    taxation:
        total_overdue_tax
        tax_filing_compliance_score
        gst_status
        pan_verified

ownership_and_governance:
    cap_table:
        shareholders_count
        equity_split
        is_pledged_shares
    management:
        management_staff_count
        avg_insured_employees
        board_members
    infrastructure:
        total_branches
        headquarters_address

legal_and_regulatory_history:
    corporate_governance:
        total_corporate_changes
        recent_corporate_changes
    litigation_summary

intellectual_property_and_ratings:
    ip_portfolio
    external_ratings:
        grade_a_credit_ratings
        iso_certified

operational_behavioral_intelligence:
    trends:
        insurance_trend_index
        revenue_growth_trend
    payment_behavior
    banking_health:
        avg_monthly_balance
        cheque_bounce_incidents

"""