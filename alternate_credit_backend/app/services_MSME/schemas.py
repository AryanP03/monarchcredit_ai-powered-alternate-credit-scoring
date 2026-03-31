

from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional
import numpy as np

# --- 1. Define Sub-Schemas ---

class BusinessIdentity(BaseModel):
    company_name: str
    entity_type: str
    registration_number: str
    founded_year: int
    company_age_years: int
    msme_size_category: int
    sector_id: int
    business_description: str
    is_active: bool

class LoanRequirements(BaseModel):
    loan_amt:int
    tenure: int

class CapitalStructure(BaseModel):
    paid_in_capital: int
    registered_capital: int

class RiskMetrics(BaseModel):
    region_risk_score: float
    debt_to_equity_ratio: Optional[float] = None
    current_ratio: Optional[float] = None

class Taxation(BaseModel):
    total_overdue_tax: int
    tax_filing_compliance_score: float
    gst_status: Optional[str] = None
    pan_verified: Optional[bool] = None

class FinancialFoundation(BaseModel):
    capital_structure: CapitalStructure
    risk_metrics: RiskMetrics
    taxation: Taxation


class OwnershipGovernance(BaseModel):
    cap_table: dict
    management: dict
    infrastructure: dict

class LegalHistory(BaseModel):
    corporate_governance: dict
    litigation_summary: dict

class IPRatings(BaseModel):
    ip_portfolio: dict
    external_ratings: dict

class OperationalIntelligence(BaseModel):
    trends: dict
    payment_behavior: dict
    banking_health: dict

# --- 2. Main MSME Schema ---

class MSMESchema(BaseModel):
    business_identity: BusinessIdentity
    loan_requirements: LoanRequirements
    financial_foundation: FinancialFoundation
    ownership_and_governance: OwnershipGovernance
    legal_and_regulatory_history: LegalHistory
    intellectual_property_and_ratings: IPRatings
    operational_behavioral_intelligence: OperationalIntelligence

# --- 3. The Extraction & Order Logic ---

def extract_and_verify_features(json_data: dict):
    try:
        # Step A: Validate Schema
        validated_data = MSMESchema(**json_data).dict()
        print("✅ JSON Schema Validation Successful!")
        
        # Step B: Map to the 37 features (Flattening the nested JSON)
        f = {}
        
        # Ownership
        f['SH_num'] = validated_data['ownership_and_governance']['cap_table']['shareholders_count']
        f['MS_num'] = validated_data['ownership_and_governance']['management']['management_staff_count']
        f['avg_insured_employees'] = validated_data['ownership_and_governance']['management']['avg_insured_employees']
        f['Branch_num'] = validated_data['ownership_and_governance']['infrastructure']['total_branches']
        
        # Identity
        f['msme_size_category'] = validated_data['business_identity']['msme_size_category']
        f['company_age_years'] = validated_data['business_identity']['company_age_years']
        
        # Legal (Litigation Summary)
        lit = validated_data['legal_and_regulatory_history']['litigation_summary']
        f['total_court_executions'] = lit['court_executions']['total_ever']
        f['recent_court_executions'] = lit['court_executions']['last_12_months']
        f['total_admin_penalties'] = lit['administrative_penalties']['total_ever']
        f['recent_admin_penalties'] = lit['administrative_penalties']['last_12_months']
        f['total_legal_proceedings'] = lit['legal_proceedings']['total_ever']
        f['recent_legal_proceedings'] = lit['legal_proceedings']['last_12_months']
        f['total_case_filings'] = lit['total_case_filings']
        
        # Financials
        f['total_overdue_tax'] = validated_data['financial_foundation']['taxation']['total_overdue_tax']
        f['tax_filing_compliance_score'] = validated_data['financial_foundation']['taxation']['tax_filing_compliance_score']
        f['region_risk_score'] = validated_data['financial_foundation']['risk_metrics']['region_risk_score']
        
        # Corporate Activity
        cg = validated_data['legal_and_regulatory_history']['corporate_governance']
        f['total_corporate_changes'] = cg['total_corporate_changes']
        f['recent_corporate_changes'] = cg['recent_corporate_changes']
        
        # IP & Ratings
        ip = validated_data['intellectual_property_and_ratings']['ip_portfolio']
        f['total_trademarks'] = ip['total_trademarks']
        f['total_patents'] = ip['total_patents']
        f['total_copyrights'] = ip['total_copyrights']
        f['total_certifications'] = ip['total_certifications']
        f['total_ip_score'] = ip['composite_ip_score']
        f['Ratepaying_Credit_Grade_A_num'] = validated_data['intellectual_property_and_ratings']['external_ratings']['grade_a_credit_ratings']
        
        # Trends & Behavior
        op = validated_data['operational_behavioral_intelligence']
        f['insurance_trend'] = op['trends']['insurance_trend_index']
        pay = op['payment_behavior']
        f['salary_disbursement_regularity_score'] = pay['salary_disbursement_regularity']
        f['account_balance_volatility_score'] = pay['account_balance_stability']
        f['loan_repayment_history_score'] = pay['loan_repayment_history']
        f['vendor_payment_delay_trend'] = pay['vendor_payment_delay_trend']
        f['utility_bill_payment_score'] = pay['utility_bill_payment_regularity']
        f['digital_payment_ratio'] = pay['digital_transaction_ratio']
        
        # Logs & Sectors
        f['log_paid_in_capital'] = np.log1p(validated_data['financial_foundation']['capital_structure']['paid_in_capital'])
        f['log_registered_capital'] = np.log1p(validated_data['financial_foundation']['capital_structure']['registered_capital'])
        
        sid = validated_data['business_identity']['sector_id']
        f['sector_Construction'] = 1 if sid == 1 else 0
        f['sector_Services']     = 1 if sid == 2 else 0
        f['sector_Tech']         = 1 if sid == 3 else 0
        f['sector_Trade']        = 1 if sid == 4 else 0

        # Step C: Final Ordered List (Matches your model's 37 features)
        final_feature_order = [
            'SH_num', 'MS_num', 'Branch_num', 'Ratepaying_Credit_Grade_A_num', 
            'msme_size_category', 'company_age_years', 'total_corporate_changes', 
            'recent_corporate_changes', 'total_court_executions', 'recent_court_executions', 
            'total_overdue_tax', 'total_admin_penalties', 'recent_admin_penalties', 
            'total_legal_proceedings', 'recent_legal_proceedings', 'total_case_filings', 
            'total_trademarks', 'total_patents', 'total_copyrights', 'total_certifications', 
            'total_ip_score', 'avg_insured_employees', 'insurance_trend', 
            'log_registered_capital', 'log_paid_in_capital', 'region_risk_score', 
            'sector_Construction', 'sector_Services', 'sector_Tech', 'sector_Trade', 
            'salary_disbursement_regularity_score', 'account_balance_volatility_score', 
            'loan_repayment_history_score', 'vendor_payment_delay_trend', 
            'utility_bill_payment_score', 'tax_filing_compliance_score', 'digital_payment_ratio'
        ]
        
        return np.array([f[col] for col in final_feature_order]).reshape(1, -1)

    except ValidationError as e:
        print(f"❌ Schema Error: {e.json()}")
        return None