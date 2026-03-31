from datetime import date
from typing import List, Literal, Optional

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator


# ==========================================================
# APPLICATION META
# ==========================================================
class ApplicationMeta(BaseModel):
    application_id: str = Field(..., min_length=3, max_length=100)
    application_date: date
    application_channel: str = Field(..., min_length=2, max_length=50)
    product_type: str = Field(..., min_length=2, max_length=50)
    currency: Literal["INR"]


# ==========================================================
# CUSTOMER PROFILE
# ==========================================================
class PersonalDetails(BaseModel):
    full_name: str = Field(..., min_length=2, max_length=255)
    first_name: str = Field(..., min_length=1, max_length=100)
    middle_name: Optional[str] = Field(default="")
    last_name: str = Field(..., min_length=1, max_length=100)

    gender: Literal["male", "female", "other"]
    date_of_birth: date
    age_years: int = Field(..., ge=18, le=100)

    marital_status: str = Field(..., min_length=2, max_length=50)
    education_qualification: str = Field(..., min_length=2, max_length=100)
    nationality: str = Field(..., min_length=2, max_length=100)

    pan_number: str = Field(..., min_length=10, max_length=10)
    aadhaar_last4: str = Field(..., min_length=4, max_length=4)
    government_id_type: str = Field(..., min_length=2, max_length=50)
    government_id_number: str = Field(..., min_length=2, max_length=100)

    id_document_age_years: float = Field(..., ge=0, le=80)

    @field_validator("pan_number")
    @classmethod
    def validate_pan(cls, v: str) -> str:
        v = v.strip().upper()
        if len(v) != 10:
            raise ValueError("pan_number must be exactly 10 characters")
        return v

    @field_validator("aadhaar_last4")
    @classmethod
    def validate_aadhaar_last4(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError("aadhaar_last4 must contain only digits")
        return v


class FamilyDetails(BaseModel):
    family_members_count: int = Field(..., ge=1, le=20)
    children_count: int = Field(..., ge=0, le=20)
    dependents_count: int = Field(..., ge=0, le=20)
    spouse_name: Optional[str] = None


class EmploymentDetails(BaseModel):
    employment_status: str = Field(..., min_length=2, max_length=50)
    occupation_type: str = Field(..., min_length=2, max_length=100)
    employer_name: Optional[str] = None

    work_experience_years_total: float = Field(..., ge=0, le=60)
    employment_years_current_job: float = Field(..., ge=0, le=50)

    monthly_income: float = Field(..., ge=0)
    annual_income_total: float = Field(..., ge=0)

    has_employment_phone: bool


class AssetsAndOwnership(BaseModel):
    owns_car: bool
    owns_property: bool


class CustomerProfile(BaseModel):
    personal_details: PersonalDetails
    family_details: FamilyDetails
    employment_details: EmploymentDetails
    assets_and_ownership: AssetsAndOwnership


# ==========================================================
# CONTACT / ADDRESS / LOAN
# ==========================================================
class ContactDetails(BaseModel):
    email: EmailStr
    primary_phone: str = Field(..., min_length=8, max_length=20)
    alternate_phone: Optional[str] = None

    has_personal_phone: bool
    has_mobile: bool
    mobile_contactable: bool
    has_email: bool

    months_since_last_phone_change: float = Field(..., ge=0, le=1000)


class AddressDetails(BaseModel):
    current_address_line_1: str = Field(..., min_length=2, max_length=255)
    current_address_line_2: Optional[str] = Field(default="")
    city: str = Field(..., min_length=2, max_length=100)
    state: str = Field(..., min_length=2, max_length=100)
    postal_code: str = Field(..., min_length=3, max_length=20)
    country: str = Field(..., min_length=2, max_length=100)

    years_at_current_address: float = Field(..., ge=0, le=80)
    region_rating_client: int = Field(..., ge=1, le=3)

    city_rating_differs_from_region: bool
    registered_city_not_live_city: bool
    registered_city_not_work_city: bool
    live_city_not_work_city: bool


class LoanDetails(BaseModel):
    loan_purpose: str = Field(..., min_length=2, max_length=255)
    loan_amount: float = Field(..., ge=1)
    goods_price: float = Field(..., ge=0)
    loan_tenure_months: int = Field(..., ge=1, le=600)


class DocumentsSubmitted(BaseModel):
    total_documents_submitted: int = Field(..., ge=0, le=100)
    submitted_documents: List[str]


# ==========================================================
# CREDIT HISTORY / EXTERNAL SCORES
# ==========================================================
class CreditHistory(BaseModel):
    bureau_inquiries_last_12_months: int = Field(..., ge=0)
    bureau_inquiries_last_month: int = Field(..., ge=0)
    previous_approved_loan_count: int = Field(..., ge=0)
    previous_refused_loan_count: int = Field(..., ge=0)

    late_installment_payments_total: int = Field(..., ge=0)
    late_payments_last_6_months: int = Field(..., ge=0)

    lowest_payment_ratio: float = Field(..., ge=0, le=1)
    average_payment_ratio: float = Field(..., ge=0, le=1)

    bureau_credit_lines_total: int = Field(..., ge=0)
    bureau_worst_status: int = Field(..., ge=0, le=5)
    bureau_closed_credit_count: int = Field(..., ge=0)

    bureau_overdue_sum: float = Field(..., ge=0)
    bureau_total_debt: float = Field(..., ge=0)
    credit_card_max_dpd: int = Field(..., ge=0)


class ExternalCreditScores(BaseModel):
    ext_source_1: Optional[float] = Field(None, ge=0, le=1)
    ext_source_2: Optional[float] = Field(None, ge=0, le=1)
    ext_source_3: Optional[float] = Field(None, ge=0, le=1)


# ==========================================================
# RAW MODEL INPUT FIELDS
# ==========================================================
class RawModelInputFields(BaseModel):
    AMT_CREDIT: float = Field(..., ge=1)
    AMT_ANNUITY: float = Field(..., ge=0)
    AMT_GOODS_PRICE: float = Field(..., ge=0)
    AMT_INCOME_TOTAL: float = Field(..., ge=0)

    AGE_YEARS: int = Field(..., ge=18, le=100)
    EMPLOYMENT_YEARS: float = Field(..., ge=0, le=50)

    NAME_EDUCATION_TYPE: str = Field(..., min_length=2, max_length=100)

    FLAG_OWN_CAR: int = Field(..., ge=0, le=1)
    FLAG_OWN_REALTY: int = Field(..., ge=0, le=1)

    CNT_CHILDREN: int = Field(..., ge=0)
    CNT_FAM_MEMBERS: int = Field(..., ge=1)

    ID_DOCUMENT_AGE_YEARS: float = Field(..., ge=0)
    YEARS_AT_CURRENT_ADDRESS: float = Field(..., ge=0)

    REGION_RATING_CLIENT: int = Field(..., ge=1, le=3)
    REG_CITY_NOT_LIVE_CITY: int = Field(..., ge=0, le=1)
    REG_CITY_NOT_WORK_CITY: int = Field(..., ge=0, le=1)
    LIVE_CITY_NOT_WORK_CITY: int = Field(..., ge=0, le=1)

    FLAG_EMP_PHONE: int = Field(..., ge=0, le=1)
    FLAG_PHONE: int = Field(..., ge=0, le=1)
    FLAG_MOBIL: int = Field(..., ge=0, le=1)
    FLAG_CONT_MOBILE: int = Field(..., ge=0, le=1)
    FLAG_EMAIL: int = Field(..., ge=0, le=1)

    MONTHS_SINCE_LAST_PHONE_CHANGE: float = Field(..., ge=0)
    TOTAL_DOCS_SUBMITTED: int = Field(..., ge=0)

    AMT_REQ_CREDIT_BUREAU_YEAR: int = Field(..., ge=0)
    AMT_REQ_CREDIT_BUREAU_MON: int = Field(..., ge=0)

    PREV_APPROVED_COUNT: int = Field(..., ge=0)
    PREV_REFUSED_COUNT: int = Field(..., ge=0)

    INST_LATE_COUNT: int = Field(..., ge=0)
    INST_RECENT_LATE: int = Field(..., ge=0)
    INST_RATIO_MIN: float = Field(..., ge=0, le=1)
    INST_RATIO_MEAN: float = Field(..., ge=0, le=1)

    BUREAU_CREDIT_COUNT: int = Field(..., ge=0)
    BUREAU_WORST_STATUS: int = Field(..., ge=0, le=5)
    BUREAU_CLOSED_COUNT: int = Field(..., ge=0)

    BUREAU_OVERDUE_SUM: float = Field(..., ge=0)
    BUREAU_TOTAL_DEBT: float = Field(..., ge=0)
    CC_DPD_MAX: int = Field(..., ge=0)

    EXT_SOURCE_1: Optional[float] = Field(None, ge=0, le=1)
    EXT_SOURCE_2: Optional[float] = Field(None, ge=0, le=1)
    EXT_SOURCE_3: Optional[float] = Field(None, ge=0, le=1)


# ==========================================================
# INTERNAL DERIVED FIELDS
# ==========================================================
class InternalDerivedFields(BaseModel):
    DAYS_BIRTH: int
    DAYS_EMPLOYED: int
    DAYS_ID_PUBLISH: int
    DAYS_REGISTRATION: int
    DAYS_LAST_PHONE_CHANGE: int


# ==========================================================
# MAIN REQUEST SCHEMA
# ==========================================================
class IndividualLoanApplicationRequest(BaseModel):
    application_meta: ApplicationMeta
    customer_profile: CustomerProfile
    contact_details: ContactDetails
    address_details: AddressDetails
    loan_details: LoanDetails
    documents_submitted: DocumentsSubmitted
    credit_history: CreditHistory
    external_credit_scores: ExternalCreditScores
    raw_model_input_fields: RawModelInputFields
    internal_derived_fields: InternalDerivedFields

    @model_validator(mode="after")
    def cross_validate_consistency(self):
        pd = self.customer_profile.personal_details
        fd = self.customer_profile.family_details
        ed = self.customer_profile.employment_details
        ao = self.customer_profile.assets_and_ownership
        cd = self.contact_details
        ad = self.address_details
        ld = self.loan_details
        docs = self.documents_submitted
        ch = self.credit_history
        ecs = self.external_credit_scores
        raw = self.raw_model_input_fields
        derived = self.internal_derived_fields

        if raw.AMT_CREDIT != ld.loan_amount:
            raise ValueError("raw_model_input_fields.AMT_CREDIT must match loan_details.loan_amount")

        if raw.AMT_GOODS_PRICE != ld.goods_price:
            raise ValueError("raw_model_input_fields.AMT_GOODS_PRICE must match loan_details.goods_price")

        if raw.AMT_INCOME_TOTAL != ed.annual_income_total:
            raise ValueError("raw_model_input_fields.AMT_INCOME_TOTAL must match employment_details.annual_income_total")

        if raw.AGE_YEARS != pd.age_years:
            raise ValueError("raw_model_input_fields.AGE_YEARS must match personal_details.age_years")

        if raw.EMPLOYMENT_YEARS != ed.employment_years_current_job:
            raise ValueError("raw_model_input_fields.EMPLOYMENT_YEARS must match employment_details.employment_years_current_job")

        if raw.NAME_EDUCATION_TYPE != pd.education_qualification:
            raise ValueError("raw_model_input_fields.NAME_EDUCATION_TYPE must match personal_details.education_qualification")

        if raw.FLAG_OWN_CAR != int(ao.owns_car):
            raise ValueError("raw_model_input_fields.FLAG_OWN_CAR must match assets_and_ownership.owns_car")

        if raw.FLAG_OWN_REALTY != int(ao.owns_property):
            raise ValueError("raw_model_input_fields.FLAG_OWN_REALTY must match assets_and_ownership.owns_property")

        if raw.CNT_CHILDREN != fd.children_count:
            raise ValueError("raw_model_input_fields.CNT_CHILDREN must match family_details.children_count")

        if raw.CNT_FAM_MEMBERS != fd.family_members_count:
            raise ValueError("raw_model_input_fields.CNT_FAM_MEMBERS must match family_details.family_members_count")

        if raw.ID_DOCUMENT_AGE_YEARS != pd.id_document_age_years:
            raise ValueError("raw_model_input_fields.ID_DOCUMENT_AGE_YEARS must match personal_details.id_document_age_years")

        if raw.YEARS_AT_CURRENT_ADDRESS != ad.years_at_current_address:
            raise ValueError("raw_model_input_fields.YEARS_AT_CURRENT_ADDRESS must match address_details.years_at_current_address")

        if raw.REGION_RATING_CLIENT != ad.region_rating_client:
            raise ValueError("raw_model_input_fields.REGION_RATING_CLIENT must match address_details.region_rating_client")

        if raw.REG_CITY_NOT_LIVE_CITY != int(ad.registered_city_not_live_city):
            raise ValueError("raw_model_input_fields.REG_CITY_NOT_LIVE_CITY must match address_details.registered_city_not_live_city")

        if raw.REG_CITY_NOT_WORK_CITY != int(ad.registered_city_not_work_city):
            raise ValueError("raw_model_input_fields.REG_CITY_NOT_WORK_CITY must match address_details.registered_city_not_work_city")

        if raw.LIVE_CITY_NOT_WORK_CITY != int(ad.live_city_not_work_city):
            raise ValueError("raw_model_input_fields.LIVE_CITY_NOT_WORK_CITY must match address_details.live_city_not_work_city")

        if raw.FLAG_EMP_PHONE != int(ed.has_employment_phone):
            raise ValueError("raw_model_input_fields.FLAG_EMP_PHONE must match employment_details.has_employment_phone")

        if raw.FLAG_PHONE != int(cd.has_personal_phone):
            raise ValueError("raw_model_input_fields.FLAG_PHONE must match contact_details.has_personal_phone")

        if raw.FLAG_MOBIL != int(cd.has_mobile):
            raise ValueError("raw_model_input_fields.FLAG_MOBIL must match contact_details.has_mobile")

        if raw.FLAG_CONT_MOBILE != int(cd.mobile_contactable):
            raise ValueError("raw_model_input_fields.FLAG_CONT_MOBILE must match contact_details.mobile_contactable")

        if raw.FLAG_EMAIL != int(cd.has_email):
            raise ValueError("raw_model_input_fields.FLAG_EMAIL must match contact_details.has_email")

        if raw.MONTHS_SINCE_LAST_PHONE_CHANGE != cd.months_since_last_phone_change:
            raise ValueError("raw_model_input_fields.MONTHS_SINCE_LAST_PHONE_CHANGE must match contact_details.months_since_last_phone_change")

        if raw.TOTAL_DOCS_SUBMITTED != docs.total_documents_submitted:
            raise ValueError("raw_model_input_fields.TOTAL_DOCS_SUBMITTED must match documents_submitted.total_documents_submitted")

        if raw.AMT_REQ_CREDIT_BUREAU_YEAR != ch.bureau_inquiries_last_12_months:
            raise ValueError("raw_model_input_fields.AMT_REQ_CREDIT_BUREAU_YEAR must match credit_history.bureau_inquiries_last_12_months")

        if raw.AMT_REQ_CREDIT_BUREAU_MON != ch.bureau_inquiries_last_month:
            raise ValueError("raw_model_input_fields.AMT_REQ_CREDIT_BUREAU_MON must match credit_history.bureau_inquiries_last_month")

        if raw.PREV_APPROVED_COUNT != ch.previous_approved_loan_count:
            raise ValueError("raw_model_input_fields.PREV_APPROVED_COUNT must match credit_history.previous_approved_loan_count")

        if raw.PREV_REFUSED_COUNT != ch.previous_refused_loan_count:
            raise ValueError("raw_model_input_fields.PREV_REFUSED_COUNT must match credit_history.previous_refused_loan_count")

        if raw.INST_LATE_COUNT != ch.late_installment_payments_total:
            raise ValueError("raw_model_input_fields.INST_LATE_COUNT must match credit_history.late_installment_payments_total")

        if raw.INST_RECENT_LATE != ch.late_payments_last_6_months:
            raise ValueError("raw_model_input_fields.INST_RECENT_LATE must match credit_history.late_payments_last_6_months")

        if raw.INST_RATIO_MIN != ch.lowest_payment_ratio:
            raise ValueError("raw_model_input_fields.INST_RATIO_MIN must match credit_history.lowest_payment_ratio")

        if raw.INST_RATIO_MEAN != ch.average_payment_ratio:
            raise ValueError("raw_model_input_fields.INST_RATIO_MEAN must match credit_history.average_payment_ratio")

        if raw.BUREAU_CREDIT_COUNT != ch.bureau_credit_lines_total:
            raise ValueError("raw_model_input_fields.BUREAU_CREDIT_COUNT must match credit_history.bureau_credit_lines_total")

        if raw.BUREAU_WORST_STATUS != ch.bureau_worst_status:
            raise ValueError("raw_model_input_fields.BUREAU_WORST_STATUS must match credit_history.bureau_worst_status")

        if raw.BUREAU_CLOSED_COUNT != ch.bureau_closed_credit_count:
            raise ValueError("raw_model_input_fields.BUREAU_CLOSED_COUNT must match credit_history.bureau_closed_credit_count")

        if raw.BUREAU_OVERDUE_SUM != ch.bureau_overdue_sum:
            raise ValueError("raw_model_input_fields.BUREAU_OVERDUE_SUM must match credit_history.bureau_overdue_sum")

        if raw.BUREAU_TOTAL_DEBT != ch.bureau_total_debt:
            raise ValueError("raw_model_input_fields.BUREAU_TOTAL_DEBT must match credit_history.bureau_total_debt")

        if raw.CC_DPD_MAX != ch.credit_card_max_dpd:
            raise ValueError("raw_model_input_fields.CC_DPD_MAX must match credit_history.credit_card_max_dpd")

        if ecs.ext_source_1 is not None and raw.EXT_SOURCE_1 != ecs.ext_source_1:
            raise ValueError("raw_model_input_fields.EXT_SOURCE_1 must match external_credit_scores.ext_source_1")

        if ecs.ext_source_2 is not None and raw.EXT_SOURCE_2 != ecs.ext_source_2:
            raise ValueError("raw_model_input_fields.EXT_SOURCE_2 must match external_credit_scores.ext_source_2")

        if ecs.ext_source_3 is not None and raw.EXT_SOURCE_3 != ecs.ext_source_3:
            raise ValueError("raw_model_input_fields.EXT_SOURCE_3 must match external_credit_scores.ext_source_3")

        expected_days_birth = -(pd.age_years * 365)
        expected_days_employed = -(int(ed.employment_years_current_job * 365))
        expected_days_id_publish = -(int(pd.id_document_age_years * 365))
        expected_days_registration = -(int(ad.years_at_current_address * 365))
        expected_days_phone_change = -(int(cd.months_since_last_phone_change * 30))

        if derived.DAYS_BIRTH != expected_days_birth:
            raise ValueError("internal_derived_fields.DAYS_BIRTH is inconsistent with age_years")

        if derived.DAYS_EMPLOYED != expected_days_employed:
            raise ValueError("internal_derived_fields.DAYS_EMPLOYED is inconsistent with employment_years_current_job")

        if derived.DAYS_ID_PUBLISH != expected_days_id_publish:
            raise ValueError("internal_derived_fields.DAYS_ID_PUBLISH is inconsistent with id_document_age_years")

        if derived.DAYS_REGISTRATION != expected_days_registration:
            raise ValueError("internal_derived_fields.DAYS_REGISTRATION is inconsistent with years_at_current_address")

        if derived.DAYS_LAST_PHONE_CHANGE != expected_days_phone_change:
            raise ValueError("internal_derived_fields.DAYS_LAST_PHONE_CHANGE is inconsistent with months_since_last_phone_change")

        return self