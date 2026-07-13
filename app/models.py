import enum
import uuid
from datetime import datetime
from sqlalchemy.sql import expression
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, \
    UniqueConstraint, Uuid, func, Enum as SQLAlchemyEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import Float


class ValidationStatus(enum.Enum):
    VALIDATED = "VALIDATED"
    NOT_VALIDATED = "NOT_VALIDATED"
    PENDING = "PENDING"


class GroupIntervals(enum.Enum):
    HOUR = "HOUR"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


class FinalStatus(enum.Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"


class PeriodicSessionType(enum.Enum):
    NEW_SESSION = "NEW_SESSION"
    RETRY_SESSION = "RETRY SESSION"
    SKIPPED = "SKIPPED"


class FinalValidatedStatus(enum.Enum):
    VALIDATED = "VALIDATED"
    NOT_VALIDATED = "NOT_VALIDATED"
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    FAILED = "FAILED"
    AUTO_REJECT = "AUTO_REJECT"
    AUTO_ACCEPT = "AUTO_ACCEPT"
    REVIEW = "REVIEW"


class OribisMatchStatus(enum.Enum):
    MATCH = "MATCH"
    NO_MATCH = "NO_MATCH"
    PENDING = "PENDING"


class TruesightStatus(enum.Enum):
    VALIDATED = "VALIDATED"
    NOT_VALIDATED = "NOT_VALIDATED"
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    NO_MATCH = "NO_MATCH"


class STATUS(str, enum.Enum):
    QUEUED = "QUEUED"
    SKIPPED = "SKIPPED"
    NOT_STARTED = "NOT_STARTED"
    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class DUPINSESSION(str, enum.Enum):
    RETAIN = "RETAIN"
    REMOVE = "REMOVE"
    UNIQUE = "UNIQUE"


class NotificationType(str, enum.Enum):
    UPDATE = "UPDATE"
    ALERT = "ALERT"
    RATING_CHANGE = "RATING_CHANGE"


class ExistingType(str, enum.Enum):
    NEW = "NEW"
    EXISTING = "EXISTING"


class SOURCEENUM(str, enum.Enum):
    NU = "NU"
    OD = "OD"
    CM = "CM"
    PD = "PD"


class Base(DeclarativeBase):
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class User(Base):
    __tablename__ = "user_account"

    user_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), primary_key=True, default=lambda _: str(uuid.uuid4())
    )
    email: Mapped[str] = mapped_column(
        String(256), nullable=False, unique=True, index=True
    )
    hashed_password: Mapped[str] = mapped_column(String(128), nullable=False)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")


class Base(DeclarativeBase):
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class UserAccount(Base):
    __tablename__ = "user_account"

    user_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), primary_key=True, default=lambda _: str(uuid.uuid4())
    )
    email: Mapped[str] = mapped_column(
        String(256), nullable=False, unique=True, index=True
    )
    hashed_password: Mapped[str] = mapped_column(String(128), nullable=False)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")


class User(Base):
    __tablename__ = "users_table"

    id = Column(Integer, autoincrement=True, index=True)
    user_id = Column(String, primary_key=True, unique=True)
    email = Column(String, unique=True, nullable=False, index=True)
    username = Column(String, unique=True, nullable=False, index=True)
    password = Column(String, nullable=False)
    verified = Column(Boolean, default=False, nullable=False)
    otp = Column(String, nullable=True)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")
    user_group = Column(String, nullable=False)
    api_key = Column(String, unique=True, nullable=False)
    key_expires_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<User(id={self.id}, user_id='{self.user_id}', email='{self.email}', username='{self.username}')>"


class RefreshToken(Base):
    __tablename__ = "refresh_token"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    refresh_token: Mapped[str] = mapped_column(
        String(512), nullable=False, unique=True, index=True
    )
    used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    exp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id = Column(String, ForeignKey("users_table.user_id", ondelete="CASCADE"))  # Change to correct PK
    user: Mapped["User"] = relationship(back_populates="refresh_tokens")
    user_group: Mapped[str] = Column(String, nullable=True)


class UploadSupplierData(Base):
    __tablename__ = "upload_supplier_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ens_id = Column(String, nullable=False)
    session_id = Column(String, nullable=False)
    user_id = Column(String, ForeignKey("users_table.user_id",
                                        ondelete="CASCADE"))  # Change to correct PK uploaded_external_vendor_id = Column(String, nullable=True)
    uploaded_external_vendor_id = Column(String, nullable=True)
    uploaded_name = Column(String, nullable=True)
    uploaded_cin = Column(String(255), nullable=True)
    uploaded_identifier = Column(String, nullable=True)
    uploaded_entity_type = Column(String, nullable=True)
    uploaded_identifier_type = Column(String, nullable=True)
    uploaded_address = Column(String, nullable=True)
    uploaded_client_onboarding_date = Column(Date, nullable=True)
    uploaded_client_msme_status = Column(String, nullable=True)
    uploaded_client_z_altman_type = Column(String, nullable=True)

    unmodified_name = Column(String, nullable=True)
    unmodified_identifier = Column(String, nullable=True)
    unmodified_entity_type = Column(String, nullable=True)
    unmodified_identifier_type = Column(String, nullable=True)
    unmodified_address = Column(String, nullable=True)
    unmodified_client_onboarding_date = Column(Date, nullable=True)
    unmodified_client_msme_status = Column(String, nullable=True)
    unmodified_client_z_altman_type = Column(String, nullable=True)

    suggested_name = Column(String, nullable=True)
    suggested_cin_id = Column(String, nullable=True)
    suggested_identifier = Column(String, nullable=True)
    suggested_entity_type = Column(String, nullable=True)
    suggested_identifier_type = Column(String, nullable=True)
    suggested_bid = Column(String, nullable=True)
    suggested_status = Column(String, nullable=True)

    name = Column(String, nullable=True)
    cin_id = Column(String, nullable=True)
    identifier = Column(String, nullable=True)
    entity_type = Column(String, nullable=True)
    identifier_type = Column(String, nullable=True)
    status = Column(String, nullable=True)
    bid = Column(String, nullable=True)

    validation_status = Column(
        SQLAlchemyEnum(ValidationStatus),
        nullable=False,
        server_default=expression.literal(ValidationStatus.PENDING.value)
    )
    final_status = Column(
        SQLAlchemyEnum(FinalStatus),
        nullable=False,
        server_default=expression.literal(FinalStatus.PENDING.value)
    )
    final_validation_status = Column(
        SQLAlchemyEnum(FinalValidatedStatus),
        nullable=False,
        server_default=expression.literal(FinalValidatedStatus.PENDING.value)
    )
    orbis_matched_status = Column(
        SQLAlchemyEnum(OribisMatchStatus),
        nullable=False,
        server_default=expression.literal(OribisMatchStatus.PENDING.value)
    )
    match_percentage = Column(Integer, nullable=False, default=0)
    preexisting_cin_id = Column(Boolean, nullable=False, default=False)
    process_status = Column(
        SQLAlchemyEnum(STATUS),
        nullable=False,
        server_default=expression.literal(STATUS.PENDING.value)
    )
    duplicate_in_session = Column(
        SQLAlchemyEnum(DUPINSESSION),
        nullable=False,
        server_default=expression.literal(DUPINSESSION.UNIQUE.value)
    )


class SupplierMasterData(Base):
    __tablename__ = "supplier_master_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=True)
    uploaded_name = Column(String(255), nullable=True)
    uploaded_cin = Column(String(255), nullable=True)
    uploaded_identifier = Column(String(255), nullable=True)
    uploaded_entity_type = Column(String(255), nullable=True)
    uploaded_identifier_type = Column(String(255), nullable=True)
    uploaded_address = Column(String, nullable=True)
    uploaded_client_onboarding_date = Column(Date, nullable=True)
    uploaded_client_msme_status = Column(String, nullable=True)
    uploaded_client_z_altman_type = Column(String, nullable=True)

    external_vendor_id = Column(String, nullable=True)
    bid = Column(String(255), nullable=True)
    cin_id = Column(String(50), nullable=True)
    ens_id = Column(String(50), nullable=True)
    session_id = Column(String(50), nullable=False)
    identifier = Column(String(255), nullable=True)
    entity_type = Column(String(255), nullable=True)
    identifier_type = Column(String(255), nullable=True)
    validation_status = Column(
        SQLAlchemyEnum(ValidationStatus),
        nullable=False,
        server_default=expression.literal(ValidationStatus.PENDING.value)
    )
    report_generation_status = Column(
        SQLAlchemyEnum(STATUS),
        nullable=False,
        server_default=expression.literal(STATUS.NOT_STARTED.value)
    )
    final_status = Column(
        SQLAlchemyEnum(FinalStatus),
        nullable=False,
        server_default=expression.literal(FinalStatus.PENDING.value)
    )
    __table_args__ = (
        UniqueConstraint("ens_id", "session_id", name="supplier_master_ensid_session"),
    )


class ExternalSupplierData(Base):
    __tablename__ = "external_supplier_data"

    id = Column(Integer, primary_key=True, autoincrement=True)

    session_id = Column(String, nullable=False)
    ens_id = Column(String, nullable=False)
    cin_id = Column(String, nullable=True)
    identifier = Column(String, nullable=True)
    entity_type = Column(String, nullable=True)
    identifier_type = Column(String, nullable=True)
    uploaded_address = Column(String, nullable=True)
    uploaded_client_onboarding_date = Column(Date, nullable=True)
    uploaded_client_msme_status = Column(String, nullable=True)
    uploaded_client_z_altman_type = Column(String, nullable=True)
    bid = Column(String, nullable=True)
    legal_name = Column(String(255), nullable=False)
    e_filing_status = Column(String(100), nullable=True)
    incorporation_date = Column(Date, nullable=True)
    address = Column(Text, nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pan = Column(String(50), nullable=True)
    website = Column(Text, nullable=True)
    classification = Column(String(100), nullable=True)
    alias = Column(JSONB, nullable=True)
    number_of_employees = Column(Integer, nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    directors = Column(JSONB, nullable=True)
    financial_ratios = Column(JSONB, nullable=True)
    financial_bs = Column(JSONB, nullable=True)
    financial_pnl = Column(JSONB, nullable=True)
    financial_cash_flow = Column(JSONB, nullable=True)
    credit_rating = Column(JSONB, nullable=True)
    auditors = Column(JSONB, nullable=True)
    related_party_transaction = Column(JSONB, nullable=True)
    shareholdings = Column(JSONB, nullable=True)
    subsidiary = Column(JSONB, nullable=True)
    open_charges = Column(JSONB, nullable=True)
    legal_history = Column(JSONB, nullable=True)
    msme = Column(JSONB, nullable=True)
    gst_details = Column(JSONB, nullable=True)
    key_indicators = Column(JSONB, nullable=True)
    b2b_validation = Column(JSONB, nullable=True)
    domain_validation = Column(JSONB, nullable=True)
    address_validation = Column(JSONB, nullable=True)
    sanctions = Column(JSONB, nullable=True)
    sanctions_employee = Column(JSONB, nullable=True)
    google_rating = Column(JSONB, nullable=True)
    z_altman_factors = Column(JSONB, nullable=True)
    ratio_factors = Column(JSONB, nullable=True)
    cyber_risk = Column(JSONB, nullable=True)
    google_image_name = Column(String, nullable=True)
    epfo=Column(JSONB, nullable=True)
    auditor_comment=Column(JSONB, nullable=True)
    director_network=Column(JSONB, nullable=True)
    __table_args__ = (
        UniqueConstraint("ens_id", "session_id", name="external_supplier_ensid_session"),
    )


class KPISchemas(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier for each record
    kpi_area = Column(String, nullable=False)  # Area of KPI (e.g., ESG)
    kpi_code = Column(String, nullable=False)  # Unique code for the KPI
    kpi_flag = Column(Boolean, nullable=False, default=False)  # Boolean flag for the KPI
    kpi_value = Column(String, nullable=True)  # Numeric value associated with the KPI
    kpi_details = Column(String, nullable=True)  # Additional details for the KPI
    ens_id = Column(String, nullable=False)  # Ensures related entity ID
    session_id = Column(String, nullable=False)  # Session identifier
    kpi_rating = Column(String, nullable=True)
    kpi_definition = Column(String, nullable=True)


class KpiFstb(KPISchemas):
    __tablename__ = "finance"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpifstb'),
    )


class KpiLgrk(KPISchemas):
    __tablename__ = "legal"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpilgrk'),
    )


class KpiOvar(KPISchemas):
    __tablename__ = "ovar"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpiovar'),
    )


class KpiRfct(KPISchemas):
    __tablename__ = "adverse_media"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpirfct'),
    )


class KpiSape(KPISchemas):
    __tablename__ = "entity_existance"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpisape'),
    )


class EnsidScreeningStatus(Base):
    __tablename__ = "ensid_screening_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=True)
    overall_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                            server_default=expression.literal(STATUS.NOT_STARTED.value))
    orbis_retrieval_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                                    server_default=expression.literal(STATUS.NOT_STARTED.value))
    screening_modules_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                                      server_default=expression.literal(STATUS.NOT_STARTED.value))
    report_generation_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                                      server_default=expression.literal(STATUS.NOT_STARTED.value))
    # Ensure unique constraint for (ens_id, session_id)
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', name='unique_ensid_session_ensid_screening_status'),
    )


class CompanyProfile(Base):
    __tablename__ = "company_profile"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Primary key added
    name = Column(String, nullable=True)
    uploaded_name = Column(String(255), nullable=True)
    external_vendor_id = Column(String, nullable=True)
    location = Column(String, nullable=True)
    address = Column(String, nullable=True)
    website = Column(String, nullable=True)
    e_filing_status = Column(String, nullable=True)
    category = Column(String, nullable=True)
    pan_id = Column(String, nullable=True)
    alias = Column(Text, nullable=True)
    incorporation_date = Column(String, nullable=True)
    shareholders = Column(Text, nullable=True)
    revenue = Column(String, nullable=True)
    subsidiaries = Column(String, nullable=True)
    key_executives = Column(Text, nullable=True)
    employee = Column(String, nullable=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=False)
    identifier = Column(String, nullable=True)
    identifier_type = Column(String, nullable=True)
    entity_type = Column(String, nullable=True)
    corporate_group = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("ens_id", "session_id", name="unique_ensid_session"),
    )


class NewsMaster(Base):
    __tablename__ = "news_master"

    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(Text)
    name = Column(String, nullable=False)
    title = Column(Text)
    category = Column(Text)
    summary = Column(Text)
    news_date = Column(Date)
    sentiment = Column(String)
    content_filtered = Column(Boolean)
    country = Column(Text)
    start_date = Column(Date)
    end_date = Column(Date)
    error = Column(Text, nullable=True)
    __table_args__ = (
        UniqueConstraint("name", "link", "news_date", name="unique_name_link_date"),
    )


class Summary(Base):
    __tablename__ = "summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=True)
    area = Column(String(50), nullable=False)
    summary = Column(Text)

    __table_args__ = (
        UniqueConstraint("ens_id", "session_id", "area", name="unique_ens_session_summary"),
    )


class ClientConfiguration(Base):
    __tablename__ = "client_configuration"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), nullable=False)
    client_name = Column(String(250), nullable=True)
    kpi_theme = Column(String(50), nullable=True)
    report_section = Column(String(50), nullable=True)
    kpi_area = Column(String, nullable=False)
    module_enabled_status = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("client_id", "client_name", "kpi_theme", "report_section", "kpi_area",
                         name="unique_client_configuration"),
    )


class SessionConfiguration(Base):
    __tablename__ = "session_configuration"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), nullable=False)
    session_id = Column(String(50), nullable=False)
    module = Column(String(50), nullable=True)
    module_active_status = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("session_id", "module", name="unique_session_configuration"),
    )


class TokenMonitor(Base):
    __tablename__ = "token_monitor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usage_time = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    token_used = Column(Integer, nullable=False)
    payload = Column(JSONB, nullable=True)
    openai_model = Column(Text, nullable=True)


class ExcludedEntities(Base):
    __tablename__ = "excluded_entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    category = Column(Text)


class APIKey(Base):
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    api_key = Column(String, unique=True, nullable=False)
    user_id = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    expires_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint('user_id', 'api_key', name='api_keys_unique'),
    )


class ScheduleMonitoring(Base):
    __tablename__ = "schedule_monitoring"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(String, nullable=False, unique=True)
    group_name = Column(String)
    periodicity = Column(String)
    frequency = Column(Integer, nullable=False)
    interval = Column(SQLAlchemyEnum(GroupIntervals), nullable=False,
                      server_default=expression.literal(GroupIntervals.WEEK.value))
    start_date = Column(DateTime(timezone=True), server_default=func.now())
    next_run_date = Column(DateTime(timezone=True), server_default=func.now())
    last_scheduled_date = Column(DateTime(timezone=True), server_default=func.now())
    last_start_time = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.ACTIVE.value))
    group_description = Column(Text)
    created_by = Column(String)


class ENSScheduleGroupMapping(Base):
    __tablename__ = "ens_schedule_group_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ens_id = Column(String, nullable=False)
    group_id = Column(String, nullable=False)


class SessionGroupMapping(Base):
    __tablename__ = "session_group_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String)
    group_id = Column(String)
    source_id = Column(String)
    mapping_type = Column(SQLAlchemyEnum(PeriodicSessionType), nullable=False,
                          server_default=expression.literal(PeriodicSessionType.NEW_SESSION.value))


# Modifications :
class SessionScreeningStatus(Base):
    __tablename__ = "session_screening_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    overall_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                            server_default=expression.literal(STATUS.NOT_STARTED.value))
    list_upload_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                                server_default=expression.literal(STATUS.NOT_STARTED.value))
    supplier_name_validation_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                                             server_default=expression.literal(STATUS.NOT_STARTED.value))
    screening_analysis_status = Column(SQLAlchemyEnum(STATUS), nullable=False,
                                       server_default=expression.literal(STATUS.NOT_STARTED.value))
    total_ens_count = Column(Integer, nullable=True)
    completed_ens_count = Column(Integer, nullable=True)
    failed_ens_count = Column(Integer, nullable=True)
    skipped_ens_count = Column(Integer, nullable=True)
    # New columns
    source = Column(SQLAlchemyEnum(SOURCEENUM), nullable=False, server_default=expression.literal(SOURCEENUM.NU.value))
    source_id = Column(String, nullable=True)  # This is GroupRunID

    __table_args__ = (
        UniqueConstraint('session_id', name='unique_sessionid_session'),
    )


class EntityUniverse(Base):
    __tablename__ = "entity_universe"

    id = Column(Integer, autoincrement=True)
    ens_id = Column(String(50), primary_key=True)
    cin_id = Column(String(50), nullable=True)
    identifier = Column(String(50), nullable=True)
    identifier_type = Column(String(50), nullable=True)
    entity_type = Column(String(50), nullable=True)
    name = Column(String(255), nullable=True)
    address = Column(Text, nullable=True)
    city = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    phone_or_fax = Column(String(50), nullable=True)
    email_or_website = Column(String(100), nullable=True)
    pan_id = Column(String(50), nullable=True)
    state = Column(String(100), nullable=True)
    last_session_id = Column(String(50), nullable=True)
    overall_supplier_rating = Column(String(50), nullable=True)
    thematic_rating = Column(JSONB, nullable=True)

    # New 12 columns
    management = Column(JSONB, nullable=True)
    unmodified_name = Column(String(255), nullable=True)
    unmodified_cin_id = Column(String(50), nullable=True)
    unmodified_identifier = Column(String(50), nullable=True)
    unmodified_identifier_type = Column(String(50), nullable=True)
    unmodified_entity_type = Column(String(50), nullable=True)

    last_screened_date = Column(DateTime(timezone=True), nullable=True)
    external_vendor_id = Column(String, nullable=True)


class Notification(Base):
    __tablename__ = "notification"

    id = Column(Integer, autoincrement=True, primary_key=True)
    ens_id = Column(String(50), nullable=True)
    notification_type = Column(SQLAlchemyEnum(NotificationType), nullable=False,
                               server_default=expression.literal(NotificationType.UPDATE.value))
    title = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    theme = Column(String(100), nullable=True)
    data_value = Column(Text, nullable=True)
    session_id = Column(String(50), nullable=True)
    create_time = Column(DateTime(timezone=True), server_default=func.now())


class GoogleRating(Base):
    __tablename__ = "google_ratings"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(255), nullable=True)
    rating = Column(String(255), nullable=True)
    no_of_reviews = Column(Text, nullable=True)
    reviews = Column(JSONB, nullable=True)
    identifier = Column(String(50), nullable=True)
    identifier_type = Column(String(50), nullable=True)
    entity_type = Column(String(50), nullable=True)
    create_time = Column(DateTime(timezone=True), server_default=func.now())


class StepStatus(str, enum.Enum):
    not_called = "not_called"
    failed = "failed"
    passed = "passed"


class GeocodeSource(str, enum.Enum):
    not_called = "not_called"
    google = "google"
    llm = "llm"


class AddressZoneMaster(Base):
    __tablename__ = "address_zone_master"

    id = Column(Integer, autoincrement=True)
    geo_id = Column(String(50), primary_key=True)
    name = Column(String(255), nullable=True)
    address = Column(Text, nullable=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    identifier = Column(String(255), nullable=True)
    identifier_type = Column(String(100), nullable=True)
    entity_type = Column(String(100), nullable=True)
    places = Column(JSONB, nullable=True)
    zone = Column(String(50), nullable=True)
    confidence = Column(Integer, nullable=True)
    reason = Column(Text, nullable=True)
    image_name = Column(String(255), nullable=True)
    geocode_status = Column(SQLAlchemyEnum(StepStatus, name="step_status_enum", schema="public"), nullable=True)
    places_status = Column(SQLAlchemyEnum(StepStatus, name="step_status_enum", schema="public"), nullable=True)
    llm_status = Column(SQLAlchemyEnum(StepStatus, name="step_status_enum", schema="public"), nullable=True)
    geocode_source = Column(SQLAlchemyEnum(GeocodeSource, name="geocode_source_enum", schema="public"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)


class Kpiadditional(KPISchemas):
    __tablename__ = "cyber_esg"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpicybesg'),
    )


from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func


class MsmeCheck(Base):
    __tablename__ = "msme_check"

    id = Column(Integer, primary_key=True, autoincrement=True)
    identifier = Column(String(50), nullable=False)
    pan = Column(String(10), nullable=False)
    name = Column(String(100), nullable=True)

    msme_status = Column(String(50), nullable=False)
    response = Column(JSONB, nullable=False)

    create_time = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    __table_args__ = (
        # ✅ Prevent duplicate PAN per identifier
        UniqueConstraint("identifier", "pan", name="unique_pan_identifier"),
    )

class Probe42Data(Base):
    __tablename__ = "probe42_data"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(255), nullable=True)
    identifier = Column(String(255), nullable=True)
    identifier_type = Column(String(100), nullable=True)
    entity_type = Column(String(100), nullable=True)
    probe42_data = Column(JSONB, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),  # set when row is created
        nullable=True
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),  # initial value
        onupdate=func.now(),  # auto update on change
        nullable=True
    )
