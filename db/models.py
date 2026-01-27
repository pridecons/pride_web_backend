# db/models.py
from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Date,
    DateTime,
    Numeric,
    Boolean,
    ForeignKey,
    Text,
    Index,
    UniqueConstraint,
    Enum,
    func,
    JSON
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY
from db.connection import Base
from sqlalchemy.sql import expression
from sqlalchemy.dialects.postgresql import JSONB

def ts_now():
    return func.now()
class TimestampMixin:
    created_at = Column(DateTime(timezone=True), server_default=ts_now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=ts_now(), onupdate=ts_now(), nullable=False)

# ============================================================
# 1) SECURITY MASTER (Securities.dat)
#     - Aligned with NSE CM30 Securities.dat v1.24 spec
# ============================================================

class NseCmSecurity(Base):
    __tablename__ = "nse_cm_securities"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # From Securities.dat: Token Number
    token_id = Column(Integer, unique=True, index=True, nullable=False)

    # Identity
    symbol = Column(String(64), index=True, nullable=False)
    series = Column(String(8), index=True, nullable=True)
    isin = Column(String(16), index=True, nullable=True)

    # Company / security details
    company_name = Column(String(256), nullable=True)

    # Market / lot related
    lot_size = Column(Integer, nullable=True)              # Market Lot / Board Lot
    face_value = Column(Numeric(10, 2), nullable=True)
    segment = Column(String(16), nullable=False, default="CM")
    active_flag = Column(Boolean, nullable=False, default=True)

    # Extra fields from Securities.dat (v1.24)
    issued_capital = Column(Numeric(20, 4), nullable=True)        # Issued capital
    settlement_cycle = Column(Integer, nullable=True)             # 0=T+0, 1=T+1, etc.
    tick_size = Column(Numeric(10, 4), nullable=True)             # Minimum price tick
    freeze_percentage = Column(Numeric(6, 2), nullable=True)      # Freeze % (risk limits)
    credit_rating = Column(String(64), nullable=True)             # If applicable

    # Important dates
    issue_start_date = Column(Date, nullable=True)
    issue_end_date = Column(Date, nullable=True)
    listing_date = Column(Date, nullable=True)
    record_date = Column(Date, nullable=True)
    book_closure_start_date = Column(Date, nullable=True)
    book_closure_end_date = Column(Date, nullable=True)
    no_delivery_start_date = Column(Date, nullable=True)
    no_delivery_end_date = Column(Date, nullable=True)

    # NSE v1.24: Permitted To Trade (0/1/2)
    # 0 = listed but not permitted, 1 = permitted to trade, 2 = BSE-only etc.
    permitted_to_trade = Column(Integer, nullable=True)

    # Audit
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    # Relationships
    bhavcopies = relationship(
        "NseCmBhavcopy",
        back_populates="security",
        lazy="selectin",
    )

    intraday_bars = relationship(
        "NseCmIntraday1Min",
        back_populates="security",
        lazy="selectin",
    )

    def __repr__(self):
        return f"<NseCmSecurity token={self.token_id} symbol={self.symbol}>"


# ============================================================
# 2) BHAVCOPY EOD (CMBhavcopy_*.txt)
# ============================================================

class NseCmBhavcopy(Base):
    __tablename__ = "nse_cm_bhavcopy"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    trade_date = Column(Date, nullable=False, index=True)

    # Link to NseCmSecurity by token
    token_id = Column(
        Integer,
        ForeignKey("nse_cm_securities.token_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )

    # Direct symbol/series from bhavcopy (text-safe)
    symbol = Column(String(64), index=True, nullable=False)
    series = Column(String(8), index=True, nullable=True)

    # Prices (already in rupees in CM bhavcopy)
    open_price = Column(Numeric(12, 4), nullable=True)
    high_price = Column(Numeric(12, 4), nullable=True)
    low_price = Column(Numeric(12, 4), nullable=True)
    close_price = Column(Numeric(12, 4), nullable=True)
    last_price = Column(Numeric(12, 4), nullable=True)
    prev_close = Column(Numeric(12, 4), nullable=True)

    total_traded_qty = Column(BigInteger, nullable=True)
    total_traded_value = Column(Numeric(18, 4), nullable=True)
    total_trades = Column(BigInteger, nullable=True)

    isin = Column(String(16), index=True, nullable=True)

    # Delivery flags (can be filled from separate delivery files)
    delivery_data_available = Column(Boolean, nullable=False, default=False)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    security = relationship(
        "NseCmSecurity",
        primaryjoin="NseCmBhavcopy.token_id==NseCmSecurity.token_id",
        back_populates="bhavcopies",
        lazy="selectin",
    )

    __table_args__ = (
        # keep your current unique (for series not null use-case)
        UniqueConstraint(
            "trade_date", "symbol", "series",
            name="uq_nse_cm_bhavcopy_date_symbol_series",
        ),

        # ✅ ensures no duplicates when series IS NULL (Postgres)
        Index(
            "uq_nse_cm_bhavcopy_date_symbol__series_null",
            "trade_date", "symbol",
            unique=True,
            postgresql_where=expression.text("series IS NULL"),
        ),

        # ✅ ensures no duplicates when series IS NOT NULL (Postgres)
        Index(
            "uq_nse_cm_bhavcopy_date_symbol_series__series_not_null",
            "trade_date", "symbol", "series",
            unique=True,
            postgresql_where=expression.text("series IS NOT NULL"),
        ),

        Index("ix_nse_cm_bhavcopy_date_token", "trade_date", "token_id"),
    )


# ============================================================
# 3) INTRADAY MARKET DATA – 1 MIN (*.mkt.gz)
#      (CM30 snapshot MBP records)
# ============================================================

class NseCmIntraday1Min(Base):
    __tablename__ = "nse_cm_intraday_1min"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    trade_date = Column(Date, index=True, nullable=False)

    # Interval / bar start time (IST or UTC – consistent with ingestion)
    interval_start = Column(
        DateTime(timezone=True),
        index=True,
        nullable=False,
    )

    # Link to security master (by token)
    token_id = Column(
        Integer,
        ForeignKey("nse_cm_securities.token_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # OHLC / last / avg (prices in rupees after scaling /100)
    open_price = Column(Numeric(14, 4), nullable=True)
    high_price = Column(Numeric(14, 4), nullable=True)
    low_price = Column(Numeric(14, 4), nullable=True)
    close_price = Column(Numeric(14, 4), nullable=True)

    last_price = Column(Numeric(14, 4), nullable=True)
    avg_price = Column(Numeric(14, 4), nullable=True)

    # Volume/value/trades
    # Current ingestion uses interval_total_traded_quantity OR total_traded_quantity
    volume = Column(BigInteger, nullable=True)          # Generic volume
    value = Column(Numeric(20, 4), nullable=True)       # Traded value (if derived)
    total_trades = Column(BigInteger, nullable=True)

    # Extra granularity from snapshot if you want to use separately
    total_traded_qty = Column(BigInteger, nullable=True)        # Full-day total_traded_quantity
    interval_traded_qty = Column(BigInteger, nullable=True)     # Interval_total_traded_quantity

    # Best bid/ask snapshot (LEVEL1 MBP)
    best_bid_price = Column(Numeric(14, 4), nullable=True)
    best_bid_qty = Column(BigInteger, nullable=True)
    best_ask_price = Column(Numeric(14, 4), nullable=True)
    best_ask_qty = Column(BigInteger, nullable=True)

    # Open interest (mostly FO, optional for CM)
    open_interest = Column(BigInteger, nullable=True)

    # NSE v1.24: Indicative Close Price (last 30 minutes)
    indicative_close_price = Column(Numeric(14, 4), nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )

    # Relationship
    security = relationship(
        "NseCmSecurity",
        back_populates="intraday_bars",
        lazy="joined",
    )

    __table_args__ = (
        Index(
            "ix_intraday_token_date_time",
            "token_id",
            "trade_date",
            "interval_start",
        ),
    )

    def __repr__(self):
        return (
            f"<NseCmIntraday1Min token={self.token_id} "
            f"{self.trade_date} {self.interval_start}>"
        )


# ============================================================
# 4) INTRADAY INDICES – 1 MIN (*.ind.gz)
#      (CM30 indices snapshot)
# ============================================================

class NseCmIndex1Min(Base):
    __tablename__ = "nse_cm_indices_1min"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    trade_date = Column(Date, index=True, nullable=False)

    interval_start = Column(
        DateTime(timezone=True),
        index=True,
        nullable=False,
    )

    # From .ind feed: index token + resolved name (via NseIndexMaster)
    index_id = Column(Integer, index=True, nullable=True)      # NSE index token/code
    index_name = Column(String(128), index=True, nullable=False)  # e.g. "NIFTY 50"

    # Headline OHLC
    open_price = Column(Numeric(14, 4), nullable=True)
    high_price = Column(Numeric(14, 4), nullable=True)
    low_price = Column(Numeric(14, 4), nullable=True)
    close_price = Column(Numeric(14, 4), nullable=True)

    last_price = Column(Numeric(14, 4), nullable=True)
    avg_price = Column(Numeric(14, 4), nullable=True)

    # Additional index fields from feed
    percentage_change = Column(Numeric(10, 4), nullable=True)       # %change *after* correct scaling
    indicative_close_value = Column(Numeric(14, 4), nullable=True)  # Indicative Close Index Value

    # Interval OHLC (if you need separate from headline)
    interval_open_price = Column(Numeric(14, 4), nullable=True)
    interval_high_price = Column(Numeric(14, 4), nullable=True)
    interval_low_price = Column(Numeric(14, 4), nullable=True)
    interval_close_price = Column(Numeric(14, 4), nullable=True)

    # Volume / turnover if ever included or derived
    volume = Column(BigInteger, nullable=True)
    turnover = Column(Numeric(20, 4), nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )

    __table_args__ = (
        Index(
            "ix_index_name_date_time",
            "index_name",
            "trade_date",
            "interval_start",
        ),
    )

    def __repr__(self):
        return f"<NseCmIndex1Min {self.index_name} {self.trade_date} {self.interval_start}>"


# ============================================================
# 5) INDEX MASTER + CONSTITUENTS (CSV/Static Mapping)
# ============================================================

class NseIndexMaster(Base):
    __tablename__ = "nse_index_master"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # e.g. "NIFTY 50", "NIFTY MIDCAP 150"
    index_symbol = Column(String(64), unique=True, nullable=False)

    # Short code if you want (e.g. "NIFTY50", "NIFTYMID150")
    short_code = Column(String(32), unique=True, nullable=True)

    full_name = Column(String(256), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)


class NseIndexConstituent(Base):
    __tablename__ = "nse_index_constituent"

    id = Column(Integer, primary_key=True, autoincrement=True)

    index_id = Column(Integer, ForeignKey("nse_index_master.id"), nullable=False)

    # Same symbol / isin scheme as NseCmSecurity
    symbol = Column(String(64), index=True, nullable=False)
    isin = Column(String(16), index=True, nullable=True)

    weight = Column(Numeric(10, 4), nullable=True)    # Optional if CSV provides
    as_of_date = Column(Date, nullable=False)

    index = relationship("NseIndexMaster", backref="constituents")

    def __repr__(self):
        return f"<NseIndexConstituent index_id={self.index_id} symbol={self.symbol} as_of={self.as_of_date}>"

class NseIngestionLog(Base):
    __tablename__ = "nse_ingestion_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    trade_date = Column(Date, nullable=False, index=True)
    segment = Column(String(16), nullable=False, index=True)   # "CM30_MKT" / "CM30_IND"
    seq = Column(Integer, nullable=False, index=True)          # 37, 38, 39...
    remote_path = Column(String(512), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("trade_date", "segment", "seq", name="uq_ingestion_log_trade_segment_seq"),
    )

class leadData(Base, TimestampMixin):
    __tablename__ = "lead_data"

    id                = Column(Integer, primary_key=True, autoincrement=True)
    email             = Column(String(100), nullable=True, index=True)
    mobile            = Column(String(20), nullable=True, index=True)
    
    full_name         = Column(String(100), nullable=True)
    director_name     = Column(String(100), nullable=True)
    father_name       = Column(String(100), nullable=True)
    gender            = Column(String(10), nullable=True)
    aadhaar           = Column(String(12), nullable=True)
    pan               = Column(String(10), nullable=True)
    state             = Column(String(100), nullable=True)
    city              = Column(String(100), nullable=True)
    district          = Column(String(100), nullable=True)
    address           = Column(Text, nullable=True)
    pincode           = Column(String(6), nullable=True)
    country           = Column(String(50), nullable=True)
    dob               = Column(Date, nullable=True)

    gstin             = Column(String(15), nullable=True, default="URP")
    alternate_mobile  = Column(String(20), nullable=True)
    marital_status    = Column(String(20), nullable=True)
    occupation        = Column(String(100), nullable=True)

    kyc               = Column(Boolean, default=False, nullable=True)
    kyc_id            = Column(String(100), nullable=True) #group_id
    kyc_url           = Column(String(500), nullable=True)
    url_date          = Column(DateTime(timezone=True), nullable=True)

    step1             = Column(Boolean, default=False, nullable=True) #Enter phone number and otp
    step2             = Column(Boolean, default=False, nullable=True) #Enter pan number
    step3             = Column(Boolean, default=False, nullable=True) #Enter data
    step4             = Column(Boolean, default=False, nullable=True) #Generate url
    step5             = Column(Boolean, default=False, nullable=True) #Done kyc
    session_id        = Column(String(500), nullable=True, index=True)
    
class OTP(Base):
    __tablename__ = "otps"

    id = Column(Integer, primary_key=True, index=True)
    mobile = Column(String(20), nullable=False, index=True)
    otp = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class MissingLogo(Base):
    __tablename__ = "missing_logo"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(100), nullable=False, unique=True, index=True)  # ✅ unique
    name = Column(String(200), nullable=False)

# mutual fund tables  (MINIMUM, category/subcategory as TEXT)
# -----------------------------------------------------------

class MfAmc(Base, TimestampMixin):
    __tablename__ = "mf_amc"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Fund house name (unique)
    name = Column(String(200), nullable=False, unique=True, index=True)

    # Optional branding
    logo_url = Column(String(500), nullable=True)
    website = Column(String(300), nullable=True)

    # Optional metadata
    incorporation_date = Column(Date, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    # Relationships
    schemes = relationship("MfScheme", back_populates="amc", lazy="selectin")

    def __repr__(self):
        return f"<MfAmc id={self.id} name={self.name}>"


class MfScheme(Base, TimestampMixin):
    """
    Master table (single source of truth)
    category/sub_category are TEXT (no separate tables)
    """
    __tablename__ = "mf_scheme"

    # AMFI / mfapi scheme_code is unique globally
    scheme_code = Column(BigInteger, primary_key=True)

    amc_id = Column(
        BigInteger,
        ForeignKey("mf_amc.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    scheme_name = Column(Text, nullable=False)

    # From mfapi meta (strings)
    scheme_type = Column(String(120), nullable=True)      # "Open Ended Schemes"
    category = Column(String(120), nullable=True)         # "Equity Scheme"
    sub_category = Column(String(200), nullable=True)     # "Large Cap Fund" / "Money Market Fund" etc.

    # Direct/Regular, Growth/IDCW/Bonus (either parsed from name or stored from your parser)
    plan = Column(String(20), nullable=True)              # DIRECT / REGULAR
    option = Column(String(20), nullable=True)            # GROWTH / IDCW / BONUS / UNKNOWN
    frequency = Column(String(20), nullable=True)         # DAILY/WEEKLY/MONTHLY/QUARTERLY/NONE
    payout_type = Column(String(20), nullable=True)       # PAYOUT/REINVESTMENT/UNKNOWN

    # Cap bucket for equity only (LARGE/MID/SMALL/FLEXI/MULTI/NOT_APPLICABLE)
    cap_bucket = Column(String(30), nullable=True)

    # ISINs from AMFI/mfapi meta
    isin_growth = Column(String(20), nullable=True)
    isin_div_reinvestment = Column(String(20), nullable=True)

    is_active = Column(Boolean, nullable=False, default=True)

    # Relationships
    amc = relationship("MfAmc", back_populates="schemes", lazy="joined")
    navs = relationship("MfNavDaily", back_populates="scheme", lazy="selectin")
    snapshot = relationship("MfSchemeSnapshot", back_populates="scheme", uselist=False, lazy="selectin")

    __table_args__ = (
        Index("ix_mf_scheme_amc_category", "amc_id", "category"),
        Index("ix_mf_scheme_filters", "category", "sub_category", "plan", "option"),
    )

    def __repr__(self):
        return f"<MfScheme code={self.scheme_code} name={self.scheme_name[:40]}...>"


class MfNavDaily(Base, TimestampMixin):
    """
    NAV history table
    PK (scheme_code, nav_date) so duplicate date insert won't happen.
    """
    __tablename__ = "mf_nav_daily"

    scheme_code = Column(
        BigInteger,
        ForeignKey("mf_scheme.scheme_code", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )

    nav_date = Column(Date, primary_key=True, nullable=False, index=True)

    nav = Column(Numeric(18, 6), nullable=False)

    # Relationships
    scheme = relationship("MfScheme", back_populates="navs", lazy="joined")

    __table_args__ = (
        Index("ix_mf_nav_daily_date", "nav_date"),
        Index("ix_mf_nav_daily_scheme_date", "scheme_code", "nav_date"),
    )

    def __repr__(self):
        return f"<MfNavDaily scheme={self.scheme_code} date={self.nav_date} nav={self.nav}>"


class MfSchemeSnapshot(Base, TimestampMixin):
    """
    Fast listing cache (one row per scheme)
    Use this for Groww-style list: latest nav, returns, expense, AUM, etc.
    """
    __tablename__ = "mf_scheme_snapshot"

    scheme_code = Column(
        BigInteger,
        ForeignKey("mf_scheme.scheme_code", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )

    as_of_date = Column(Date, nullable=False, index=True)

    latest_nav = Column(Numeric(18, 6), nullable=True)

    # Optional extras for UI (fill when you have data source)
    aum_cr = Column(Numeric(18, 2), nullable=True)            # fund size in Cr
    expense_ratio = Column(Numeric(10, 4), nullable=True)     # %
    risk_level = Column(String(30), nullable=True)            # Low/Moderate/High etc.

    return_1y = Column(Numeric(12, 4), nullable=True)
    return_3y = Column(Numeric(12, 4), nullable=True)
    return_5y = Column(Numeric(12, 4), nullable=True)

    # Relationship
    scheme = relationship("MfScheme", back_populates="snapshot", lazy="joined")

    __table_args__ = (
        Index("ix_mf_snapshot_date", "as_of_date"),
    )

    def __repr__(self):
        return f"<MfSchemeSnapshot scheme={self.scheme_code} as_of={self.as_of_date} nav={self.latest_nav}>"


class MfJobLog(Base):
    __tablename__ = "mf_job_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    job_name = Column(String(80), nullable=False, index=True)   # NAV_SYNC / SNAPSHOT_SYNC / AMC_SYNC etc.
    run_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    status = Column(String(20), nullable=False, index=True)     # SUCCESS / FAILED
    message = Column(Text, nullable=True)

    # Optional: how many records processed
    inserted = Column(Integer, nullable=True)
    updated = Column(Integer, nullable=True)
    skipped = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_mf_joblog_job_run", "job_name", "run_at"),
    )

    def __repr__(self):
        return f"<MfJobLog job={self.job_name} status={self.status} at={self.run_at}>"

class PreopenMoversCache(Base):
    __tablename__ = "preopen_movers_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)

    index_id = Column(Integer, nullable=False)
    trade_date = Column(Date, nullable=False)

    # store only essential preopen results
    limit = Column(Integer, nullable=False, default=50)

    gainers = Column(JSON, nullable=False)  # list[dict]
    losers = Column(JSON, nullable=False)   # list[dict]

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index("uq_preopen_movers_cache", "index_id", "trade_date", "limit", unique=True),
    )


class StockDetail(Base):
    __tablename__ = "stock_details"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ✅ identity
    symbol = Column(String(32), nullable=False)          # e.g. TATASTEEL / NSE code
    company_name = Column(String(255), nullable=False)   # e.g. Tata Steel
    industry = Column(String(120), nullable=True)

    # ✅ snapshot date (one record per day)
    fetch_date = Column(Date, nullable=False)

    # ✅ full payload
    data = Column(JSONB, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        # ✅ prevent duplicates per day
        UniqueConstraint("symbol", "fetch_date", name="uq_stock_details_symbol_fetch_date"),
        # ✅ indexes for fast filters
        Index("ix_stock_details_fetch_date", "fetch_date"),
        Index("ix_stock_details_symbol", "symbol"),
        Index("ix_stock_details_industry", "industry"),
    )

