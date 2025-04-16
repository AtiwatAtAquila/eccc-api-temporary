from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Boolean
from app.db.base import Base


class DummyData(Base):
    __tablename__ = "electricDummyData"
    id = Column(Integer,
                primary_key=True,
                index=True,
                autoincrement=True,
                nullable=False)
    submit_timestamp = Column(DateTime, index=True, nullable=False)
    data_timestamp = Column(DateTime, index=True, nullable=False)
    category = Column(String(16), index=True, nullable=False)
    zone = Column(String, index=True, nullable=True)
    province = Column(String, index=True, nullable=True)
    value_tag = Column(String, index=True, nullable=True)
    value = Column(Float, nullable=False)


class Project(Base):
    __tablename__ = "electricProjects"

    g_project_key = Column(String, primary_key=True, index=True)
    spp_vspp_rowid = Column(String)
    e_license_rowid = Column(String)
    pk_powersystemresource = Column(String)
    erc_cd = Column(String)
    org = Column(String)
    spp_vspp_plant_cd = Column(String)
    ppa_contract_no = Column(String)
    project_name = Column(String)
    spp_vspp_project_name = Column(String)
    licensee = Column(String)
    display_addr = Column(String)
    subdistrict = Column(String)
    district = Column(String)
    province = Column(String)
    country_zone = Column(String)
    egat_zone = Column(String)
    contract_status = Column(String)
    e_license_instl_mw = Column(Float)
    e_license_instl_kva = Column(Float)
    installed_cap_mw = Column(Float)
    contracted_cap_mw = Column(Float)
    project_type = Column(String)
    contract_type = Column(String)
    technology_a_group_1 = Column(String)
    technology_a_group_2 = Column(String)
    technology_a_detail = Column(String)
    primary_fuel_a_group_1 = Column(String)
    primary_fuel_a_group_2 = Column(String)
    primary_fuel_a_group_3 = Column(String)
    secondary_fuel_a_group_1 = Column(String)
    secondary_fuel_a_group_2 = Column(String)
    secondary_fuel_a_group_3 = Column(String)
    technology_b_group_1 = Column(String)
    technology_b_group_2 = Column(String)
    technology_b_detail = Column(String)
    primary_fuel_b_group_1 = Column(String)
    primary_fuel_b_group_2 = Column(String)
    primary_fuel_b_group_3 = Column(String)
    secondary_fuel_b_group_1 = Column(String)
    secondary_fuel_b_group_2 = Column(String)
    secondary_fuel_b_group_3 = Column(String)
    utilization = Column(String)
    scod = Column(DateTime)
    cod = Column(DateTime)
    lat = Column(Float)
    lng = Column(Float)
    is_egat_sys_gen = Column(Boolean)
    is_sharing_lic = Column(Boolean)
    licensingno = Column(String)
    erc_district_no = Column(String)
    erc_district_displayname = Column(String)
    month_key = Column(String)
    update_timestamp = Column(DateTime, index=True)


class PeakDay(Base):
    __tablename__ = "electricPeakDay"
    id = Column(Integer,
                primary_key=True,
                index=True,
                autoincrement=True,
                nullable=False)
    peak_date = Column(Date, index=True, nullable=False)
    peak_datetime = Column(DateTime, index=True, nullable=False)
    peak_type = Column(String, index=True, nullable=False)
    value = Column(Float, nullable=False)
