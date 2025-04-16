from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional


class DummyDataBase(BaseModel):
    data_timestamp: datetime
    category: str
    zone: Optional[str] = None
    province: Optional[str] = None
    value_tag: Optional[str] = None
    value: float


class DummyDataCreate(DummyDataBase):
    submit_timestamp: datetime


class DummyDataResponse(DummyDataBase):
    submit_timestamp: datetime


class ProjectCreate(BaseModel):
    g_project_key: str
    spp_vspp_rowid: Optional[str]
    e_license_rowid: Optional[str]
    pk_powersystemresource: Optional[str]
    erc_cd: Optional[str]
    org: Optional[str]
    spp_vspp_plant_cd: Optional[str]
    ppa_contract_no: Optional[str]
    project_name: Optional[str]
    spp_vspp_project_name: Optional[str]
    licensee: Optional[str]
    display_addr: Optional[str]
    subdistrict: Optional[str]
    district: Optional[str]
    province: Optional[str]
    country_zone: Optional[str]
    egat_zone: Optional[str]
    contract_status: Optional[str]
    e_license_instl_mw: Optional[float]
    e_license_instl_kva: Optional[float]
    installed_cap_mw: Optional[float]
    contracted_cap_mw: Optional[float]
    project_type: Optional[str]
    contract_type: Optional[str]
    technology_a_group_1: Optional[str]
    technology_a_group_2: Optional[str]
    technology_a_detail: Optional[str]
    primary_fuel_a_group_1: Optional[str]
    primary_fuel_a_group_2: Optional[str]
    primary_fuel_a_group_3: Optional[str]
    secondary_fuel_a_group_1: Optional[str]
    secondary_fuel_a_group_2: Optional[str]
    secondary_fuel_a_group_3: Optional[str]
    technology_b_group_1: Optional[str]
    technology_b_group_2: Optional[str]
    technology_b_detail: Optional[str]
    primary_fuel_b_group_1: Optional[str]
    primary_fuel_b_group_2: Optional[str]
    primary_fuel_b_group_3: Optional[str]
    secondary_fuel_b_group_1: Optional[str]
    secondary_fuel_b_group_2: Optional[str]
    secondary_fuel_b_group_3: Optional[str]
    utilization: Optional[str]
    scod: Optional[datetime]
    cod: Optional[datetime]
    lat: Optional[float]
    lng: Optional[float]
    is_egat_sys_gen: Optional[bool]
    is_sharing_lic: Optional[bool]
    licensingno: Optional[str]
    erc_district_no: Optional[str]
    erc_district_displayname: Optional[str]
    month_key: Optional[str]
    update_timestamp: Optional[datetime]

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True
        json_encoders = {
            bytes: lambda v: v.decode('utf-8', errors='ignore') if v else None
        }


class PeakDayBase(BaseModel):
    peak_date: date
    peak_type: str


class PeakDayCreate(PeakDayBase):
    peak_datetime: datetime
    value: float
