package com.linuxtek.kona.zipCode.entity;

import com.linuxtek.kona.data.entity.KEntityObject;

public interface KZipCode extends KEntityObject {

	public Long getId();

	public void setId(Long id);

	public Double getLongitude();

	public Double getLatitude();

	public String getCity();

	public String getState();

	public String getCounty();

	public Integer getAreaCode();

	public void setLongitude(Double longitude);

	public void setLatitude(double latitude);

	public void setCity(String city);

	public void setState(String state);

	public void setCounty(String county);

	public void setAreaCode(Integer areacode);

	public Integer intValue();

	public String getLocation();
}