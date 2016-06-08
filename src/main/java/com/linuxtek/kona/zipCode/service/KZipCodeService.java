/*
 * Copyright (C) 2011 LINUXTEK, Inc.  All Rights Reserved.
 */
package com.linuxtek.kona.zipCode.service;

import com.linuxtek.kona.remote.service.KService;
import com.linuxtek.kona.remote.service.KServiceRelativePath;
import com.linuxtek.kona.zipCode.entity.KZipCode;

@KServiceRelativePath(KZipCodeService.SERVICE_PATH)
public interface KZipCodeService extends KService {

    // NOTE: SERVICE_PATH must begin with rpc/ prefix
    public static final String SERVICE_PATH = "rpc/kona/ZipCodeService";

	public boolean isValid(Integer zip);

	public Double getDistance(KZipCode zip1, KZipCode zip2);

	public Double getDistance(Integer zip1, Integer zip2);

	public Boolean areZipsInRadius(KZipCode zip1, KZipCode zip2, Integer radius);

	public Boolean areZipsInRadius(Integer zip1, Integer zip2, Integer radius);

	public Integer[] getZipsInRadius(KZipCode zip, Integer radius);

	public Integer[] getZipsInRadius(Integer zip, Integer radius);

}
