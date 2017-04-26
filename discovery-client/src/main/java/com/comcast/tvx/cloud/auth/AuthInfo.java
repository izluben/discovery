/* ===========================================================================
 * Copyright (c) 2017 Comcast Corp. All rights reserved.
 * ===========================================================================
 *
 * Author: Stanislav Menshykov
 * Created: 04/26/2017  3:40 PM
 */
package com.comcast.tvx.cloud.auth;

public class AuthInfo {
    private final String authScheme;
    private final String authId;
    private final Boolean allowNotAuthenticated;

    public AuthInfo(String authScheme, String authId, Boolean allowNotAuthenticated) {
        this.authScheme = authScheme;
        this.authId = authId;
        this.allowNotAuthenticated = allowNotAuthenticated;
    }

    public String getAuthScheme() {
        return authScheme;
    }

    public String getAuthId() {
        return authId;
    }

    public Boolean getAllowNotAuthenticated() {
        return allowNotAuthenticated;
    }
}
