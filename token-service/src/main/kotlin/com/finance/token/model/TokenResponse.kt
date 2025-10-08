package com.finance.token.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.camel.component.http.HttpMethods

@JsonIgnoreProperties(ignoreUnknown = true)
data class TokenResponse @JsonCreator constructor(
    @JsonProperty("access_token") val accessToken: String?,
    @JsonProperty("token_type")  val tokenType: String?,
    @JsonProperty("expires_in")  val expiresIn: Long?,
    @JsonProperty("scope")       val scope: String?
)
