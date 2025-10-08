package com.finance.token.routes

import com.finance.token.model.TokenResponse
import com.finance.token.processors.ExchangeCodeForToken
import com.finance.token.processors.PrepareKafkaMessage
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.camel.Exchange
import org.apache.camel.LoggingLevel
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.component.jackson.JacksonDataFormat
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class TokenRoutes : RouteBuilder() {

    @ConfigProperty(name = "transactions.list.url") lateinit var transactionsListUrl: String

    @Inject lateinit var exchangeCodeForToken: ExchangeCodeForToken
    @Inject lateinit var prepareKafkaMessage: PrepareKafkaMessage

    override fun configure() {

        onException(Exception::class.java)
            .handled(true)
            .log(LoggingLevel.ERROR, "token-service error: ${'$'}{exception.message}")
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .setBody(simple("""{"error":"${'$'}{exception.message}"}"""))

        from("direct:tokenFlow").routeId("token-flow")
            .log("Starting token flow with code=${'$'}{header.code}")

            // Exchange code -> accessToken
            .process(exchangeCodeForToken)
            .toD("${'$'}{exchangeProperty.kcTokenUrl}")
            .unmarshal(JacksonDataFormat(TokenResponse::class.java))
            .process {
                val tr = it.`in`.getBody(TokenResponse::class.java)
                val token = tr.accessToken ?: error("no access_token")
                it.setProperty("accessToken", token)
            }

            // Call transactions-service
            .setHeader(Exchange.HTTP_METHOD, constant("GET"))
            .setHeader("Authorization", simple("Bearer ${'$'}{exchangeProperty.accessToken}"))
            .setHeader("Accept", constant("application/json"))
            .setBody(constant(null))
            .toD("{{transactions.list.url}}&throwExceptionOnFailure=false")
            .process {
                val code = it.`in`.getHeader(Exchange.HTTP_RESPONSE_CODE, Int::class.java) ?: 0
                val body = it.`in`.getBody(String::class.java)
                if (code != 200) error("transactions-service $code: $body")
                it.setProperty("transactionsJson", body)
                val userId = Regex(""""userId"\s*:\s*"([^"]+)"""").find(body)?.groupValues?.getOrNull(1) ?: "unknown"
                it.setProperty("userId", userId)
            }

            .process(prepareKafkaMessage)
            .to("kafka:user-transactions")

            .process {
                val userId = it.getProperty("userId", String::class.java)
                it.`in`.body = mapOf("status" to "published", "userId" to userId)
                it.`in`.setHeader(Exchange.CONTENT_TYPE, "application/json")
            }
    }
}
